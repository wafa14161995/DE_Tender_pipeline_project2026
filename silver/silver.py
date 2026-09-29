# ============================================================
# TENDER PIPELINE — SILVER LAYER (CSV edition, no Delta / no Spark)
#
# Flow:
#   Azure Bronze  (tenderbronze/<source>/<YYYY-MM-DD>/<run_timestamp>.json)
#   -> rebuild (all history) OR incremental (new Bronze blobs only)
#   -> 4V profiling
#   -> Technology / Review / Non-Tech classification
#   -> unified schema (schema.py) + source fixes + FX to SAR
#   -> cleaning + source-specific date parsing + quality gates
#   -> stable tender_id + dedupe + latest-snapshot collapse
#   -> ONE Silver dataset, stored twice in the tendersilver container:
#        tendersilver/delta/silver_tenders/     Delta table  (used by the pipeline)
#        tendersilver/tenders/silver_tenders.csv CSV backup  (full copy)
#      Incremental runs upsert by tender_id in pandas (newest scrape wins).
#
# Important:
#   - Silver keeps Technology + Review + Non-Tech.
#   - Silver does NOT derive Open/Closed status; Gold owns that feature.
#   - Source-provided status, when available, is preserved as source_status.
#   - Pure pandas: NO Spark. Delta is written with the `deltalake` package
#     (delta-rs), so there is no cluster/Spark cost and no Unity Catalog
#     permission needed — only the storage connection-string secret.
#     On Databricks serverless run first:  %pip install deltalake
#   - Local mode reads Bronze but writes only under ./local_silver_output.
# ============================================================

import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient


# ============================================================
# RUNTIME CONFIG
# ============================================================

def _env_flag(name, default=False):
    value = os.getenv(name)
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


SILVER_LOCAL_MODE = _env_flag("SILVER_LOCAL_MODE", False)
SILVER_DRY_RUN = _env_flag("SILVER_DRY_RUN", False)

# auto:
#   no state -> rebuild
#   state exists -> incremental
# rebuild:
#   read all Bronze history and replace the Silver CSV baseline
# incremental:
#   read only Bronze blobs not yet recorded in Silver state, then MERGE
SILVER_MODE = os.getenv("SILVER_MODE", "rebuild").strip().lower()
if SILVER_MODE not in {"auto", "rebuild", "incremental"}:
    raise ValueError(
        "SILVER_MODE must be one of: auto, rebuild, incremental"
    )

SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "tendersilver").strip()
SILVER_OUTPUT_BLOB = os.getenv(          # CSV backup
    "SILVER_OUTPUT_BLOB",
    "tenders/silver_tenders.csv",
).strip()
SILVER_DELTA_PATH = os.getenv(           # Delta table folder in the container
    "SILVER_DELTA_PATH",
    "delta/silver_tenders",
).strip().strip("/")
SILVER_DELTA_VACUUM_HOURS = int(os.getenv("SILVER_DELTA_VACUUM_HOURS", "0"))
SILVER_STATE_BLOB = os.getenv(
    "SILVER_STATE_BLOB",
    "_state/processed_bronze_blobs.json",
)

LOCAL_OUTPUT_ROOT = Path(
    os.getenv("SILVER_LOCAL_OUTPUT", "./local_silver_output")
).resolve()
LOCAL_SILVER_CSV_PATH = Path(
    os.getenv(
        "SILVER_LOCAL_CSV_PATH",
        str(LOCAL_OUTPUT_ROOT / "silver_tenders.csv"),
    )
).resolve()
LOCAL_SILVER_DELTA_PATH = Path(
    os.getenv(
        "SILVER_LOCAL_DELTA_PATH",
        str(LOCAL_OUTPUT_ROOT / "delta" / "silver_tenders"),
    )
).resolve()
LOCAL_STATE_PATH = Path(
    os.getenv(
        "SILVER_LOCAL_STATE_PATH",
        str(LOCAL_OUTPUT_ROOT / "processed_bronze_blobs.json"),
    )
).resolve()


def _get_connection_string():
    # Databricks: keep the team's existing secret convention.
    try:
        return dbutils.secrets.get(
            scope="azure-storage",
            key="connection-string",
        )
    except Exception:
        pass

    # Local VS Code / terminal: use an environment variable.
    value = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if value and value.strip():
        return value.strip()

    raise RuntimeError(
        "Azure Storage connection string not found. "
        "In Databricks use secret scope azure-storage / connection-string. "
        "Locally set AZURE_STORAGE_CONNECTION_STRING in the terminal/session."
    )


# Local Python does not have Databricks display().
if "display" not in globals():
    def display(value):
        try:
            import pandas as _pd
            if isinstance(value, _pd.DataFrame):
                print(value.head(30).to_string(index=False))
                return
        except Exception:
            pass
        print(value)


# ============================================================
# AZURE BRONZE CONNECTION
# ============================================================

raw_connection_string = _get_connection_string()

clean_parts = []
for part in raw_connection_string.strip().split(";"):
    if "=" in part:
        key, value = part.split("=", 1)
        clean_parts.append(f"{key.strip()}={value.strip()}")

clean_connection_string = ";".join(clean_parts)

blob_service_client = BlobServiceClient.from_connection_string(
    clean_connection_string
)

container_name = os.getenv("BRONZE_CONTAINER", "tenderbronze").strip()
storage_account_name = blob_service_client.account_name

container_client = blob_service_client.get_container_client(
    container_name
)
container_client.get_container_properties()

# ============================================================
# DELTA (delta-rs) HELPERS — pure Python, no Spark
# ============================================================

def _connection_part(name):
    for part in clean_connection_string.split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            if key.strip().lower() == name.lower():
                return value.strip()
    return None


def delta_location(container, path, local_path):
    """Return (uri, storage_options) for delta-rs."""
    if SILVER_LOCAL_MODE:
        return str(local_path), None
    account_key = _connection_part("AccountKey")
    if not account_key:
        raise RuntimeError(
            "The storage connection string has no AccountKey; delta-rs needs "
            "AccountName + AccountKey to write the Delta table."
        )
    return (
        f"az://{container}/{path}",
        {
            "account_name": storage_account_name,
            "account_key": account_key,
        },
    )


def _require_deltalake():
    try:
        import deltalake  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Package 'deltalake' is missing. On Databricks run "
            "`%pip install deltalake` in the first cell (or add it to the job "
            "environment dependencies). Locally: pip install deltalake"
        ) from exc


def delta_exists(uri, storage_options):
    _require_deltalake()
    from deltalake import DeltaTable
    try:
        return bool(DeltaTable.is_deltatable(uri, storage_options=storage_options))
    except AttributeError:
        try:
            DeltaTable(uri, storage_options=storage_options)
            return True
        except Exception:
            return False


def _is_null(value):
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def frame_to_text(df):
    """
    Every value -> text (dates ISO), None for missing. This is the common
    in-memory form used for the upsert, so rows read from Delta, from the CSV
    backup and from the new batch compare exactly the same way.
    """
    import datetime as _dt

    def conv(v):
        if _is_null(v):
            return None
        if isinstance(v, pd.Timestamp):
            if v.tzinfo is not None:
                return v.tz_convert("UTC").isoformat()
            return v.isoformat()
        if isinstance(v, _dt.datetime):
            if v.tzinfo is not None:
                return pd.Timestamp(v).tz_convert("UTC").isoformat()
            return v.isoformat()
        if isinstance(v, _dt.date):
            return v.isoformat()
        if isinstance(v, (bool, np.bool_)):
            return "True" if v else "False"
        return str(v)

    out = pd.DataFrame(
        {c: [conv(v) for v in df[c].tolist()] for c in df.columns},
        index=df.index,
    )
    return out.astype(object)


DELTA_DATE_COLUMNS = {"published_date", "closing_date"}
DELTA_TIMESTAMP_COLUMNS = {"scraped_at"}
DELTA_FLOAT_COLUMNS = {
    "document_fee",
    "tender_value_min_sar",
    "tender_value_max_sar",
    "temporary_bond_sar",
    "fx_rate_to_sar",
}
DELTA_INT_COLUMNS = {"source_id_group_size"}
DELTA_BOOL_COLUMNS = {
    "has_quality_warning",
    "possible_cross_source_duplicate",
    "source_id_reused",
}


def text_to_arrow(df):
    """Typed Arrow table for Delta: real DATE / TIMESTAMP / numbers."""
    import pyarrow as pa

    arrays, fields = [], []
    for column in df.columns:
        values = df[column].tolist()

        if column in DELTA_DATE_COLUMNS:
            typ = pa.date32()
            data = [None if _is_null(v) else pd.Timestamp(v).date() for v in values]
        elif column in DELTA_TIMESTAMP_COLUMNS:
            typ = pa.timestamp("us", tz="UTC")
            data = []
            for v in values:
                ts = parse_utc_timestamp(v)
                data.append(None if _is_null(ts) else ts.to_pydatetime())
        elif column in DELTA_FLOAT_COLUMNS:
            typ = pa.float64()
            data = [
                None if _is_null(x) else float(x)
                for x in pd.to_numeric(pd.Series(values, dtype=object), errors="coerce")
            ]
        elif column in DELTA_INT_COLUMNS:
            typ = pa.int64()
            data = [
                None if _is_null(x) else int(x)
                for x in pd.to_numeric(pd.Series(values, dtype=object), errors="coerce")
            ]
        elif column in DELTA_BOOL_COLUMNS:
            typ = pa.bool_()
            data = [
                None if _is_null(v) else str(v).strip().lower() in {"true", "1", "yes"}
                for v in values
            ]
        else:
            typ = pa.string()
            data = [None if _is_null(v) else str(v) for v in values]

        arrays.append(pa.array(data, type=typ))
        fields.append(pa.field(column, typ, nullable=True))

    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def write_delta_overwrite(df_text, uri, storage_options, vacuum_hours=168):
    """
    Overwrite the Delta table with the full current dataset (a new Delta
    version — older versions stay available for time travel until VACUUM).
    """
    _require_deltalake()
    from deltalake import DeltaTable, write_deltalake

    table = text_to_arrow(df_text)
    try:
        write_deltalake(
            uri, table, mode="overwrite", schema_mode="overwrite",
            storage_options=storage_options,
        )
    except TypeError:  # older deltalake versions
        write_deltalake(
            uri, table, mode="overwrite", overwrite_schema=True,
            storage_options=storage_options,
        )

    dt = DeltaTable(uri, storage_options=storage_options)
    if vacuum_hours and vacuum_hours > 0:
        try:
            dt.vacuum(
                retention_hours=vacuum_hours,
                dry_run=False,
                enforce_retention_duration=False,
            )
        except Exception as exc:
            print(f"  ⚠️ Delta VACUUM skipped: {exc}")
    return dt.version()


def read_delta_text(uri, storage_options):
    _require_deltalake()
    from deltalake import DeltaTable
    return frame_to_text(DeltaTable(uri, storage_options=storage_options).to_pandas())


SILVER_DELTA_URI, SILVER_DELTA_OPTIONS = delta_location(
    SILVER_CONTAINER, SILVER_DELTA_PATH, LOCAL_SILVER_DELTA_PATH
)
_require_deltalake()

print("✅ Azure Bronze connection ready")
print("Storage Account:", storage_account_name)
print("Bronze Container:", container_name)
print("Local mode:", SILVER_LOCAL_MODE)
print("Dry run:", SILVER_DRY_RUN)
print("Requested Silver mode:", SILVER_MODE)
print("Silver Delta:", SILVER_DELTA_URI)

# ============================================================
# SILVER STEP 1 — MERGE-READY BRONZE READER
#
# Bronze layout:
#   <source>/<YYYY-MM-DD>/<run_timestamp>.json
#
# Manual/demo run (default):
#   - Reads ALL historical Bronze JSON blobs across all run_date folders.
#   - Concatenates history per source into one DataFrame.
#   - A partial individual run does NOT invalidate older/newer history.
#
# Optional explicit-run mode (future orchestration/debugging):
#   - Pass notebook parameter: bronze_run_id
#   - Reads exactly that run and fails if that explicit run is incomplete.
#
# No Azure writes are performed.
# ============================================================

import json
from collections import defaultdict
from pathlib import PurePosixPath

import pandas as pd


# ============================================================
# 1. REQUIRED CONNECTION OBJECTS
# ============================================================

required_globals = [
    "blob_service_client",
    "container_name",
    "storage_account_name",
]

missing_globals = [
    name for name in required_globals
    if name not in globals()
]

if missing_globals:
    raise RuntimeError(
        "Missing Azure connection objects: "
        + ", ".join(missing_globals)
        + ". Run the Azure Bronze connection cell first."
    )


# ============================================================
# 2. SOURCES EXPECTED IN BRONZE HISTORY
# ============================================================

TARGET_SOURCES = [
    "bahrain",
    "capt_kw",
    "etimad",
    "forsah",
    "oman_T_tendersBoard",
    "qatar",
    "qatar_foundation",
    "qatar_monaqasat",
    "uae_global",
    "uae_mof",
]

TARGET_SOURCE_SET = set(TARGET_SOURCES)


# ============================================================
# 3. RUN PARAMETERS + INCREMENTAL STATE
# ============================================================

from datetime import datetime, timezone

requested_bronze_run_id = None

# Databricks widgets are optional. Local execution uses environment variables.
try:
    try:
        dbutils.widgets.get("bronze_run_id")
    except Exception:
        dbutils.widgets.text("bronze_run_id", "")

    widget_value = dbutils.widgets.get("bronze_run_id").strip()
    if widget_value:
        requested_bronze_run_id = widget_value
except Exception:
    pass

if not requested_bronze_run_id:
    env_run = os.getenv("BRONZE_RUN_ID", "").strip()
    if env_run:
        requested_bronze_run_id = env_run

try:
    try:
        dbutils.widgets.get("silver_mode")
    except Exception:
        dbutils.widgets.text("silver_mode", "")

    widget_mode = dbutils.widgets.get("silver_mode").strip().lower()
    if widget_mode:
        SILVER_MODE = widget_mode
except Exception:
    pass

if SILVER_MODE not in {"auto", "rebuild", "incremental"}:
    raise ValueError(
        "silver_mode/SILVER_MODE must be one of: auto, rebuild, incremental"
    )


def _load_processed_blob_state():
    """
    Return (processed_blob_names, state_exists).

    Local mode NEVER reads/writes Azure Silver state. This protects the real
    production checkpoint while the team is testing from VS Code.
    """
    if SILVER_LOCAL_MODE:
        if not LOCAL_STATE_PATH.exists():
            return set(), False
        payload = json.loads(LOCAL_STATE_PATH.read_text(encoding="utf-8"))
        return set(payload.get("processed_blobs", [])), True

    try:
        silver_state_container = blob_service_client.get_container_client(
            SILVER_CONTAINER
        )
        if not silver_state_container.exists():
            return set(), False

        state_blob = silver_state_container.get_blob_client(
            SILVER_STATE_BLOB
        )
        if not state_blob.exists():
            return set(), False

        payload = json.loads(
            state_blob.download_blob().readall()
        )
        return set(payload.get("processed_blobs", [])), True
    except Exception as exc:
        raise RuntimeError(
            "Failed to read Silver incremental state."
        ) from exc


processed_blob_names, silver_state_exists = _load_processed_blob_state()

if requested_bronze_run_id:
    resolved_silver_mode = "explicit_run"
elif SILVER_MODE == "auto":
    resolved_silver_mode = (
        "incremental" if silver_state_exists else "rebuild"
    )
    # State exists but there is no Silver baseline (Delta or CSV backup)
    # -> rebuild it from all Bronze history.
    if resolved_silver_mode == "incremental":
        if delta_exists(SILVER_DELTA_URI, SILVER_DELTA_OPTIONS):
            _baseline_exists = True
        elif SILVER_LOCAL_MODE:
            _baseline_exists = LOCAL_SILVER_CSV_PATH.exists()
        else:
            _baseline_exists = (
                blob_service_client
                .get_container_client(SILVER_CONTAINER)
                .get_blob_client(SILVER_OUTPUT_BLOB)
                .exists()
            )
        if not _baseline_exists:
            resolved_silver_mode = "rebuild"
else:
    resolved_silver_mode = SILVER_MODE


# ============================================================
# 4. LIST BRONZE ONCE AND INVENTORY ALL HISTORY
# ============================================================

container_client = blob_service_client.get_container_client(
    container_name
)

json_blobs = [
    blob
    for blob in container_client.list_blobs()
    if blob.name.endswith(".json")
]

if not json_blobs:
    raise RuntimeError(
        f"No JSON blobs found in Bronze container: {container_name}"
    )

source_to_entries = defaultdict(list)
run_to_source_blobs = defaultdict(dict)
unexpected_blob_names = []
seen_source_run = {}

for blob in json_blobs:
    parts = PurePosixPath(blob.name).parts

    if len(parts) != 3:
        unexpected_blob_names.append(blob.name)
        continue

    source, run_date, filename = parts

    if source not in TARGET_SOURCE_SET:
        continue

    if not filename.endswith(".json"):
        continue

    run_id = filename[:-5]
    source_run_key = (source, run_id)

    if source_run_key in seen_source_run:
        raise RuntimeError(
            "Duplicate Bronze blob for the same source/run_id: "
            f"source={source}, run_id={run_id}, "
            f"blobs={seen_source_run[source_run_key]}, {blob.name}"
        )

    seen_source_run[source_run_key] = blob.name

    entry = {
        "blob": blob,
        "run_date": run_date,
        "run_id": run_id,
    }

    source_to_entries[source].append(entry)
    run_to_source_blobs[run_id][source] = entry

if not source_to_entries:
    raise RuntimeError(
        "No Bronze blobs matched "
        "<source>/<run_date>/<run_timestamp>.json"
    )

# Overall Bronze history must eventually contain all configured sources.
# Individual incremental runs may be partial.
missing_history_sources = sorted(
    TARGET_SOURCE_SET - set(source_to_entries)
)

if missing_history_sources:
    raise RuntimeError(
        "Bronze history is missing configured source(s): "
        + ", ".join(missing_history_sources)
    )


# ============================================================
# 5. SELECT SCOPE
# ============================================================

if requested_bronze_run_id:
    if requested_bronze_run_id not in run_to_source_blobs:
        raise RuntimeError(
            "Requested bronze_run_id does not exist: "
            f"{requested_bronze_run_id}"
        )

    explicit_source_map = run_to_source_blobs[requested_bronze_run_id]
    present_sources = set(explicit_source_map)
    missing_sources = sorted(TARGET_SOURCE_SET - present_sources)

    if missing_sources:
        raise RuntimeError(
            "Requested Bronze run is INCOMPLETE.\n"
            f"run_id={requested_bronze_run_id}\n"
            f"present={len(present_sources)}/{len(TARGET_SOURCES)}\n"
            f"missing={', '.join(missing_sources)}"
        )

    explicit_dates = {
        item["run_date"]
        for item in explicit_source_map.values()
    }

    if len(explicit_dates) != 1:
        raise RuntimeError(
            "Requested Bronze run appears under multiple run_date folders: "
            + ", ".join(sorted(explicit_dates))
        )

    selected_entries_by_source = {
        source: [explicit_source_map[source]]
        for source in TARGET_SOURCES
    }
    selected_run_ids = [requested_bronze_run_id]
    selected_run_dates = sorted(explicit_dates)
    selected_bronze_run_id = requested_bronze_run_id
    selected_bronze_run_date = selected_run_dates[0]
    selection_mode = "explicit_run"

elif resolved_silver_mode == "rebuild":
    selected_entries_by_source = {
        source: sorted(
            source_to_entries[source],
            key=lambda item: (
                item["run_date"],
                item["run_id"],
                item["blob"].name,
            ),
        )
        for source in TARGET_SOURCES
    }

    selected_run_ids = sorted({
        entry["run_id"]
        for entries in selected_entries_by_source.values()
        for entry in entries
    })
    selected_run_dates = sorted({
        entry["run_date"]
        for entries in selected_entries_by_source.values()
        for entry in entries
    })

    selected_bronze_run_id = "ALL_HISTORY"
    selected_bronze_run_date = (
        selected_run_dates[0]
        if len(selected_run_dates) == 1
        else f"{selected_run_dates[0]}..{selected_run_dates[-1]}"
    )
    selection_mode = "rebuild_all_history"

else:
    # Incremental mode: only Bronze blobs that were not successfully
    # checkpointed by a previous successful Silver run.
    selected_entries_by_source = {
        source: [
            entry
            for entry in sorted(
                source_to_entries[source],
                key=lambda item: (
                    item["run_date"],
                    item["run_id"],
                    item["blob"].name,
                ),
            )
            if entry["blob"].name not in processed_blob_names
        ]
        for source in TARGET_SOURCES
    }

    # Partial incremental batches are expected.
    selected_entries_by_source = {
        source: entries
        for source, entries in selected_entries_by_source.items()
        if entries
    }

    if not selected_entries_by_source:
        print("=" * 78)
        print("SILVER INCREMENTAL NO-OP")
        print("=" * 78)
        print("No new Bronze blobs were found.")
        print("Existing Silver CSV/state remains unchanged.")
        print("=" * 78)
        try:
            dbutils.notebook.exit("NO_OP")
        except NameError:
            raise SystemExit(0)

    selected_run_ids = sorted({
        entry["run_id"]
        for entries in selected_entries_by_source.values()
        for entry in entries
    })
    selected_run_dates = sorted({
        entry["run_date"]
        for entries in selected_entries_by_source.values()
        for entry in entries
    })
    selected_bronze_run_id = (
        selected_run_ids[0]
        if len(selected_run_ids) == 1
        else f"INCREMENTAL:{len(selected_run_ids)}_RUNS"
    )
    selected_bronze_run_date = (
        selected_run_dates[0]
        if len(selected_run_dates) == 1
        else f"{selected_run_dates[0]}..{selected_run_dates[-1]}"
    )
    selection_mode = "incremental_new_blobs"


# ============================================================
# 6. READ SELECTED BRONZE BLOBS AND CONCATENATE PER SOURCE
# ============================================================

class _LocalFrame:
    """Tiny Spark-like wrapper used by local/dry-run transformation tests."""
    def __init__(self, records):
        self._pdf = pd.DataFrame(records)

    def toPandas(self):
        return self._pdf.copy()

    def count(self):
        return int(len(self._pdf))


dfs_by_source = {}
created_dataframes = []
bronze_run_manifest_rows = []

active_sources = [
    source
    for source in TARGET_SOURCES
    if selected_entries_by_source.get(source)
]

for source in active_sources:
    source_records = []

    for entry in selected_entries_by_source[source]:
        blob = entry["blob"]
        run_id = entry["run_id"]
        run_date = entry["run_date"]

        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob.name,
        )

        try:
            payload = blob_client.download_blob().readall()
            json_data = json.loads(payload)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read Bronze blob for source={source}: {blob.name}"
            ) from exc

        rows = json_data if isinstance(json_data, list) else [json_data]
        records_in_blob = []

        for row in rows:
            record = dict(row) if isinstance(row, dict) else {"value": row}

            record["file_path"] = (
                f"wasbs://{container_name}@{storage_account_name}"
                f".blob.core.windows.net/{blob.name}"
            )
            record["_bronze_run_id"] = run_id
            record["_bronze_run_date"] = run_date
            record["_bronze_blob_name"] = blob.name

            records_in_blob.append(record)

        source_records.extend(records_in_blob)

        bronze_run_manifest_rows.append({
            "source": source,
            "rows": len(records_in_blob),
            "blob_name": blob.name,
            "run_id": run_id,
            "run_date": run_date,
        })

    if not source_records:
        # A selected source with only empty new blobs contributes nothing to
        # the transformation. Keep the blobs in the manifest so state can be
        # advanced after a successful no-row batch for that source.
        continue

    # Pandas-backed frame for all modes: spark.createDataFrame on nested,
    # mixed-type dicts often fails on Serverless (Spark Connect), and the
    # 4V step converts to pandas immediately anyway.
    source_df = _LocalFrame(source_records)

    dfs_by_source[source] = source_df
    created_dataframes.append(f"df_{source}")

bronze_run_manifest = pd.DataFrame(bronze_run_manifest_rows)

if not dfs_by_source:
    print("=" * 78)
    print("SILVER NO-ROW BATCH")
    print("=" * 78)
    print("Selected Bronze blobs contained no records.")
    print("No Silver business rows will be changed.")
    print("=" * 78)
    try:
        dbutils.notebook.exit("NO_OP")
    except NameError:
        raise SystemExit(0)


# ============================================================
# 7. READER QUALITY GATES
# ============================================================

if selection_mode in {"rebuild_all_history", "explicit_run"}:
    assert set(dfs_by_source) == TARGET_SOURCE_SET, (
        "Baseline/explicit Bronze reader did not create all required source DataFrames"
    )
else:
    assert set(dfs_by_source).issubset(TARGET_SOURCE_SET), (
        "Incremental Bronze reader produced an unexpected source"
    )

source_counts = {
    source: df.count()
    for source, df in dfs_by_source.items()
}

total_bronze_rows = sum(source_counts.values())

assert all(count > 0 for count in source_counts.values()), (
    "One or more selected Bronze source DataFrames are empty"
)

assert not bronze_run_manifest.empty, "Bronze reader manifest is empty"
assert bronze_run_manifest["blob_name"].is_unique, (
    "Same Bronze blob was selected more than once"
)

if selection_mode == "explicit_run":
    assert set(bronze_run_manifest["run_id"]) == {selected_bronze_run_id}, (
        "Explicit Bronze mode mixed run IDs"
    )


# ============================================================
# 8. OUTPUT
# ============================================================

print("=" * 78)
print("MERGE-READY BRONZE READER")
print("=" * 78)
print("Container:", container_name)
print("Selection mode:", selection_mode)
print("Bronze runs represented:", len(selected_run_ids))
print("Bronze date folders represented:", len(selected_run_dates))
print("Bronze blobs read:", len(bronze_run_manifest))
print("Sources in this batch:", len(dfs_by_source))
print("Total Bronze rows:", total_bronze_rows)
print("-" * 78)

for source in active_sources:
    if source not in source_counts:
        continue
    source_blob_count = int(
        (bronze_run_manifest["source"] == source).sum()
    )
    print(
        f"{source:<24} {source_counts[source]:>7} rows "
        f"from {source_blob_count:>3} Bronze file(s)"
    )

print("-" * 78)
if selection_mode == "rebuild_all_history":
    print("✅ ALL historical Bronze runs selected for baseline rebuild")
elif selection_mode == "incremental_new_blobs":
    print("✅ Only NEW Bronze blobs selected for incremental processing")
    print("✅ Partial source batches are allowed")
else:
    print("✅ One explicit complete Bronze run selected")
print("✅ Per-record Bronze run/date/blob lineage preserved")
print("✅ Bronze is READ-ONLY")
print("=" * 78)

# ============================================================
# SILVER STEP 1 — DATA PROFILING & 4V CHECKS
# Volume | Velocity | Variety | Veracity
# ============================================================

import json
import numpy as np
import pandas as pd


# ------------------------------------------------------------
# 1. Create safe Pandas copies
# Original Spark DataFrames remain unchanged.
# ------------------------------------------------------------
pandas_dfs = {
    source: spark_df.toPandas().copy()
    for source, spark_df in dfs_by_source.items()
}


# ------------------------------------------------------------
# 2. Helper functions
# ------------------------------------------------------------
TITLE_CANDIDATES = [
    "Tender Subject",
    "Title",
    "موضوع المناقصة",
    "عنوان_المناقصة",
]


def find_existing_column(df, candidates):
    """Return the first matching column that exists."""
    for column in candidates:
        if column in df.columns:
            return column
    return None


def normalize_for_duplicate_check(value):
    """
    Convert non-hashable/nested values into stable hashable text
    ONLY for duplicate detection.

    The original DataFrame is never modified.
    """

    # Missing value
    if value is None:
        return None

    # NumPy array
    if isinstance(value, np.ndarray):
        return json.dumps(
            value.tolist(),
            ensure_ascii=False,
            sort_keys=True,
            default=str
        )

    # Dictionary
    if isinstance(value, dict):
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            default=str
        )

    # List / tuple / set
    if isinstance(value, (list, tuple, set)):
        return json.dumps(
            list(value),
            ensure_ascii=False,
            sort_keys=True,
            default=str
        )

    # Fallback for any unexpected unhashable object
    try:
        hash(value)
        return value
    except TypeError:
        return str(value)


def count_duplicate_rows(df):
    """
    Count exact duplicate rows safely.
    This is a CHECK only — no rows are removed.
    """

    check_df = df.copy()

    for column in check_df.columns:
        check_df[column] = check_df[column].map(
            normalize_for_duplicate_check
        )

    return int(check_df.duplicated().sum())


def count_missing_cells(df):
    """
    Count:
    - None / NaN
    - blank strings
    """

    missing_cells = 0

    for column in df.columns:

        series = df[column]

        # Actual nulls
        missing_cells += int(series.isna().sum())

        # Blank strings only
        if series.dtype == "object":

            non_null = series.dropna()

            blank_count = (
                non_null
                .map(
                    lambda x:
                    isinstance(x, str)
                    and x.strip() == ""
                )
                .sum()
            )

            missing_cells += int(blank_count)

    return missing_cells


# ------------------------------------------------------------
# 3. Build 4V report
# ------------------------------------------------------------
four_v_rows = []

for source, df in pandas_dfs.items():

    # ========================================================
    # V1 — VOLUME
    # ========================================================
    row_count = len(df)
    column_count = len(df.columns)


    # ========================================================
    # V2 — VELOCITY
    # ========================================================
    first_extracted_at = None
    last_extracted_at = None
    extraction_span_hours = None

    if "_extracted_at" in df.columns:

        extracted_at = pd.to_datetime(
            df["_extracted_at"],
            errors="coerce",
            utc=True
        )

        valid_times = extracted_at.dropna()

        if not valid_times.empty:

            first_extracted_at = valid_times.min()
            last_extracted_at = valid_times.max()

            extraction_span_hours = round(
                (
                    last_extracted_at
                    - first_extracted_at
                ).total_seconds() / 3600,
                2
            )


    bronze_files = (
        int(df["file_path"].nunique())
        if "file_path" in df.columns
        else None
    )


    # ========================================================
    # V3 — VARIETY
    # ========================================================
    schema_columns = list(df.columns)


    # ========================================================
    # V4 — VERACITY
    # ========================================================
    total_cells = row_count * column_count

    missing_cells = count_missing_cells(df)

    missing_percentage = (
        round(
            (missing_cells / total_cells) * 100,
            2
        )
        if total_cells > 0
        else 0
    )

    # CHECK only — nothing deleted
    duplicated_rows = count_duplicate_rows(df)


    # Title quality check
    title_column = find_existing_column(
        df,
        TITLE_CANDIDATES
    )

    if title_column is not None:

        missing_title_rows = int(
            df[title_column]
            .map(
                lambda x:
                x is None
                or (
                    isinstance(x, str)
                    and x.strip() == ""
                )
            )
            .sum()
        )

    else:
        missing_title_rows = None


    # --------------------------------------------------------
    # Save report row
    # --------------------------------------------------------
    four_v_rows.append({
        "source": source,

        # Volume
        "rows": row_count,
        "columns": column_count,

        # Velocity
        "bronze_files": bronze_files,
        "first_extracted_at": first_extracted_at,
        "last_extracted_at": last_extracted_at,
        "extraction_span_hours": extraction_span_hours,

        # Variety
        "schema_columns": schema_columns,

        # Veracity
        "missing_cells": missing_cells,
        "missing_percentage": missing_percentage,
        "duplicated_rows_check": duplicated_rows,
        "missing_title_rows": missing_title_rows,
    })


# ------------------------------------------------------------
# 4. Final report
# ------------------------------------------------------------
four_v_report = pd.DataFrame(four_v_rows)

print("✅ Silver 4V profiling completed")
print(f"Sources profiled: {len(four_v_report)}")
print(f"Total Bronze rows: {four_v_report['rows'].sum()}")

display(four_v_report)

# ============================================================
# SILVER STEP 2 — VERIFIED SELF-CONTAINED CLASSIFICATION
#
# Inputs:
#   pandas_dfs  (created by Silver 4V step)
#
# Outputs:
#   classified_dfs
#   technology_dfs  (Technology-only reporting subset; NOT the downstream Silver input)
#   review_dfs
#   nontech_dfs
#   classification_summary
#   decision_method_summary
#   classification_qa
#
# Design:
#   - No ML / joblib
#   - No dependency on main.py
#   - No personal Workspace path
#   - Strong title evidence -> Technology
#   - Strong Non-Tech title evidence -> Non-Tech
#   - Conflicts / ambiguous signals -> Review
#   - Oman exact source category "خدمات تقنية المعلومات" is
#     treated as authoritative IT evidence unless title has
#     explicit Non-Tech evidence
#   - No Azure writes; Bronze remains unchanged
# ============================================================

import re
import unicodedata
import numpy as np
import pandas as pd

if "pandas_dfs" not in globals():
    raise RuntimeError("pandas_dfs not found. Run Bronze extraction + 4V first.")

TITLE_FIELDS = ['Tender Subject', 'Title', 'عنوان_المناقصة', 'موضوع المناقصة', 'No./Tender Subject']
CONTEXT_FIELDS = ['Categories',
 'raw_text',
 'Value Label',
 'Activity',
 'المجال_والدرجة',
 'نوع القطاع المطلوب',
 'Tender Type',
 'Type',
 'Negotiation Type']

TECH_PHRASES = ['information technology',
 'it services',
 'it support',
 'helpdesk',
 'help desk',
 'information systems',
 'software',
 'software license',
 'software licenses',
 'software licensing',
 'operating system',
 'operating systems',
 'web application',
 'web applications',
 'website development',
 'website maintenance',
 'digital platform',
 'electronic platform',
 'online platform',
 'digital services',
 'electronic services',
 'database',
 'databases',
 'it accessories',
 'fleet tracking system',
 'tracking system services',
 'cloud services',
 'cloud service',
 'cloud platform',
 'cloud infrastructure',
 'cloud based',
 'microsoft 365',
 'office 365',
 'azure',
 'cybersecurity',
 'cyber security',
 'firewall',
 'web application firewall',
 'data loss prevention',
 'penetration testing',
 'application security',
 'threat intelligence',
 'privileged access management',
 'pam license',
 'beyondtrust',
 'crowdstrike',
 'trend micro',
 'trendmicro',
 'sonicwall',
 'computer server',
 'computer servers',
 'server infrastructure',
 'server',
 'servers',
 'computer hardware',
 'it hardware',
 'computer',
 'computers',
 'laptop',
 'laptops',
 'workstation',
 'workstations',
 'mini pc',
 'desktop computer',
 'surface pro',
 'surface studio',
 'imac',
 'thinkpad',
 'data tape',
 'data cartridge',
 'nas drive',
 'backup storage',
 'memory card',
 'usb drive',
 'network equipment',
 'network infrastructure',
 'network management',
 'network security',
 'internet connectivity',
 'computer network',
 'data network',
 'campus network',
 'switch catalyst',
 'cisco switch',
 'aruba',
 'access point',
 'ethernet',
 'voip',
 'ip telephone',
 'ip telephony',
 'cisco webex',
 'webex',
 'avaya',
 'wireless active components',
 'lan passive components',
 'data centre',
 'data center',
 'erp system',
 'erp solution',
 'crm system',
 'customer relationship management',
 'document management system',
 'cmdb',
 'asset tracking solution',
 'point of sale system',
 'attendance management system',
 'artificial intelligence',
 'machine learning',
 'business intelligence',
 'data analytics',
 'data platform',
 'intelligent platform',
 'statistical data analysis',
 'api integration',
 'system integration',
 'systems integration',
 'system analysis',
 'system quality assurance',
 'developer outsourcing',
 'application developers',
 'business continuity',
 'disaster recovery',
 'it administration & support services',
 'bug fix',
 'health check and bug fix',
 'end user devices',
 'smart led pixel screen',
 'audio visual over ip',
 'avoip',
 'digital forensics',
 '3d mapping',
 'digital mapping',
 'interactive 3d',
 'rfid barcode reader',
 'billing & accounting solution',
 'billing and accounting solution',
 'cobit',
 'deep freeze',
 'autocad',
 'oracle license',
 'oracle licenses',
 'sql server',
 'smart net cisco',
 'f5 licence',
 'f5 license',
 'تقنية المعلومات',
 'تكنولوجيا المعلومات',
 'خدمات تقنية المعلومات',
 'نظم المعلومات',
 'انظمة المعلومات',
 'برمجيات',
 'الامن السيبراني',
 'جدار حماية',
 'منصة الكترونية',
 'منصة رقمية',
 'خدمات الكترونية',
 'قواعد البيانات',
 'الخوادم',
 'خوادم',
 'اجهزة الحاسب',
 'اجهزة الحاسوب',
 'حاسب الي',
 'حواسيب',
 'لابتوب',
 'لابتوبات',
 'شبكات تقنية المعلومات',
 'بنية تحتية تقنية',
 'البنية التحتية التقنية',
 'البنية التحتية التكنولوجية',
 'الذكاء الاصطناعي',
 'مركز البيانات',
 'مراكز البيانات',
 'نظام نقاط البيع',
 'مكتب الدعم التقني',
 'لمكتب الدعم التقني',
 'محاكاة مراقبة الحركة الجوية',
 'صيانة وتطوير وتشغيل وضمان انظمة',
 'تطوير موقع الكتروني',
 'تصميم موقع الكتروني',
 'موقع الكتروني',
 'تطبيق ذكي',
 'تطوير نظام',
 'تطوير الانظمة',
 'رخص مايكروسوفت',
 'رخص اوراكل',
 'جدار حماية تطبيقات الويب',
 'تشفير قواعد البيانات',
 'الحوسبة السحابية',
 'نظام تخطيط موارد المؤسسات',
 'سويتشات',
 'محول شبكات',
 'سيرفر',
 'نظام الوصول المتميز',
 'vmware',
 'vsphere',
 'veeam',
 'proofpoint',
 'tenable',
 'palo alto',
 'fortinet',
 'fortigate',
 'symantec bluecoat',
 'ssd',
 'solid state drive',
 'barcode scanner',
 'barcode scanners',
 'label printers',
 'log360',
 'admanager',
 'admanager plus',
 'm-signer',
 'virtual private network',
 'vpn solution',
 'شبكة خاصة افتراضية',
 'الشبكة الخاصة الافتراضية',
 'cloud computing platform',
 'private cloud computing',
 'private cloud',
 'network and infrastructure support services',
 'network support services',
 'network devices',
 'network device',
 'أجهزة الشبكات',
 'أدوات الشبكة',
 'تجهيز وتشغيل الشبكة والإنترنت',
 'أجهزة تقنية',
 'ملحقات تقنية',
 'نظام التخزين',
 'رخص برامج',
 'تجديد رخص برامج',
 'شراء رخص لبرنامج',
 'تصميم وتطوير منصة وتطبيق',
 'تصميم وإطلاق الموقع الإلكتروني',
 'نظام إدارة الاستراتيجية',
 'تجديد الرخص وتوفير الدعم الفني',
 'data centre infrastructure',
 'data center infrastructure',
 'hardware support',
 'security red teaming',
 'configuration management database',
 'fuel management system',
 'iot',
 'internet of things',
 'smart remote monitoring solution',
 'smart interactive screens',
 'interactive screens',
 'security analytics platform',
 'microsoft teams',
 'نظام موارد المؤسسة',
 'erp',
 'ذكاء اصطناعي',
 'أدوات أمنية تقنية',
 'حل آمن للشبكة الخاصة الافتراضية',
 'technical support for the privileged access management system',
 'اجهزة الاتصالات',
 'انظمة الاتصالات',
 'نظم الاتصالات',
 'telecommunications equipment',
 'communications equipment',
 'enterprise resource system',
 'enterprise resource planning',
 'أجهزة الشبكة',
 'hudl',
 'نظام البدالة',
 'بدالة هاتف رقمية',
 'البدالة',
 'منصات أوراكل',
 'مايكروسوفت تيمز',
 'برامج التصميم والمونتاج',
 'تطبيق معلوماتي تفاعلي',
 'توصيلات شبكية',
 'نقاط الشبكة',
 'جهاز لاب توب',
 'برامج تعليمية',
 'رخص البرامج التعليمية',
 'برنامج لحماية البريد الإلكتروني',
 'جهاز التخزين',
 'جدار الحماية',
 'محولات الشبكة',
 'تراخيص محولات الشبكة',
 'أشرطة النسخ الاحتياطي',
 'اشرطة النسخ الاحتياطي',
 'اجهزة حاسوب',
 'أجهزة حاسوب',
 'كمبيوتر محمول',
 'حاسب الالي',
 'حاسب الآلي',
 'محطة عمل',
 'خادم',
 'خدمة تهيئة شبكة الانترنت',
 'خدمة تهيئة شبكة الإنترنت',
 'نظام إدارة أمن المعلومات',
 'نظام اداره امن المعلومات',
 'ترقية نظام إدارة امن المعلومات',
 'ترقية نظام إدارة أمن المعلومات',
 'تجديد رخص برنامج',
 'اتلاسين',
 'atlassian',
 'نظام تحليل المباريات',
 'نظام البث',
 'شبكة بعض الاجهزة',
 'شبكة بعض الأجهزة',
 'منصة اتفاقيات',
 'التحويل الرقمي',
 'نظام عرض مرئي',
 'نظام الاتصال',
 'نظام صوتي متكامل',
 'لنظام إدارة الاستراتيجية',
 'لنظام اداره الاستراتيجيه',
 'strategy360',
 'datacenter services',
 'aws professional support services',
 'aws support services',
 'digitalization strategy',
 'cyber exercise',
 'الطباعة ثلاثية الابعاد',
 'الطباعة ثلاثية الأبعاد',
 'للطباعة ثلاثية الابعاد',
 'للطباعة ثلاثية الأبعاد',
 'ملحقات الحاسب',
 'حاسب محمول',
 'اجهزة حاسب محمول',
 'أجهزة حاسب محمول',
 'الكمبيوتر المحمول',
 'الكمبيوتر المحمولة',
 'أجهزة الكمبيوتر المحمولة',
 'اجهزة الكمبيوتر المحمولة',
 'الدعم الفني لوحدات التخزين',
 'تجديد الدعم الفني لوحدات التخزين',
 'electronic account management',
 'google account management',
 'microsoft account management',
 'نظام لإدارة أجهزة المستخدمين',
 'نظام لادارة اجهزة المستخدمين',
 'إدارة أجهزة المستخدمين',
 'ادارة اجهزة المستخدمين',
 'نظام آلي لأرش',
 'نظام الي لارش',
 'توريد اجهزة و الأدوات الرقمية',
 'توريد أجهزة و الأدوات الرقمية',
 'الشاشات الرقمية',
 'منصة هدل',
 'هدل لتحليل كرة القدم',
 'national platform for judicial and legal training',
 'supply and implementation of the national platform',
 'agentic ai',
 'agentic ai infrastructure',
 'enterprise agentic ai platform',
 'aris support services',
 'aris',
 'cisco call manager',
 'crowdstrike falcon',
 'crowdstrike',
 'كراود سترايك',
 'بيوند ترست',
 'beyond trust',
 'lims integration',
 'lims',
 'الترجمة الذكية',
 'oracle drcc',
 'google gcp',
 'gcp',
 'اختبار أمن التطبيقات',
 'امن التطبيقات',
 'أمن التطبيقات',
 'تطوير مركز الذكاء الاصطناعي',
 'power bi',
 'aspen oneliner',
 'mdr xdr',
 'appian',
 'تقنيات الشبكات',
 'حلول أمن الشبكات',
 'حلول امن الشبكات',
 'talent network solution',
 'حل شبكة المواهب',
 'audio and video communication system',
 'track and trace platform',
 'sanctions & adverse media',
 'sanctions and adverse media',
 'health information exchange platform',
 'منصة تبادل المعلومات الصحية',
 'منصة وزارة التعليم العالي والبحث العلمي',
 'جدار ناري',
 'tasks system',
 'نظام التكليفات',
 'printing operations management system',
 'نظام إدارة عمليات الطباعة',
 'نظام اداره عمليات الطباعه',
 'elsevier-pure',
 'elsevier pure',
 'منصة إدارة وتشغيل الموارد الافتراضية',
 'منصة اداره وتشغيل الموارد الافتراضيه',
 'unified enterprise service management platform',
 'enterprise service management platform',
 'منصة موحدة لإدارة خدمات المؤسسة',
 'منصه موحده لاداره خدمات الموسسه',
 'نظام أمن المواقع والبرامج والتطبيقات',
 'نظام امن المواقع والبرامج والتطبيقات',
 'enterprise governed business communication platform',
 'business communication platform',
 'security orchestration automation',
 'استخبارات التهديدات',
 'اتمتة الاوركسترا الامنية',
 'اتمته الاوركسترا الامنيه',
 'ultrasound reporting software',
 'برنامج تقارير الموجات فوق الصوتية',
 'technical services and support for medium and big professional dishes',
 'professional dishes',
 'telecontrol hardware spares',
 'telecontrol',
 'أجهزة دفع الزكاة والوقف',
 'اجهزة دفع الزكاة والوقف',
 'it systems operation maintenance and application support',
 'application support',
 'ترقية نظام إدارة امن المعلوم',
 'ترقية نظام إدارة أمن المعلوم',
 'توريد اجهزة و الأدوات الرقمي',
 'توريد أجهزة و الأدوات الرقمي',
 'توريد منصة ا تفاقيات',
 'نظام صوتي متكا',
 'تطوير النظام الالي المتكامل',
 'صيانة شبكة مجلس البحث العلمي',
 'نظام صوت',
 'تجديد الدعم الفني لوحدات تخز',
 'اجهزة حاسب محمو',
 'أجهزة حاسب محمو',
 'توريد أجهزة الكمبيوتر المحمو',
 'توريد اجهزة الكمبيوتر المحمو',
 'اوراكل دي ار سي سي',
 'أوراكل دي ار سي سي',
 'قوقل جي سي بي',
 'جوجل جي سي بي',
 'مركز الذكاء االصطناعي',
 'الذكاء االصطناعي',
 'نظام إدارة الوصول المميز',
 'نظام اداره الوصول المميز',
 'إدارة الوصول المميز',
 'ادارة الوصول المميز',
 'المنصة الموحدة للموارد المائية',
 'المنصه الموحده للموارد المائيه',
 'مركز عمليات الأمن',
 'مركز عمليات الامن',
 'إدارة الأجهزة المحمولة',
 'ادارة الاجهزة المحمولة',
 'مشروع خدمة الإنترنت',
 'مشروع خدمة الانترنت',
 'تجديد تراخيص مايكروسوفت',
 'المساعد الذكي',
 'خدمات الاستجابة للحواد',
 'البنية الاتصالية',
 'البنيه الاتصاليه',
 'استكمال تطوير الشبكات',
 'حلول ودعم مايكر',
 'صيانة ودعم مركز بيا']
NONTECH_PHRASES = ['cleaning services',
 'cleaning and hospitality',
 'laundry services',
 'landscaping',
 'waste management',
 'security guards',
 'guarding services',
 'vehicle leasing',
 'purchase of vehicle',
 'supply of vehicles',
 'transportation services',
 'cargo van',
 'cctv',
 'surveillance camera',
 'surveillance cameras',
 'security camera',
 'security cameras',
 'ip camera',
 'security screening equipment',
 'security gate',
 'security gates',
 'access control system',
 'remote control security lock',
 'fire alarm',
 'fire suppression',
 'firefighting',
 'fire fighting',
 'fire equipment',
 'fire cable',
 'radiation dose reader',
 'x-ray',
 'printer toner',
 'printer toners',
 'toner cartridge',
 'toner cartridges',
 'toner',
 'toners',
 'ink cartridge',
 'ink cartridges',
 'color ribbon',
 'printer ribbon',
 'card cleaner',
 'printable cd',
 'stationery',
 'office stationery',
 'paper cup',
 'forklift battery',
 'counterfeit currency detector',
 'public relations',
 'social media content',
 'marketing services',
 'marketing project',
 'weekly brochure',
 'brochure',
 'digital roll up',
 'design and montage',
 'communications agency services',
 'event management',
 'ceremony organization',
 'exhibition design and build',
 'road construction',
 'civil works',
 'building construction',
 'geotechnical',
 'materials testing',
 'air conditioning',
 'chiller maintenance',
 'underfloor heating',
 'electrical equipment',
 'generator maintenance',
 'substation maintenance',
 'water pumps',
 'pump spares',
 'pipes and fittings',
 'stormwater network',
 'stormwater system',
 'medical consumables',
 'laboratory reagents',
 'laboratory chemicals',
 'ultrasound',
 'blood sugar meter',
 'support medical services',
 'furniture and fittings',
 'uniforms',
 'archival preservation',
 'archival storage materials',
 'tax consulting',
 'financial audit',
 'commemorative coins',
 'recruitment services',
 'خدمات النظافة',
 'خدمات الحراسة',
 'الحراسة الامنية',
 'كاميرات مراقبة',
 'كاميرا مراقبة',
 'توريد كاميرات',
 'كميرات مراقبة',
 'كميرا مراقبة',
 'الكاميرات الامنية',
 'بوابات امنية',
 'بوابة امنية',
 'اجهزة الحريق',
 'معدات الحريق',
 'انذار الحريق',
 'كيابل نظام الحريق',
 'احبار طابعات',
 'احبار',
 'قرطاسية',
 'ادوات مكتبية',
 'مستلزمات مكتبية',
 'بطارية رافعة',
 'كاشف تزوير عملات',
 'البروشور الاسبوعي',
 'خدمات التسويق',
 'مشروع التسويق',
 'تصميم و مونتاج',
 'تصميم ومونتاج',
 'خدمات العلاقات العامة',
 'كاميرا كانون',
 'كاميره احترافيه',
 'كاميرا احترافية',
 'كاميرات حرارية',
 'توثيق الرصيد الارشيفي',
 'قطع غيار اجهزة مكتبية',
 'بحث واستقطاب الكفاءات',
 'ملحقات الاجهزه الاعلاميه',
 'اعمال الطرق',
 'اعمال مدنية',
 'اجهزة التكييف',
 'مستهلكات طبية',
 'اجهزة المسح بالاشعة',
 'اجهزة اشعة',
 'معدات كهربائية',
 'عملات تذكارية',
 'برامج القيادات',
 'ادارة الموارد المالية',
 'مكتب دعم دافعي الضرائب',
 'تدفئة الارضية',
 'مخاطر الحريق والسلامة',
 'مخاطر الصحة المهنية',
 'استراتيجيات النقل',
 'الاستشارية للاعارة',
 'الموجات فوق الصوتية',
 'اعادة تاهيل مكاتب',
 'صيانة واعادة تاهيل مكاتب',
 'تاهيل مكاتب',
 'تجهيز مكاتب',
 'تعديل مكاتب',
 'office renovation',
 'office refurbishment',
 'office supplies',
 'office support services',
 'carbon steel',
 'pipe carbon steel',
 'central air conditioning',
 'air conditioning units',
 'sewage pumping',
 'pumping stations',
 'low voltage cables',
 'high voltage cables',
 'electrical transmission network',
 'شبكة النقل الكهربائي',
 'مشاريع الكهرباء',
 'إدارة مشاريع الكهرباء',
 'wastewater network',
 'treated wastewater network',
 'الحفاظات',
 'حفاظات',
 'مستلزمات جراحية',
 'ادوية طبية',
 'قفازات',
 'معقمات',
 'اسعافات اولية',
 'video production',
 'road and infrastructure design',
 'الهندسة المدنية',
 'قمصان رياضية',
 'sports shirts',
 'ups battery',
 'ups batteries',
 'glucose monitoring system',
 'medical gas system',
 'fire water',
 'high voltage',
 'low voltage',
 'solar system',
 'valve',
 'conference halls',
 'wayfinding system',
 'iso 9001',
 'quality management system',
 'chillers',
 'تسربات المياه',
 'نظام ري',
 'شبكة أنابيب',
 'نظام تصريف مياه',
 'نظام التكييف',
 'نظام حريق',
 'مكافحة الحرائق',
 'طاقة شمسية',
 'نظام طاقة شمسية',
 'نظام معالجة مياه',
 'نظام ضخ',
 'نظام مجفف الهواء الطبي',
 'التصوير المقطعي',
 'نظام مراقبه القلب',
 'وحدة تبريد',
 'نظام تمارين المشي',
 'حفر بئر',
 'نظام تصريف المي',
 'نظام وحدات التكيف',
 'نظام طاقة الشمس',
 'طاقة الشمس',
 'قفل باب رقمي',
 'جهاز قياس أبعاد رقمي',
 'نظام التحميل الآلي لنقالة',
 'نظام غاز البترول',
 'نظام معالجة ميا',
 'نظام الجودة',
 'نظام التشحيم',
 'نظام تهوية',
 'نظام مصدر الطاقة',
 'نظام الحريق',
 'الشاشات الإعلانية',
 'door lock cylinders',
 'piv infiltration',
 'extravasation monitoring system',
 'ups system',
 'wdd aged network',
 'drag camera system',
 'fine screen system',
 'tubli stp',
 'f1 2026 fan village',
 'medicalgas system']
NONTECH_OVERRIDES = ['صيانة وإعادة تاهيل مكاتب',
 'إعادة تأهيل مكاتب',
 'office renovation',
 'office refurbishment',
 'cctv',
 'surveillance camera',
 'access control system',
 'كاميرات مراقبة',
 'كاميرات المراقبة',
 'fire alarm',
 'firefighting',
 'fire fighting',
 'انذار الحريق',
 'اطفاء الحريق']
WEAK_TECH_HINTS = ['network',
 'platform',
 'system',
 'digital',
 'license',
 'licence',
 'data logger',
 'telecontrol',
 'hardware equipment',
 'technical support',
 'دعم فني',
 'شبكة',
 'منصة',
 'نظام',
 'رقمي',
 'رقمية',
 'رخص',
 'بنية تحتية تقنية',
 'البنية التحتية التقنية']

def normalize_text(value):
    if value is None:
        return ""
    text = str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(
        ch for ch in text
        if not unicodedata.combining(ch)
        and unicodedata.category(ch) != "Cf"
    )
    for old, new in {
        "أ": "ا", "إ": "ا", "آ": "ا", "ى": "ي",
        "ؤ": "و", "ئ": "ي", "ة": "ه", "ـ": ""
    }.items():
        text = text.replace(old, new)
    text = text.casefold()
    text = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", text)
    text = re.sub(r"[_\-/\\|(),:;.&+]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def is_missing(value):
    if value is None:
        return True
    if isinstance(value, (dict, list, tuple, set, np.ndarray)):
        return False
    try:
        return bool(pd.isna(value))
    except Exception:
        return False

def flatten_value(value):
    if value is None:
        return []
    if isinstance(value, dict):
        result = []
        for nested in value.values():
            result.extend(flatten_value(nested))
        return result
    if isinstance(value, (list, tuple, set, np.ndarray)):
        result = []
        for nested in list(value):
            result.extend(flatten_value(nested))
        return result
    if is_missing(value):
        return []
    text = str(value).strip()
    return [text] if text else []

def compile_phrases(phrases, allow_arabic_clitics=False):
    compiled = []
    seen = set()
    for original in sorted(phrases, key=lambda x: len(normalize_text(x)), reverse=True):
        phrase = normalize_text(original)
        if not phrase or phrase in seen:
            continue
        seen.add(phrase)
        escaped = re.escape(phrase).replace(r"\ ", r"\s+")
        if re.search(r"[\u0600-\u06FF]", phrase):
            if allow_arabic_clitics:
                # Arabic clitics may attach to a strong phrase:
                # لنظام / بالشبكة / والبرمجيات / للطباعة...
                prefix = r"(?:و)?(?:[فبكل])?(?:ال)?"
            else:
                # Weak/generic hints stay strict to avoid sending every
                # physical "...لنظام..." title to Review.
                prefix = r"(?:و)?(?:ال)?"
            pattern = re.compile(
                r"(?<!\S)" + prefix + escaped + r"(?!\S)"
            )
        else:
            pattern = re.compile(
                r"(?<!\w)" + escaped + r"(?!\w)",
                flags=re.IGNORECASE,
            )
        compiled.append((original, pattern))
    return compiled

COMPILED_TECH = compile_phrases(TECH_PHRASES, allow_arabic_clitics=True)
COMPILED_NONTECH = compile_phrases(NONTECH_PHRASES, allow_arabic_clitics=True)
COMPILED_OVERRIDES = compile_phrases(NONTECH_OVERRIDES, allow_arabic_clitics=True)
COMPILED_WEAK = compile_phrases(WEAK_TECH_HINTS, allow_arabic_clitics=False)

def find_compiled(normalized_text, compiled_phrases):
    if not normalized_text:
        return None
    for original, pattern in compiled_phrases:
        if pattern.search(normalized_text):
            return original
    return None

TECH_REGEX_RULES = [
    (
        "website_development",
        re.compile(
            r"\b(?:develop(?:ment|ing)?|design|maintenan\w*|launch)\b"
            r".{0,60}\bwebsite\b",
            re.IGNORECASE,
        ),
    ),
    (
        "website_development_reverse",
        re.compile(
            r"\bwebsite\b.{0,60}"
            r"\b(?:develop(?:ment|ing)?|design|maintenan\w*|launch)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "vendor_license_support",
        re.compile(
            r"\b(?:microsoft|adobe|oracle|vmware|veeam|proofpoint|trend\s*micro|"
            r"crowdstrike|tenable|palo\s+alto|cisco|beyondtrust|autodesk|"
            r"symantec|bluecoat)\b"
            r".{0,80}\b(?:licen[cs]\w*|subscription|renewal|support)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "network_it",
        re.compile(
            r"\bnetwork\b.{0,50}"
            r"\b(?:support|infrastructure|equipment|devices?|security|items?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "network_it_reverse",
        re.compile(
            r"\b(?:support|infrastructure|equipment|devices?|security)\b"
            r".{0,50}\bnetwork\b",
            re.IGNORECASE,
        ),
    ),
    (
        "cloud_it",
        re.compile(
            r"\b(?:private\s+)?cloud\b.{0,60}"
            r"\b(?:computing|platform|license|licenses|subscription|services?|infrastructure)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "software_support",
        re.compile(
            r"\bsoftware\b.{0,60}"
            r"\b(?:support|maintenance|upgrade|subscription|license|licenses)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "system_license",
        re.compile(
            r"\b(?:license|licence)\b.{0,110}\bsystem\b"
            r"|\bsystem\b.{0,70}\b(?:license|licence)\w*",
            re.IGNORECASE,
        ),
    ),
    (
        "hardware_ict",
        re.compile(
            r"\bhardware\s+equipment\b.{0,50}\bict\b"
            r"|\bict\b.{0,50}\bhardware\b",
            re.IGNORECASE,
        ),
    ),
]

def find_tech_regex(normalized_title):
    for rule_name, pattern in TECH_REGEX_RULES:
        if pattern.search(normalized_title):
            return rule_name
    return None

def get_title(record):
    for field in TITLE_FIELDS:
        value = record.get(field)
        if value is None or is_missing(value):
            continue
        if isinstance(value, (dict, list, tuple, set, np.ndarray)):
            continue
        text = str(value).strip()
        if text:
            return text
    return ""

def get_context(record):
    values = []
    for field in CONTEXT_FIELDS:
        if field in record:
            values.extend(flatten_value(record.get(field)))
    return " | ".join(values)

def oman_it_category(record):
    value = record.get("المجال_والدرجة")
    if value is None or is_missing(value):
        return False
    return "خدمات تقنية المعلومات" in str(value)

DOMINANT_TECH_PHRASES = {
    "software",
    "web application",
    "web applications",
    "website development",
    "website maintenance",
    "digital platform",
    "electronic platform",
    "ultrasound reporting software",
    "برنامج تقارير الموجات فوق الصوتية",
}

DOMINANT_TECH_REGEX = {
    "vendor_license_support",
    "software_support",
    "system_license",
    "cloud_it",
    "network_it",
    "network_it_reverse",
    "hardware_ict",
    "website_development",
    "website_development_reverse",
}

def classify_tender(record, source):
    title = get_title(record)
    context = get_context(record)

    nt = normalize_text(title)
    nc = normalize_text(context)

    # Missing / broken title stays Review.
    if not nt or len(nt) <= 2:
        return "Review", "bad_title", "Missing or unusably short title"

    # Explicit high-risk Non-Tech override runs first.
    override = find_compiled(nt, COMPILED_OVERRIDES)
    if override:
        return "Non-Tech", "nontech_override", override

    tech_rule = find_compiled(nt, COMPILED_TECH)
    tech_regex = find_tech_regex(nt)
    nontech_rule = find_compiled(nt, COMPILED_NONTECH)

    has_tech = bool(tech_rule or tech_regex)
    has_nontech = bool(nontech_rule)

    # Conflict: clear software/web/vendor/system-license evidence can dominate
    # a physical-domain word such as "ultrasound"; otherwise keep for review.
    if has_tech and has_nontech:
        dominant = (
            tech_rule in DOMINANT_TECH_PHRASES
            or tech_regex in DOMINANT_TECH_REGEX
        )
        if dominant:
            return "Technology", "tech_dominant", tech_rule or tech_regex
        return (
            "Review",
            "title_conflict",
            f"TECH={tech_rule or tech_regex} | NONTECH={nontech_rule}",
        )

    if has_nontech:
        return "Non-Tech", "nontech_title_rule", nontech_rule

    if tech_rule:
        return "Technology", "tech_title_rule", tech_rule

    if tech_regex:
        return "Technology", "tech_title_regex", tech_regex

    # Oman exposes an explicit procurement category for IT services.
    # Trust that exact source category only after explicit Non-Tech title
    # rules have already had a chance to reject physical/fire/CCTV/etc.
    if source == "oman_T_tendersBoard" and oman_it_category(record):
        return (
            "Technology",
            "source_it_category",
            "خدمات تقنية المعلومات",
        )

    # Secondary context is not allowed to directly promote other sources.
    context_tech = find_compiled(nc, COMPILED_TECH)
    if context_tech:
        return (
            "Review",
            "tech_context_review",
            f"Technology evidence exists only in context: {context_tech}",
        )

    # Narrow ambiguous cues remain Review.
    weak_hint = find_compiled(nt, COMPILED_WEAK)
    if weak_hint:
        return (
            "Review",
            "weak_tech_title_review",
            f"Possible Technology signal: {weak_hint}",
        )

    # No technology evidence: Non-Tech.
    return "Non-Tech", "no_tech_evidence", "No Technology evidence detected"

# ------------------------------------------------------------
# Classification cache
# The result depends ONLY on (source, title, context, Oman category).
# Bronze keeps every run's full snapshot, so the same tender appears once
# per run; classifying it once and reusing the answer gives identical
# results and avoids re-running ~all regex rules on every repeat.
# ------------------------------------------------------------
_CLASSIFY_CACHE = {}


def classify_tender_cached(record, source):
    key = (
        source,
        get_title(record),
        get_context(record),
        oman_it_category(record),
    )
    result = _CLASSIFY_CACHE.get(key)
    if result is None:
        result = classify_tender(record, source)
        _CLASSIFY_CACHE[key] = result
    return result


classified_dfs = {}
technology_dfs = {}
review_dfs = {}
nontech_dfs = {}
summary_rows = []

for source, original_df in pandas_dfs.items():
    df = original_df.copy().reset_index(drop=True)
    metadata_rows = []

    for _, row in df.iterrows():
        classification, method, reason = classify_tender_cached(
            row.to_dict(),
            source,
        )
        metadata_rows.append(
            {
                "_classification": classification,
                "_classification_method": method,
                "_classification_reason": reason,
                "_model_score": None,
            }
        )

    classified_df = pd.concat(
        [df, pd.DataFrame(metadata_rows)],
        axis=1,
    )

    classified_dfs[source] = classified_df

    technology_df = classified_df[
        classified_df["_classification"].eq("Technology")
    ].copy()

    review_df = classified_df[
        classified_df["_classification"].eq("Review")
    ].copy()

    nontech_df = classified_df[
        classified_df["_classification"].eq("Non-Tech")
    ].copy()

    technology_dfs[source] = technology_df
    review_dfs[source] = review_df
    nontech_dfs[source] = nontech_df

    summary_rows.append(
        {
            "source": source,
            "total": len(classified_df),
            "technology": len(technology_df),
            "review": len(review_df),
            "non_tech": len(nontech_df),
            "reconciled": (
                len(technology_df)
                + len(review_df)
                + len(nontech_df)
                == len(classified_df)
            ),
        }
    )

classification_summary = pd.DataFrame(summary_rows)

total_input = int(classification_summary["total"].sum())
total_technology = int(classification_summary["technology"].sum())
total_review = int(classification_summary["review"].sum())
total_nontech = int(classification_summary["non_tech"].sum())

expected_input = sum(len(df) for df in pandas_dfs.values())

assert total_input == expected_input, "Input row count changed"
assert total_technology + total_review + total_nontech == total_input, (
    "Classification reconciliation failed"
)
assert classification_summary["reconciled"].all(), (
    "One or more sources failed reconciliation"
)

decision_method_rows = []
for source, df in classified_dfs.items():
    counts = df["_classification_method"].value_counts()
    for method, count in counts.items():
        decision_method_rows.append(
            {
                "source": source,
                "method": method,
                "records": int(count),
            }
        )

decision_method_summary = pd.DataFrame(decision_method_rows)

# QA: show enough records to inspect the decision surface without
# modifying any data.
qa_rows = []
for source, df in classified_dfs.items():
    title_column = next(
        (candidate for candidate in TITLE_FIELDS if candidate in df.columns),
        None,
    )
    if title_column is None:
        continue

    for class_name, sample_size in (
        ("Technology", 12),
        ("Review", 12),
        ("Non-Tech", 8),
    ):
        sample = df[df["_classification"].eq(class_name)].head(sample_size)
        for _, row in sample.iterrows():
            qa_rows.append(
                {
                    "source": source,
                    "classification": class_name,
                    "title": row.get(title_column),
                    "method": row.get("_classification_method"),
                    "reason": row.get("_classification_reason"),
                }
            )

classification_qa = pd.DataFrame(qa_rows)

print("=" * 76)
print("VERIFIED SELF-CONTAINED TECHNOLOGY CLASSIFICATION")
print("=" * 76)
print(f"Total input:      {total_input}")
print(f"Technology:       {total_technology}")
print(f"Review:           {total_review}")
print(f"Non-Tech:         {total_nontech}")
print("-" * 76)

if total_input:
    print(f"Technology rate:  {total_technology / total_input * 100:.2f}%")
    print(f"Review rate:      {total_review / total_input * 100:.2f}%")
    print(f"Non-Tech rate:    {total_nontech / total_input * 100:.2f}%")

print("-" * 76)
print("No personal Workspace dependency")
print("No main.py / joblib / ML dependency")
print("Exact Oman IT source category supported")
print("Physical/fire/CCTV/office guards applied before category promotion")
print("No rows lost")
print("Bronze unchanged")
print("=" * 76)

display(classification_summary)
display(decision_method_summary)
display(classification_qa)

# ============================================================
# SILVER STEP 2A — CLASSIFICATION QUALITY CHECK
# Review representative samples before standardization
# ============================================================

import pandas as pd


TITLE_CANDIDATES = [
    "Tender Subject",
    "Title",
    "موضوع المناقصة",
    "عنوان_المناقصة",
]


def get_title_column(df):
    for column in TITLE_CANDIDATES:
        if column in df.columns:
            return column
    return None


qa_samples = []

# Sources that need the closest inspection
QA_SOURCES = [
    "oman_T_tendersBoard",
    "capt_kw",
    "bahrain",
    "forsah",
]


for source in QA_SOURCES:

    if source not in classified_dfs:
        continue

    df = classified_dfs[source]

    title_col = get_title_column(df)

    if title_col is None:
        continue

    for label in ["Technology", "Review", "Non-Tech"]:

        subset = df[
            df["_classification"] == label
        ].copy()

        if subset.empty:
            continue

        # Take a small representative sample only
        sample = subset.head(5)

        for _, row in sample.iterrows():

            qa_samples.append({
                "source": source,
                "classification": label,
                "title": row.get(title_col),
                "method": row.get("_classification_method"),
                "reason": row.get("_classification_reason"),
                "model_score": row.get("_model_score"),
            })


classification_qa = pd.DataFrame(qa_samples)

print("✅ Classification QA sample created")
display(classification_qa)

# ============================================================
# SILVER STEP 3 — VERIFIED UNIFIED SCHEMA + SOURCE FIXES
#                   + MONETARY STANDARDIZATION TO SAR
#
# Input:
#   classified_dfs   (output from the verified classifier)
#
# Outputs:
#   standardized_dfs
#   standardized_silver_df
#   standardization_report
#
# What this step does:
#   1) Maps all 10 sources to the project canonical schema.py
#   2) Fixes Kuwait values available inside raw_text
#   3) Preserves original monetary values/currency
#   4) Converts monetary fields to SAR with an auditable FX snapshot
#   5) Keeps document fee / temporary bond separate from tender value
#   6) Preserves source lineage and original record JSON
#
# What this step does NOT do:
#   - No deduplication
#   - No row deletion
#   - No Azure write
#   - No final date parsing (that is the Cleaning step)
# ============================================================

import json
import re
import warnings
import numpy as np
import pandas as pd


# ============================================================
# 1. REQUIRE CLASSIFICATION OUTPUT
# ============================================================

if "classified_dfs" not in globals():
    raise RuntimeError(
        "classified_dfs not found. Run the verified Classification first."
    )


# ============================================================
# 2. PROJECT CANONICAL SCHEMA
#    Matches schema.py in the team repository.
# ============================================================

CANONICAL_SCHEMA = [
    "tender_number",
    "title",
    "authority",
    "published_date",
    "closing_date",
    "sector_type",
    "tender_type",
    "document_fee",
    "currency",
    "source",
    "source_url",
    "scraped_at",
]


# ============================================================
# 3. SOURCE METADATA
# ============================================================

SOURCE_METADATA = {
    "bahrain": {
        "country": "Bahrain",
        "default_currency": "BHD",
    },
    "capt_kw": {
        "country": "Kuwait",
        "default_currency": "KWD",
    },
    "etimad": {
        "country": "Saudi Arabia",
        "default_currency": "SAR",
    },
    "forsah": {
        "country": "Saudi Arabia",
        "default_currency": "SAR",
    },
    "oman_T_tendersBoard": {
        "country": "Oman",
        "default_currency": "OMR",
    },
    "qatar": {
        "country": "Qatar",
        "default_currency": "QAR",
    },
    "qatar_foundation": {
        "country": "Qatar",
        "default_currency": "QAR",
    },
    "qatar_monaqasat": {
        "country": "Qatar",
        "default_currency": "QAR",
    },
    "uae_global": {
        "country": "United Arab Emirates",
        "default_currency": "AED",
    },
    "uae_mof": {
        "country": "United Arab Emirates",
        "default_currency": "AED",
    },
}


# ============================================================
# 4. FX SNAPSHOT TO SAR
#
# 1 unit of source currency = X SAR.
# We keep the rate + date in every monetary record for lineage.
# ============================================================

FX_RATE_DATE = "2026-09-21"

FX_RATES_TO_SAR = {
    "SAR": 1.00000,
    "AED": 1.02916,
    "QAR": 1.03020,
    "BHD": 9.97231,
    "OMR": 9.76628,
    "KWD": 12.25170,
}


# ============================================================
# 5. SOURCE COLUMN MAPPING
# ============================================================

COLUMN_MAP = {
    "bahrain": {
        "tender_number": ["Tender Number", "No."],
        "title": ["Tender Subject", "No./Tender Subject"],
        "authority": ["Purchasing Authority"],
        "published_date": ["Published Date"],
        "closing_date": ["Closing Date"],
        "sector_type": [],
        "tender_type": ["Tender Type"],
        "status": [],
        "document_fee": [],
        "currency": [],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": [],
        "source_url": ["Detail URL"],
    },

    "capt_kw": {
        "tender_number": ["Tender Number"],
        "title": ["Title"],
        "authority": ["Entity Name"],
        "published_date": ["Open Date"],
        "closing_date": ["Close Date"],
        "sector_type": [],
        "tender_type": ["Type"],
        "status": [],
        "document_fee": ["Document Price"],
        "currency": [],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": ["Insurance"],
        "source_url": ["Link"],
    },

    "etimad": {
        "tender_number": ["Item ID"],
        "title": ["Title"],
        "authority": ["Agency"],
        "published_date": ["Published Date"],
        "closing_date": ["Closing Date"],
        "sector_type": ["Activity"],
        "tender_type": ["Tender Type"],
        "status": [],
        "document_fee": [],
        "currency": [],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": [],
        "source_url": ["Detail URL"],
    },

    "forsah": {
        "tender_number": ["Item ID"],
        "title": ["Title"],
        "authority": [],
        "published_date": ["Published Date"],
        # Due Date contains the actual deadline/time and is preferred.
        "closing_date": ["Due Date", "Closing Date"],
        "sector_type": ["Categories"],
        "tender_type": ["Type"],
        "status": ["Status"],
        "document_fee": [],
        "currency": [],
        "tender_value_min": ["Value Min"],
        "tender_value_max": ["Value Max"],
        "temporary_bond": [],
        "source_url": ["Detail URL"],
    },

    "oman_T_tendersBoard": {
        "tender_number": ["رقم_المناقصة"],
        "title": ["عنوان_المناقصة"],
        "authority": ["الجهة_الحكومية"],
        "published_date": ["تاريخ_طرح_المناقصة"],
        "closing_date": ["تاريخ_إغلاق_العطاء"],
        "sector_type": ["المجال_والدرجة"],
        "tender_type": ["نوع_المناقصة"],
        "status": [],
        "document_fee": [],
        "currency": [],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": [],
        "source_url": ["Link"],
    },

    "qatar": {
        "tender_number": ["Item ID"],
        "title": ["Title"],
        "authority": [],
        "published_date": ["Published Date"],
        "closing_date": ["Closing Date"],
        "sector_type": [],
        "tender_type": [],
        "status": [],
        "document_fee": [],
        "currency": [],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": [],
        "source_url": ["Detail URL"],
    },

    "qatar_foundation": {
        "tender_number": ["Negotiation Number"],
        "title": ["Title"],
        "authority": [],
        "published_date": ["Posting Date", "Open Date"],
        "closing_date": ["Close Date"],
        "sector_type": [],
        "tender_type": ["Negotiation Type"],
        "status": ["Status"],
        "document_fee": [],
        "currency": [],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": [],
        "source_url": [],
    },

    "qatar_monaqasat": {
        "tender_number": ["رقم المناقصة"],
        "title": ["موضوع المناقصة"],
        "authority": ["الجهة"],
        "published_date": ["تاريخ الطرح"],
        "closing_date": ["تاريخ الإغلاق"],
        "sector_type": ["نوع القطاع المطلوب"],
        "tender_type": ["النوع"],
        "status": [],
        "document_fee": ["قيمة الوثائق (رق)"],
        "currency": ["_currency"],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": ["التأمين المؤقت (رق)"],
        "source_url": ["_page_url"],
    },

    "uae_global": {
        "tender_number": ["Item ID"],
        "title": ["Title"],
        "authority": [],
        "published_date": ["Published Date"],
        "closing_date": ["Closing Date"],
        "sector_type": [],
        "tender_type": [],
        "status": [],
        "document_fee": [],
        "currency": [],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": [],
        "source_url": ["Detail URL"],
    },

    "uae_mof": {
        "tender_number": ["RFQ Number"],
        "title": ["Title"],
        "authority": ["Entity Name"],
        "published_date": ["Open Date"],
        "closing_date": ["Close Date"],
        "sector_type": [],
        "tender_type": [],
        "status": [],
        "document_fee": [],
        "currency": [],
        "tender_value_min": [],
        "tender_value_max": [],
        "temporary_bond": [],
        "source_url": ["Link"],
    },
}


# ============================================================
# 6. SAFE VALUE HELPERS
# ============================================================

def is_null_value(value):
    if value is None:
        return True

    if isinstance(value, (dict, list, tuple, set, np.ndarray)):
        return False

    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def standard_scalar(value):
    if is_null_value(value):
        return None

    if isinstance(value, np.ndarray):
        value = value.tolist()

    if isinstance(value, set):
        value = list(value)

    if isinstance(value, (dict, list, tuple)):
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )

    text = str(value).strip()
    return text if text else None


def first_value(row, candidate_columns):
    for column in candidate_columns:
        if column not in row.index:
            continue

        value = standard_scalar(row[column])

        if value is not None:
            return value

    return None


# ============================================================
# 7. DATE-TEXT NORMALIZATION
#
# We are NOT converting to datetime yet.
# This only normalizes Arabic month names so the Cleaning step
# can parse them consistently.
# ============================================================

ARABIC_MONTHS = {
    "يناير": "January",
    "فبراير": "February",
    "مارس": "March",
    "أبريل": "April",
    "ابريل": "April",
    "مايو": "May",
    "يونيو": "June",
    "يوليو": "July",
    "أغسطس": "August",
    "اغسطس": "August",
    "سبتمبر": "September",
    "أكتوبر": "October",
    "اكتوبر": "October",
    "نوفمبر": "November",
    "ديسمبر": "December",
}


def normalize_date_text(value):
    text = standard_scalar(value)

    if text is None:
        return None

    for arabic_month, english_month in ARABIC_MONTHS.items():
        text = text.replace(arabic_month, english_month)

    text = re.sub(r"\s+", " ", text).strip()
    return text


# ============================================================
# 8. ROBUST MONEY PARSER
#
# Important:
#   "1000.000 د.ك"     -> 1000.0
#   "23,000.000 د.ك"  -> 23000.0
#   "1,000.00"        -> 1000.0
#
# We extract the numeric token BEFORE currency abbreviations,
# preventing dots inside "د.ك" from corrupting the number.
# ============================================================

ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩",
    "0123456789",
)


def parse_money(value):
    text = standard_scalar(value)

    if text is None:
        return np.nan

    text = text.translate(ARABIC_DIGITS)

    negative_parentheses = (
        "(" in text
        and ")" in text
    )

    match = re.search(
        r"[-+]?\d[\d,٬]*(?:[.٫]\d+)?",
        text,
    )

    if not match:
        return np.nan

    number = (
        match.group(0)
        .replace(",", "")
        .replace("٬", "")
        .replace("٫", ".")
    )

    try:
        amount = float(number)
    except Exception:
        return np.nan

    if negative_parentheses:
        amount = -abs(amount)

    return amount


# ============================================================
# 9. CURRENCY NORMALIZATION
# ============================================================

CURRENCY_ALIASES = {
    "sar": "SAR",
    "saudi riyal": "SAR",
    "saudi arabian riyal": "SAR",
    "ريال سعودي": "SAR",

    "aed": "AED",
    "uae dirham": "AED",
    "dirham": "AED",
    "درهم": "AED",
    "درهم اماراتي": "AED",

    "qar": "QAR",
    "qatari riyal": "QAR",
    "qatar riyal": "QAR",
    "ريال قطري": "QAR",

    "bhd": "BHD",
    "bahraini dinar": "BHD",
    "دينار بحريني": "BHD",

    "kwd": "KWD",
    "kuwaiti dinar": "KWD",
    "دينار كويتي": "KWD",
    "د.ك": "KWD",

    "omr": "OMR",
    "omani rial": "OMR",
    "ريال عماني": "OMR",
}


def normalize_currency(value, default_currency=None):
    text = standard_scalar(value)

    if text is not None:
        normalized = text.casefold().strip()

        if normalized in CURRENCY_ALIASES:
            return CURRENCY_ALIASES[normalized]

        if re.fullmatch(r"[A-Za-z]{3}", text):
            code = text.upper()
            if code in FX_RATES_TO_SAR:
                return code

    return default_currency


def convert_to_sar(amount, currency_code):
    if amount is None or pd.isna(amount):
        return np.nan

    rate = FX_RATES_TO_SAR.get(currency_code)

    if rate is None:
        return np.nan

    return round(
        float(amount) * float(rate),
        2,
    )


# ============================================================
# 10. KUWAIT RAW_TEXT RECOVERY
#
# The Kuwait extractor left Close Date / Document Price empty,
# but those values exist inside raw_text.
# We recover them here WITHOUT modifying the extractor/Bronze.
# ============================================================

def extract_pipe_field(raw_text, label):
    text = standard_scalar(raw_text)

    if text is None:
        return None

    match = re.search(
        rf"{re.escape(label)}\s*\|\s*([^|]+)",
        text,
        flags=re.IGNORECASE,
    )

    if not match:
        return None

    return match.group(1).strip()


def recover_kuwait_fields(row):
    raw_text = row.get("raw_text")

    return {
        "closing_date": extract_pipe_field(
            raw_text,
            "اخر موعد للعطاء",
        ),
        "document_fee": extract_pipe_field(
            raw_text,
            "السعر",
        ),
        "temporary_bond": extract_pipe_field(
            raw_text,
            "التأمين",
        ),
    }


# ============================================================
# 11. PRESERVE ORIGINAL RECORD FOR LINEAGE
# ============================================================

def row_to_json(row):
    payload = {}

    for column, value in row.items():
        if is_null_value(value):
            payload[column] = None
        elif isinstance(value, np.ndarray):
            payload[column] = value.tolist()
        elif isinstance(value, set):
            payload[column] = list(value)
        else:
            payload[column] = value

    return json.dumps(
        payload,
        ensure_ascii=False,
        default=str,
    )


# ============================================================
# 11b. SOURCE URL FALLBACKS
# The Etimad API does not return a detail link; build it from the
# encrypted tender id it does return ("Tender ID String").
# ============================================================

ETIMAD_DETAIL_URL = "https://tenders.etimad.sa/Tender/DetailsForVisitor?STenderId="


def build_source_url(row, source):
    if source == "etimad":
        existing = standard_scalar(row.get("Detail URL"))
        if existing:
            return None  # keep the scraped link
        token = standard_scalar(row.get("Tender ID String"))
        if token:
            from urllib.parse import quote
            return ETIMAD_DETAIL_URL + quote(str(token), safe="")
    return None


# ============================================================
# 12. STANDARDIZE ALL CLASSIFIED RECORDS
# ============================================================

standardized_dfs = {}
standardized_frames = []

unknown_sources = (
    set(classified_dfs.keys())
    - set(COLUMN_MAP.keys())
)

if unknown_sources:
    raise ValueError(
        f"Missing mapping for source(s): {sorted(unknown_sources)}"
    )


for source, source_df in classified_dfs.items():
    mapping = COLUMN_MAP[source]
    metadata = SOURCE_METADATA[source]

    standardized_rows = []

    for _, row in source_df.iterrows():
        published_date = first_value(
            row,
            mapping["published_date"],
        )

        closing_date = first_value(
            row,
            mapping["closing_date"],
        )

        document_fee_raw = first_value(
            row,
            mapping["document_fee"],
        )

        temporary_bond_raw = first_value(
            row,
            mapping["temporary_bond"],
        )

        # ----------------------------------------------------
        # Kuwait source repair from raw_text
        # ----------------------------------------------------

        if source == "capt_kw":
            kuwait_fields = recover_kuwait_fields(row)

            if closing_date is None:
                closing_date = kuwait_fields["closing_date"]

            if document_fee_raw is None:
                document_fee_raw = kuwait_fields["document_fee"]

            if temporary_bond_raw is None:
                temporary_bond_raw = kuwait_fields["temporary_bond"]

        # ----------------------------------------------------
        # Normalize raw date text only
        # ----------------------------------------------------

        published_date = normalize_date_text(
            published_date
        )

        closing_date = normalize_date_text(
            closing_date
        )

        # ----------------------------------------------------
        # Monetary values
        # ----------------------------------------------------

        document_fee_original = parse_money(
            document_fee_raw
        )

        tender_value_min_original = parse_money(
            first_value(
                row,
                mapping["tender_value_min"],
            )
        )

        tender_value_max_original = parse_money(
            first_value(
                row,
                mapping["tender_value_max"],
            )
        )

        temporary_bond_original = parse_money(
            temporary_bond_raw
        )

        has_any_money = any(
            pd.notna(value)
            for value in [
                document_fee_original,
                tender_value_min_original,
                tender_value_max_original,
                temporary_bond_original,
            ]
        )

        explicit_currency = first_value(
            row,
            mapping["currency"],
        )

        currency_original = normalize_currency(
            explicit_currency,
            default_currency=(
                metadata["default_currency"]
                if has_any_money
                else None
            ),
        )

        fx_rate = (
            FX_RATES_TO_SAR.get(currency_original)
            if currency_original is not None
            else None
        )

        # ----------------------------------------------------
        # IMPORTANT SEMANTICS
        #
        # document_fee        = document fee converted to SAR
        # tender_value_*_sar  = actual/range tender value in SAR
        # temporary_bond_*    = bond, kept separate
        #
        # We NEVER mix document fee or bond into tender value.
        # ----------------------------------------------------

        document_fee_sar = convert_to_sar(
            document_fee_original,
            currency_original,
        )

        tender_value_min_sar = convert_to_sar(
            tender_value_min_original,
            currency_original,
        )

        tender_value_max_sar = convert_to_sar(
            tender_value_max_original,
            currency_original,
        )

        temporary_bond_sar = convert_to_sar(
            temporary_bond_original,
            currency_original,
        )

        standardized_rows.append({
            # ------------------------------------------------
            # Canonical schema.py columns
            # ------------------------------------------------
            "tender_number": first_value(
                row,
                mapping["tender_number"],
            ),

            "title": first_value(
                row,
                mapping["title"],
            ),

            "authority": first_value(
                row,
                mapping["authority"],
            ),

            "published_date": published_date,
            "closing_date": closing_date,

            "sector_type": first_value(
                row,
                mapping["sector_type"],
            ),

            "tender_type": first_value(
                row,
                mapping["tender_type"],
            ),

            # Canonical document_fee is standardized to SAR.
            "document_fee": document_fee_sar,

            # Currency describes standardized monetary columns.
            "currency": (
                "SAR"
                if has_any_money and fx_rate is not None
                else None
            ),

            "source": source,

            "source_url": build_source_url(row, source) or first_value(
                row,
                mapping["source_url"],
            ),

            "scraped_at": standard_scalar(
                row.get("_extracted_at")
            ),

            # ------------------------------------------------
            # Additional Silver business fields
            # ------------------------------------------------
            "country": metadata["country"],

            "source_status": first_value(
                row,
                mapping["status"],
            ),

            # ------------------------------------------------
            # Monetary lineage — ORIGINAL values
            # ------------------------------------------------
            "document_fee_original": document_fee_original,
            "currency_original": currency_original,

            "tender_value_min_original": tender_value_min_original,
            "tender_value_max_original": tender_value_max_original,

            "temporary_bond_original": temporary_bond_original,

            # ------------------------------------------------
            # Monetary standardized values — SAR
            # ------------------------------------------------
            "tender_value_min_sar": tender_value_min_sar,
            "tender_value_max_sar": tender_value_max_sar,
            "temporary_bond_sar": temporary_bond_sar,

            "fx_rate_to_sar": fx_rate,
            "fx_rate_date": (
                FX_RATE_DATE
                if fx_rate is not None
                else None
            ),

            # ------------------------------------------------
            # Classification metadata
            # ------------------------------------------------
            "classification": standard_scalar(
                row.get("_classification")
            ),

            "classification_method": standard_scalar(
                row.get("_classification_method")
            ),

            "classification_reason": standard_scalar(
                row.get("_classification_reason")
            ),

            # ------------------------------------------------
            # Technical lineage
            # ------------------------------------------------
            "bronze_file_path": standard_scalar(
                row.get("file_path")
            ),

            "bronze_run_id": standard_scalar(
                row.get("_bronze_run_id")
            ),

            "bronze_run_date": standard_scalar(
                row.get("_bronze_run_date")
            ),

            "bronze_blob_name": standard_scalar(
                row.get("_bronze_blob_name")
            ),

            "source_page": standard_scalar(
                row.get("_page")
            ),

            "source_record_json": row_to_json(row),
        })

    standardized_df = pd.DataFrame(
        standardized_rows
    )

    standardized_dfs[source] = standardized_df
    standardized_frames.append(standardized_df)


# ============================================================
# 13. COMBINE ALL CLASSIFIED SOURCES
# ============================================================

if standardized_frames:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        standardized_silver_df = pd.concat(
            standardized_frames,
            ignore_index=True,
        )
else:
    standardized_silver_df = pd.DataFrame()


# ============================================================
# 14. COLUMN ORDER
# ============================================================

EXTRA_COLUMNS = [
    "country",
    "source_status",

    "document_fee_original",
    "currency_original",

    "tender_value_min_original",
    "tender_value_max_original",
    "tender_value_min_sar",
    "tender_value_max_sar",

    "temporary_bond_original",
    "temporary_bond_sar",

    "fx_rate_to_sar",
    "fx_rate_date",

    "classification",
    "classification_method",
    "classification_reason",

    "bronze_file_path",
    "bronze_run_id",
    "bronze_run_date",
    "bronze_blob_name",
    "source_page",
    "source_record_json",
]

ordered_columns = [
    column
    for column in (
        CANONICAL_SCHEMA + EXTRA_COLUMNS
    )
    if column in standardized_silver_df.columns
]

standardized_silver_df = (
    standardized_silver_df[
        ordered_columns
    ]
    .copy()
)


# ============================================================
# 15. QUALITY GATES
# ============================================================

expected_classified_rows = sum(
    len(df)
    for df in classified_dfs.values()
)

assert (
    len(standardized_silver_df)
    == expected_classified_rows
), "❌ Standardization changed classified row count"

assert all(
    column in standardized_silver_df.columns
    for column in CANONICAL_SCHEMA
), "❌ Canonical schema.py columns are incomplete"

assert (
    standardized_silver_df["title"]
    .notna()
    .all()
), "❌ Missing title after standardization"

ALLOWED_CLASSIFICATIONS = {"Technology", "Review", "Non-Tech"}

assert standardized_silver_df["classification"].notna().all(), (
    "❌ Missing classification after standardization"
)

assert set(standardized_silver_df["classification"].unique()).issubset(
    ALLOWED_CLASSIFICATIONS
), "❌ Unexpected classification value entered Silver path"

# Any row containing standardized monetary values must be SAR.
has_standardized_money = (
    standardized_silver_df[
        [
            "document_fee",
            "tender_value_min_sar",
            "tender_value_max_sar",
            "temporary_bond_sar",
        ]
    ]
    .notna()
    .any(axis=1)
)

assert (
    standardized_silver_df.loc[
        has_standardized_money,
        "currency",
    ]
    .eq("SAR")
    .all()
), "❌ Standardized monetary row is not marked SAR"

assert (
    standardized_silver_df.loc[
        has_standardized_money,
        "fx_rate_to_sar",
    ]
    .notna()
    .all()
), "❌ Monetary row is missing FX rate"


# ============================================================
# 16. VERIFY FX CALCULATIONS
# ============================================================

def validate_conversion(original_col, sar_col):
    check = (
        standardized_silver_df[original_col].notna()
        & standardized_silver_df["fx_rate_to_sar"].notna()
    )

    if not check.any():
        return True

    expected = (
        standardized_silver_df.loc[
            check,
            original_col,
        ]
        * standardized_silver_df.loc[
            check,
            "fx_rate_to_sar",
        ]
    ).round(2)

    actual = standardized_silver_df.loc[
        check,
        sar_col,
    ]

    return bool(
        np.isclose(
            expected.astype(float),
            actual.astype(float),
            rtol=0,
            atol=0.01,
        ).all()
    )


assert validate_conversion(
    "document_fee_original",
    "document_fee",
), "❌ Document fee SAR conversion mismatch"

assert validate_conversion(
    "tender_value_min_original",
    "tender_value_min_sar",
), "❌ Tender minimum value SAR conversion mismatch"

assert validate_conversion(
    "tender_value_max_original",
    "tender_value_max_sar",
), "❌ Tender maximum value SAR conversion mismatch"

assert validate_conversion(
    "temporary_bond_original",
    "temporary_bond_sar",
), "❌ Temporary bond SAR conversion mismatch"


# ============================================================
# 17. STANDARDIZATION REPORT
# ============================================================

standardization_report = pd.DataFrame({
    "metric": [
        "Classified input rows",
        "Standardized output rows",
        "Sources represented",
        "Missing title",
        "Missing tender number",
        "Missing authority",
        "Missing published date",
        "Missing closing date",
        "Missing source URL",
        "Rows with document fee SAR",
        "Rows with tender value min SAR",
        "Rows with tender value max SAR",
        "Rows with temporary bond SAR",
        "Money rows without FX rate",
    ],
    "value": [
        expected_classified_rows,
        len(standardized_silver_df),
        standardized_silver_df["source"].nunique(),
        int(standardized_silver_df["title"].isna().sum()),
        int(standardized_silver_df["tender_number"].isna().sum()),
        int(standardized_silver_df["authority"].isna().sum()),
        int(standardized_silver_df["published_date"].isna().sum()),
        int(standardized_silver_df["closing_date"].isna().sum()),
        int(standardized_silver_df["source_url"].isna().sum()),
        int(standardized_silver_df["document_fee"].notna().sum()),
        int(standardized_silver_df["tender_value_min_sar"].notna().sum()),
        int(standardized_silver_df["tender_value_max_sar"].notna().sum()),
        int(standardized_silver_df["temporary_bond_sar"].notna().sum()),
        int((has_standardized_money & standardized_silver_df["fx_rate_to_sar"].isna()).sum()),
    ],
})


# ============================================================
# 18. SOURCE-LEVEL REPORT
# ============================================================

source_standardization_report = (
    standardized_silver_df
    .groupby("source", dropna=False)
    .agg(
        rows=("title", "size"),
        missing_authority=("authority", lambda s: int(s.isna().sum())),
        missing_closing_date=("closing_date", lambda s: int(s.isna().sum())),
        missing_source_url=("source_url", lambda s: int(s.isna().sum())),
        document_fee_rows=("document_fee", lambda s: int(s.notna().sum())),
        tender_value_rows=("tender_value_min_sar", lambda s: int(s.notna().sum())),
    )
    .reset_index()
)


# ============================================================
# 19. KUWAIT VERIFICATION
# ============================================================

kuwait_verification = standardized_silver_df[
    standardized_silver_df["source"].eq("capt_kw")
][
    [
        "tender_number",
        "title",
        "published_date",
        "closing_date",
        "document_fee_original",
        "currency_original",
        "document_fee",
        "currency",
        "temporary_bond_original",
        "temporary_bond_sar",
    ]
].copy()


# ============================================================
# 20. OUTPUT
# ============================================================

print("=" * 78)
print("VERIFIED SILVER STANDARDIZATION + SAR NORMALIZATION")
print("=" * 78)

print(
    "Classified input:",
    expected_classified_rows,
)

print(
    "Standardized output:",
    len(standardized_silver_df),
)

print(
    "Sources represented:",
    standardized_silver_df["source"].nunique(),
)

print(
    "Canonical schema columns:",
    len(CANONICAL_SCHEMA),
)

print(
    "FX snapshot date:",
    FX_RATE_DATE,
)

print("-" * 78)
print("✅ Canonical schema.py names used")
print("✅ All classified row count preserved")
print("✅ Kuwait missing close date/document fee recovered from raw_text")
print("✅ Original monetary values preserved")
print("✅ Monetary values converted to SAR with stored FX rate")
print("✅ Document fee / bond are NOT mixed with tender value")
print("✅ Source lineage preserved")
print("✅ No Azure write performed")
print("=" * 78)


display(standardization_report)

display(source_standardization_report)

if not kuwait_verification.empty:
    print("\nKuwait repaired classified row:")
    display(kuwait_verification)

print("\nStandardized sample:")
display(standardized_silver_df.head(20))

# ============================================================
# SILVER STEP 4 — VERIFIED CLEANING + DATE / TYPE QUALITY
#
# Input:
#   standardized_silver_df
#
# Outputs:
#   cleaned_silver_df
#   cleaning_report
#   source_cleaning_report
#   cleaning_issue_rows
#
# Scope:
#   - Normalize null/text representations
#   - Preserve raw date text before parsing
#   - Parse dates using source-specific formats
#   - Normalize canonical dates to YYYY-MM-DD semantics
#   - Cast numeric fields safely
#   - Validate monetary lineage/SAR consistency
#   - Create row-level quality flags
#
# This step does NOT:
#   - deduplicate
#   - generate final tender_id
#   - delete/quarantine rows
#   - write to Azure
# ============================================================

import re
from datetime import datetime
import numpy as np
import pandas as pd


if "standardized_silver_df" not in globals():
    raise RuntimeError(
        "standardized_silver_df not found. Run verified Standardization first."
    )


# ============================================================
# 1. SAFE COPY
# ============================================================

cleaned_silver_df = (
    standardized_silver_df
    .copy()
    .reset_index(drop=True)
)

input_rows = len(cleaned_silver_df)


# ============================================================
# 2. NULL + TEXT NORMALIZATION
# ============================================================

NULL_LIKE = {
    "",
    "-",
    "--",
    "---",
    "n/a",
    "na",
    "none",
    "null",
    "nan",
    "not available",
    "not applicable",
}


def clean_text(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = str(value)
    text = re.sub(r"\s+", " ", text).strip()

    if text.casefold() in NULL_LIKE:
        return None

    return text


TEXT_COLUMNS = [
    "tender_number",
    "title",
    "authority",
    "sector_type",
    "tender_type",
    "currency",
    "source",
    "source_url",
    "country",
    "source_status",
    "currency_original",
    "classification",
    "classification_method",
    "classification_reason",
    "bronze_file_path",
    "bronze_run_id",
    "bronze_run_date",
    "bronze_blob_name",
]

for column in TEXT_COLUMNS:
    if column in cleaned_silver_df.columns:
        cleaned_silver_df[column] = (
            cleaned_silver_df[column]
            .map(clean_text)
        )


# Title is critical. Clean whitespace only; never translate/change meaning.
cleaned_silver_df["title"] = (
    cleaned_silver_df["title"]
    .map(clean_text)
)


# ============================================================
# 3. PRESERVE RAW DATE VALUES
# ============================================================

cleaned_silver_df["published_date_raw"] = (
    cleaned_silver_df["published_date"]
    .map(clean_text)
)

cleaned_silver_df["closing_date_raw"] = (
    cleaned_silver_df["closing_date"]
    .map(clean_text)
)

cleaned_silver_df["scraped_at_raw"] = (
    cleaned_silver_df["scraped_at"]
    .map(clean_text)
)


# ============================================================
# 4. DATE TEXT PRE-CLEANING
# ============================================================

ARABIC_MONTHS = {
    "يناير": "January",
    "فبراير": "February",
    "مارس": "March",
    "أبريل": "April",
    "ابريل": "April",
    "مايو": "May",
    "يونيو": "June",
    "يوليو": "July",
    "أغسطس": "August",
    "اغسطس": "August",
    "سبتمبر": "September",
    "أكتوبر": "October",
    "اكتوبر": "October",
    "نوفمبر": "November",
    "ديسمبر": "December",
}


def clean_date_text(value):
    text = clean_text(value)

    if text is None:
        return None

    for ar_month, en_month in ARABIC_MONTHS.items():
        text = text.replace(ar_month, en_month)

    # Known extractor issue:
    # 2026-09-23 2026-09-23 09:59 09:59
    text = re.sub(
        r"\b(\d{4}-\d{2}-\d{2})\s+\1\b",
        r"\1",
        text,
    )

    text = re.sub(
        r"\b(\d{1,2}:\d{2}(?::\d{2})?)\s+\1\b",
        r"\1",
        text,
    )

    text = re.sub(r"\s+", " ", text).strip()
    return text


cleaned_silver_df["published_date_raw"] = (
    cleaned_silver_df["published_date_raw"]
    .map(clean_date_text)
)

cleaned_silver_df["closing_date_raw"] = (
    cleaned_silver_df["closing_date_raw"]
    .map(clean_date_text)
)


# ============================================================
# 5. SOURCE-SPECIFIC DATE PARSING
#
# Explicit formats avoid day/month ambiguity.
# ============================================================

DATE_FORMATS = {
    "bahrain": {
        "published": ["%d, %b,%Y", "%d %b %Y", "%d %b,%Y"],
        "closing": ["%d %b,%Y", "%d, %b,%Y", "%d %b %Y"],
    },
    "capt_kw": {
        "published": ["%B %d, %Y", "%b %d, %Y"],
        "closing": ["%B %d, %Y", "%b %d, %Y"],
    },
    "etimad": {
        # Old HTML scraper: "2026-09-23 09:59"; new API: "2026-09-23T09:59:00"
        "published": [
            "%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M",
        ],
        "closing": [
            "%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M",
        ],
    },
    "forsah": {
        "published": ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"],
        "closing": ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"],
    },
    "oman_T_tendersBoard": {
        # List page is normalized to DD-MM-YYYY by the extractor, but values
        # from the detail page are stored as shown (YYYY-MM-DD or with "/").
        "published": ["%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"],
        "closing": ["%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"],
    },
    "qatar": {
        "published": ["%d %b %Y", "%d %B %Y"],
        "closing": ["%d %b %Y", "%d %B %Y"],
    },
    "qatar_foundation": {
        "published": ["%d-%b-%Y %I:%M %p", "%d-%b-%Y"],
        "closing": ["%d-%b-%Y %I:%M %p", "%d-%b-%Y"],
    },
    "qatar_monaqasat": {
        "published": ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S"],
        "closing": ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S"],
    },
    "uae_global": {
        "published": ["%d %b %Y", "%d %B %Y"],
        "closing": ["%d %b %Y", "%d %B %Y"],
    },
    "uae_mof": {
        "published": ["%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y"],
        "closing": ["%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y"],
    },
}


_DOTNET_DATE_RE = re.compile(r"/Date\((-?\d+)(?:[+-]\d{4})?\)/")
_ISO_START_RE = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}")


def parse_with_formats(value, formats):
    text = clean_date_text(value)

    if text is None:
        return pd.NaT

    # .NET JSON dates from APIs, e.g. "/Date(1758585540000)/" (ms since epoch)
    dotnet = _DOTNET_DATE_RE.search(text)
    if dotnet:
        try:
            return (
                pd.Timestamp(int(dotnet.group(1)), unit="ms", tz="UTC")
                .tz_convert("Asia/Riyadh")
                .tz_localize(None)
            )
        except Exception:
            return pd.NaT

    for fmt in formats:
        try:
            return pd.Timestamp(datetime.strptime(text, fmt))
        except Exception:
            pass

    # Conservative fallback only after all source-specific formats fail.
    # ISO-looking text (YYYY-MM-DD...) is parsed as ISO. Anything else is
    # parsed DAY-FIRST, because every portal in this project is day-first
    # (04/10/2026 = 4 October, never April 10).
    try:
        if _ISO_START_RE.match(text):
            ts = pd.Timestamp(text)
            return ts.tz_localize(None) if ts.tzinfo is not None else ts
        return pd.to_datetime(text, errors="coerce", dayfirst=True)
    except Exception:
        return pd.NaT


def parse_source_date(row, raw_column, kind):
    source = row.get("source")
    value = row.get(raw_column)

    formats = DATE_FORMATS.get(source, {}).get(kind, [])
    return parse_with_formats(value, formats)


cleaned_silver_df["published_date"] = (
    cleaned_silver_df.apply(
        lambda row: parse_source_date(
            row,
            "published_date_raw",
            "published",
        ),
        axis=1,
    )
)

cleaned_silver_df["closing_date"] = (
    cleaned_silver_df.apply(
        lambda row: parse_source_date(
            row,
            "closing_date_raw",
            "closing",
        ),
        axis=1,
    )
)

# Canonical business dates are date-level fields in the project schema.
cleaned_silver_df["published_date"] = pd.to_datetime(
    cleaned_silver_df["published_date"],
    errors="coerce",
).dt.normalize()

cleaned_silver_df["closing_date"] = pd.to_datetime(
    cleaned_silver_df["closing_date"],
    errors="coerce",
).dt.normalize()


# ============================================================
# 6. SCRAPED TIMESTAMP -> UTC
# ============================================================

def parse_utc_timestamp(value):
    """
    Parse ONE timestamp to UTC. Parsing value-by-value matters: pandas 2+
    infers a single format from the first row of a column, so mixed values
    (with / without microseconds, 'Z' vs '+00:00') silently became NaT.
    """
    if value is None:
        return pd.NaT
    try:
        if pd.isna(value):
            return pd.NaT
    except Exception:
        pass
    try:
        ts = pd.Timestamp(str(value).strip())
    except Exception:
        return pd.NaT
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


cleaned_silver_df["scraped_at"] = pd.to_datetime(
    cleaned_silver_df["scraped_at_raw"].map(parse_utc_timestamp),
    errors="coerce",
    utc=True,
)


# ============================================================
# 7. NUMERIC CASTING
# ============================================================

NUMERIC_COLUMNS = [
    "document_fee",
    "document_fee_original",
    "tender_value_min_original",
    "tender_value_max_original",
    "tender_value_min_sar",
    "tender_value_max_sar",
    "temporary_bond_original",
    "temporary_bond_sar",
    "fx_rate_to_sar",
]

for column in NUMERIC_COLUMNS:
    if column in cleaned_silver_df.columns:
        cleaned_silver_df[column] = pd.to_numeric(
            cleaned_silver_df[column],
            errors="coerce",
        )

if "source_page" in cleaned_silver_df.columns:
    cleaned_silver_df["source_page"] = pd.to_numeric(
        cleaned_silver_df["source_page"],
        errors="coerce",
    ).astype("Int64")


# ============================================================
# 8. COUNTRY + CURRENCY CODE NORMALIZATION
# ============================================================

COUNTRY_MAP = {
    "bahrain": "Bahrain",
    "kuwait": "Kuwait",
    "saudi arabia": "Saudi Arabia",
    "oman": "Oman",
    "qatar": "Qatar",
    "united arab emirates": "United Arab Emirates",
    "uae": "United Arab Emirates",
}

cleaned_silver_df["country"] = (
    cleaned_silver_df["country"]
    .map(lambda x: COUNTRY_MAP.get(x.casefold(), x) if isinstance(x, str) else x)
)

for currency_col in ["currency", "currency_original"]:
    if currency_col in cleaned_silver_df.columns:
        cleaned_silver_df[currency_col] = (
            cleaned_silver_df[currency_col]
            .map(lambda x: x.upper().strip() if isinstance(x, str) else x)
        )


# ============================================================
# 9. QUALITY MASKS
# ============================================================

published_parse_failed = (
    cleaned_silver_df["published_date_raw"].notna()
    & cleaned_silver_df["published_date"].isna()
)

closing_parse_failed = (
    cleaned_silver_df["closing_date_raw"].notna()
    & cleaned_silver_df["closing_date"].isna()
)

scraped_parse_failed = (
    cleaned_silver_df["scraped_at_raw"].notna()
    & cleaned_silver_df["scraped_at"].isna()
)

closing_before_published = (
    cleaned_silver_df["published_date"].notna()
    & cleaned_silver_df["closing_date"].notna()
    & (
        cleaned_silver_df["closing_date"]
        < cleaned_silver_df["published_date"]
    )
)

missing_title = cleaned_silver_df["title"].isna()
missing_tender_number = cleaned_silver_df["tender_number"].isna()
missing_authority = cleaned_silver_df["authority"].isna()
missing_closing_date = cleaned_silver_df["closing_date"].isna()
missing_source_url = cleaned_silver_df["source_url"].isna()


# ============================================================
# 10. MONEY VALIDATION
# ============================================================

SAR_COLUMNS = [
    "document_fee",
    "tender_value_min_sar",
    "tender_value_max_sar",
    "temporary_bond_sar",
]

negative_money = pd.Series(
    False,
    index=cleaned_silver_df.index,
)

for column in SAR_COLUMNS:
    negative_money = (
        negative_money
        | (
            cleaned_silver_df[column].notna()
            & (cleaned_silver_df[column] < 0)
        )
    )

invalid_value_range = (
    cleaned_silver_df["tender_value_min_sar"].notna()
    & cleaned_silver_df["tender_value_max_sar"].notna()
    & (
        cleaned_silver_df["tender_value_min_sar"]
        > cleaned_silver_df["tender_value_max_sar"]
    )
)

has_any_original_money = (
    cleaned_silver_df[
        [
            "document_fee_original",
            "tender_value_min_original",
            "tender_value_max_original",
            "temporary_bond_original",
        ]
    ]
    .notna()
    .any(axis=1)
)

money_missing_original_currency = (
    has_any_original_money
    & cleaned_silver_df["currency_original"].isna()
)

money_missing_fx = (
    has_any_original_money
    & cleaned_silver_df["fx_rate_to_sar"].isna()
)

# Standardized currency must be SAR only when a standardized monetary value exists.
has_any_sar_money = (
    cleaned_silver_df[SAR_COLUMNS]
    .notna()
    .any(axis=1)
)

invalid_standard_currency = (
    has_any_sar_money
    & ~cleaned_silver_df["currency"].eq("SAR")
)


# ============================================================
# 11. ROW-LEVEL QUALITY FLAGS
#
# Critical = cannot safely continue as an accepted Silver row.
# Warning  = data source is incomplete but row remains usable.
# ============================================================


def build_quality_flags(i):
    critical = []
    warning = []

    if missing_title.loc[i]:
        critical.append("missing_title")

    if published_parse_failed.loc[i]:
        critical.append("published_date_parse_failed")

    if scraped_parse_failed.loc[i]:
        critical.append("scraped_at_parse_failed")

    if negative_money.loc[i]:
        critical.append("negative_monetary_value")

    if invalid_value_range.loc[i]:
        critical.append("tender_value_min_gt_max")

    if money_missing_original_currency.loc[i]:
        critical.append("money_missing_original_currency")

    if money_missing_fx.loc[i]:
        critical.append("money_missing_fx_rate")

    if invalid_standard_currency.loc[i]:
        critical.append("invalid_standard_currency")

    if missing_tender_number.loc[i]:
        warning.append("missing_tender_number")

    if missing_authority.loc[i]:
        warning.append("missing_authority")

    if missing_closing_date.loc[i]:
        warning.append("missing_closing_date")

    if missing_source_url.loc[i]:
        warning.append("missing_source_url")

    if closing_parse_failed.loc[i]:
        warning.append("closing_date_parse_failed")

    if closing_before_published.loc[i]:
        warning.append("closing_before_published")

    return (
        "|".join(critical) if critical else None,
        "|".join(warning) if warning else None,
    )


flag_pairs = [
    build_quality_flags(i)
    for i in cleaned_silver_df.index
]

cleaned_silver_df["critical_quality_flags"] = [x[0] for x in flag_pairs]
cleaned_silver_df["warning_quality_flags"] = [x[1] for x in flag_pairs]

# Optional/source-level gaps are warnings only; they do NOT make an accepted
# Silver record fail the quality gate. Gold can use quality_status == "PASS"
# while still exposing warning_quality_flags for transparency.
cleaned_silver_df["has_quality_warning"] = (
    cleaned_silver_df["warning_quality_flags"].notna()
)

cleaned_silver_df["quality_status"] = np.where(
    cleaned_silver_df["critical_quality_flags"].notna(),
    "REJECT",
    "PASS",
)


# ============================================================
# 12. QUALITY GATES FOR THIS STAGE
# ============================================================

assert len(cleaned_silver_df) == input_rows, (
    "❌ Cleaning changed row count"
)

assert cleaned_silver_df["classification"].notna().all(), (
    "❌ Missing classification after cleaning"
)

assert set(cleaned_silver_df["classification"].unique()).issubset(
    ALLOWED_CLASSIFICATIONS
), "❌ Unexpected classification value after cleaning"

# Critical data problems are quarantined; they must NOT crash the whole
# Silver batch. Verify that every critical row is explicitly marked REJECT.
critical_problem_mask = (
    missing_title
    | published_parse_failed
    | scraped_parse_failed
    | negative_money
    | invalid_value_range
    | money_missing_original_currency
    | money_missing_fx
    | invalid_standard_currency
)

assert cleaned_silver_df.loc[
    critical_problem_mask,
    "quality_status",
].eq("REJECT").all(), (
    "❌ One or more critical-quality rows were not quarantined"
)

assert cleaned_silver_df.loc[
    ~critical_problem_mask,
    "quality_status",
].eq("PASS").all(), (
    "❌ A clean row was incorrectly rejected"
)


# ============================================================
# 13. REPORTS
# ============================================================

cleaning_report = pd.DataFrame({
    "metric": [
        "Input rows",
        "Output rows",
        "Missing title",
        "Missing tender number",
        "Missing authority",
        "Missing published date",
        "Missing closing date",
        "Missing source URL",
        "Published date parse failures",
        "Closing date parse failures",
        "scraped_at parse failures",
        "Closing before published",
        "Negative monetary values",
        "Invalid tender min/max ranges",
        "Money rows missing original currency",
        "Money rows missing FX rate",
        "Invalid standardized currency rows",
        "PASS rows",
        "Rows with warnings",
        "REJECT rows",
    ],
    "value": [
        input_rows,
        len(cleaned_silver_df),
        int(missing_title.sum()),
        int(missing_tender_number.sum()),
        int(missing_authority.sum()),
        int(cleaned_silver_df["published_date"].isna().sum()),
        int(missing_closing_date.sum()),
        int(missing_source_url.sum()),
        int(published_parse_failed.sum()),
        int(closing_parse_failed.sum()),
        int(scraped_parse_failed.sum()),
        int(closing_before_published.sum()),
        int(negative_money.sum()),
        int(invalid_value_range.sum()),
        int(money_missing_original_currency.sum()),
        int(money_missing_fx.sum()),
        int(invalid_standard_currency.sum()),
        int(cleaned_silver_df["quality_status"].eq("PASS").sum()),
        int(cleaned_silver_df["has_quality_warning"].sum()),
        int(cleaned_silver_df["quality_status"].eq("REJECT").sum()),
    ],
})

source_cleaning_report = (
    cleaned_silver_df
    .groupby("source", dropna=False)
    .agg(
        rows=("title", "size"),
        missing_authority=("authority", lambda s: int(s.isna().sum())),
        missing_closing_date=("closing_date", lambda s: int(s.isna().sum())),
        missing_source_url=("source_url", lambda s: int(s.isna().sum())),
        pass_rows=("quality_status", lambda s: int(s.eq("PASS").sum())),
        warning_rows=("has_quality_warning", lambda s: int(s.sum())),
        reject_rows=("quality_status", lambda s: int(s.eq("REJECT").sum())),
    )
    .reset_index()
)

cleaning_issue_rows = cleaned_silver_df[
    cleaned_silver_df["has_quality_warning"]
    | cleaned_silver_df["quality_status"].eq("REJECT")
].copy()


# ============================================================
# 14. OUTPUT
# ============================================================

print("=" * 78)
print("VERIFIED SILVER CLEANING + DATE / TYPE QUALITY")
print("=" * 78)
print("Input rows:", input_rows)
print("Output rows:", len(cleaned_silver_df))
print("Published date parse failures:", int(published_parse_failed.sum()))
print("Closing date parse failures:", int(closing_parse_failed.sum()))
print("Closing before published:", int(closing_before_published.sum()))
print("REJECT rows:", int(cleaned_silver_df["quality_status"].eq("REJECT").sum()))
print("-" * 78)
print("✅ Source-specific date parsing applied")
print("✅ Canonical dates normalized")
print("✅ Original date text preserved")
print("✅ Numeric fields cast safely")
print("✅ Monetary lineage / SAR consistency validated")
print("✅ Missing optional fields recorded as warnings, not invented")
print("✅ No rows removed")
print("✅ No Azure write performed")
print("=" * 78)

try:
    display(cleaning_report)
    display(source_cleaning_report)
except NameError:
    pass

# ============================================================
# SILVER STEP 5 — VERIFIED STABLE ID + DEDUPE + FINAL GATE
#
# Input:
#   cleaned_silver_df
#
# Outputs:
#   final_silver_df
#   duplicate_rows
#   rejected_rows
#   possible_cross_source_duplicates
#   final_quality_report
#   reconciliation_report
#   id_collision_report
#
# Rules:
#   - Critical rejects are quarantined, never silently deleted.
#   - Exact SAME-SOURCE semantic duplicates may be removed, keeping latest scrape.
#   - Historical snapshots with the SAME stable tender_id are collapsed to the
#     latest scrape; older snapshots are retained in a superseded report.
#   - True reissues/versions that resolve to a different tender_id are retained.
#   - Cross-source duplicates are FLAGGED only, never deleted automatically.
#   - tender_id is deterministic/stable and collision-safe for repeated source IDs.
#   - No Azure write is performed here.
# ============================================================

import hashlib
import re
import unicodedata
from itertools import combinations

import numpy as np
import pandas as pd


# ============================================================
# 1. REQUIRE CLEANING OUTPUT
# ============================================================

if "cleaned_silver_df" not in globals():
    raise RuntimeError(
        "cleaned_silver_df not found. Run verified Cleaning first."
    )

work_df = cleaned_silver_df.copy().reset_index(drop=True)
input_rows = len(work_df)

# ------------------------------------------------------------
# Compatibility normalization for accepted rows
#
# Some earlier Cleaning cells used PASS_WITH_WARNING while the
# verified Cleaning cell stores warnings separately and uses PASS.
# Normalize both representations here so the Final Gate is robust.
# ------------------------------------------------------------
if "warning_quality_flags" not in work_df.columns:
    work_df["warning_quality_flags"] = None

if "critical_quality_flags" not in work_df.columns:
    work_df["critical_quality_flags"] = None

if "has_quality_warning" not in work_df.columns:
    work_df["has_quality_warning"] = work_df["warning_quality_flags"].notna()

if "quality_status" not in work_df.columns:
    work_df["quality_status"] = np.where(
        work_df["critical_quality_flags"].notna(),
        "REJECT",
        "PASS",
    )
else:
    work_df["quality_status"] = work_df["quality_status"].replace({
        "PASS_WITH_WARNING": "PASS",
    })


# ============================================================
# 2. FINAL CORE SCHEMA REQUIRED BY PROJECT
# ============================================================

CANONICAL_SCHEMA = [
    "tender_number",
    "title",
    "authority",
    "published_date",
    "closing_date",
    "sector_type",
    "tender_type",
    "document_fee",
    "currency",
    "source",
    "source_url",
    "scraped_at",
]

missing_core_columns = [
    column for column in CANONICAL_SCHEMA
    if column not in work_df.columns
]

assert not missing_core_columns, (
    f"❌ Missing canonical Silver columns: {missing_core_columns}"
)


# ============================================================
# 3. NORMALIZATION HELPERS FOR KEYS ONLY
#    These do NOT modify displayed business values.
# ============================================================

def normalize_key_text(value):
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    text = unicodedata.normalize("NFKC", str(value)).casefold().strip()

    # Arabic letter normalization for matching/keys only.
    for old, new in {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
        "ؤ": "و",
        "ئ": "ي",
        "ة": "ه",
        "ـ": "",
    }.items():
        text = text.replace(old, new)

    text = re.sub(r"\s+", " ", text)
    return text.strip()


def compact_key_text(value):
    text = normalize_key_text(value)
    # Keep Unicode letters/numbers only. This works for Arabic and English.
    return "".join(ch for ch in text if ch.isalnum())


def date_key(value):
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    try:
        return pd.Timestamp(value).strftime("%Y-%m-%d")
    except Exception:
        return normalize_key_text(value)


def sha24(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


# ============================================================
# 4. SOURCE TENDER ID + REPEATED-ID COLLISION CHECK
#
# A source tender number is preferred when it uniquely identifies a record.
# Some real sources reuse the same number for reissues/versions. When that
# occurs, source_url or title+published_date is appended so IDs do not collide.
# ============================================================

work_df["source_tender_id"] = work_df["tender_number"].copy()
work_df["_source_norm"] = work_df["source"].map(compact_key_text)
work_df["_source_tender_id_norm"] = work_df["source_tender_id"].map(compact_key_text)

has_source_id = work_df["_source_tender_id_norm"].ne("")

source_id_group_size = pd.Series(1, index=work_df.index, dtype="int64")

if has_source_id.any():
    counts = (
        work_df.loc[has_source_id]
        .groupby(["_source_norm", "_source_tender_id_norm"], dropna=False)
        .size()
    )

    for i in work_df.index[has_source_id]:
        key = (
            work_df.at[i, "_source_norm"],
            work_df.at[i, "_source_tender_id_norm"],
        )
        source_id_group_size.at[i] = int(counts.loc[key])

work_df["source_id_group_size"] = source_id_group_size
work_df["source_id_reused"] = work_df["source_id_group_size"].gt(1)


def build_business_key(row):
    """
    Build a batch-independent identity key.

    Incremental Silver cannot decide identity from "how many times this ID
    appears in the current batch", because a later run may contain only one
    snapshot. Use stable business fields instead:

      source + source_tender_id + published_date

    Published date disambiguates a true reissue that reuses the same source ID,
    while closing date/status are intentionally excluded because they can change
    during the life of the same tender.

    If published_date is unavailable, fall back to source URL, then title.
    """
    source = compact_key_text(row.get("source"))
    source_id = compact_key_text(row.get("source_tender_id"))
    url = normalize_key_text(row.get("source_url"))
    title = compact_key_text(row.get("title"))
    published = date_key(row.get("published_date"))
    closing = date_key(row.get("closing_date"))

    if source_id:
        if published:
            return (
                f"{source}|source_id={source_id}"
                f"|published={published}"
            )

        if url:
            return (
                f"{source}|source_id={source_id}"
                f"|url={url}"
            )

        return (
            f"{source}|source_id={source_id}"
            f"|title={title}"
        )

    # Fallback when a source does not provide its own ID.
    # closing_date is included only in this weakest fallback because there is
    # no source ID to anchor the identity.
    return (
        f"{source}|title={title}|published={published}"
        f"|closing={closing}|url={url}"
    )


work_df["business_key"] = work_df.apply(build_business_key, axis=1)
work_df["tender_id"] = work_df["business_key"].map(sha24)

# Human-friendly display code.
# IMPORTANT:
# - tender_id remains the technical upsert key.
# - tender_code is only for Gold/dashboard display.
# - A short deterministic suffix from tender_id keeps the display code unique
#   and stable across rebuild/incremental runs.
SOURCE_PREFIX = {
    "etimad": "ETM",
    "forsah": "FOR",
    "bahrain": "BHR",
    "capt_kw": "KWT",
    "oman_T_tendersBoard": "OMN",
    "qatar": "QAT",
    "qatar_foundation": "QF",
    "qatar_monaqasat": "QMN",
    "uae_global": "UAEG",
    "uae_mof": "UAEM",
}


def clean_display_token(value):
    """
    Convert a source tender number into a readable code fragment.

    Examples:
      4722/2026   -> 4722-2026
      QF-RFQ-1409 -> QF-RFQ-1409

    Unicode letters/numbers are preserved; punctuation/whitespace is collapsed
    to a single hyphen.
    """
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    text = unicodedata.normalize("NFKC", str(value)).strip()
    if not text:
        return ""

    parts = []
    current = []

    for ch in text:
        if ch.isalnum():
            current.append(ch.upper())
        else:
            if current:
                parts.append("".join(current))
                current = []

    if current:
        parts.append("".join(current))

    return "-".join(part for part in parts if part)


def build_tender_code(row):
    """
    Build a readable, stable display code.

    Format when source_tender_id exists:
      <SOURCE_PREFIX>-<SOURCE_TENDER_ID>-<8-char technical suffix>

    Fallback when the source has no tender number:
      <SOURCE_PREFIX>-<8-char technical suffix>

    The suffix comes from tender_id, so the code stays stable across runs and
    remains unique even when a source reuses tender numbers.
    """
    source = str(row.get("source") or "").strip()
    prefix = SOURCE_PREFIX.get(source, "TND")

    source_id = clean_display_token(row.get("source_tender_id"))
    short_id = str(row.get("tender_id") or "").strip().upper()[:8]

    if not short_id:
        short_id = sha24(str(row.get("business_key") or ""))[:8].upper()

    if source_id:
        return f"{prefix}-{source_id}-{short_id}"

    return f"{prefix}-{short_id}"


work_df["tender_code"] = work_df.apply(build_tender_code, axis=1)
# ============================================================
# 5. ID COLLISION REPORT
#    A duplicate tender_id with a DIFFERENT business_key would be a hash bug.
# ============================================================

id_collision_report = (
    work_df.groupby("tender_id", dropna=False)
    .agg(
        rows=("tender_id", "size"),
        business_keys=("business_key", "nunique"),
    )
    .reset_index()
)

id_collision_report = id_collision_report[
    id_collision_report["business_keys"].gt(1)
].copy()

assert id_collision_report.empty, "❌ tender_id hash collision detected"


# ============================================================
# 6. EXACT SAME-SOURCE SEMANTIC DUPLICATE FINGERPRINT
#
# Technical lineage such as scraped_at/file_path is intentionally excluded.
# If the business content changed (dates/status/value/etc.), it is NOT treated
# as an exact duplicate and is retained as a separate version/reissue.
# ============================================================

FINGERPRINT_COLUMNS = [
    "source",
    "tender_number",
    "title",
    "authority",
    "published_date",
    "closing_date",
    "sector_type",
    "tender_type",
    "document_fee",
    "currency",
    "source_url",
    "country",
    "source_status",
    "document_fee_original",
    "currency_original",
    "tender_value_min_original",
    "tender_value_max_original",
    "tender_value_min_sar",
    "tender_value_max_sar",
    "temporary_bond_original",
    "temporary_bond_sar",
]


def fingerprint_value(value):
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if isinstance(value, (float, np.floating)):
        return f"{float(value):.10f}"

    return normalize_key_text(value)


def build_record_fingerprint(row):
    parts = []

    for column in FINGERPRINT_COLUMNS:
        parts.append(
            f"{column}={fingerprint_value(row.get(column))}"
        )

    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


work_df["record_fingerprint"] = work_df.apply(
    build_record_fingerprint,
    axis=1,
)

# Keep the latest scrape only when semantic business content is
# exactly identical. Different versions are never collapsed here.
work_df["_original_order"] = np.arange(len(work_df))

sort_df = work_df.sort_values(
    by=["record_fingerprint", "scraped_at", "_original_order"],
    ascending=[True, False, True],
    na_position="last",
).copy()

sort_df["exact_duplicate_rank"] = (
    sort_df.groupby("record_fingerprint", dropna=False)
    .cumcount()
)

work_df = (
    sort_df
    .sort_values("_original_order")
    .reset_index(drop=True)
)

work_df["is_exact_duplicate"] = work_df["exact_duplicate_rank"].gt(0)


# ============================================================
# 7. CROSS-SOURCE DUPLICATE CANDIDATES — FLAG ONLY
#
# We NEVER delete across sources automatically.
# Evidence:
#   A) same explicit external tender reference embedded in titles, OR
#   B) very high title-token similarity in same country with compatible dates.
# ============================================================

REFERENCE_REGEX = re.compile(
    r"\b[A-Z0-9]+(?:[\-/][A-Z0-9]+){2,}\b",
    flags=re.IGNORECASE,
)


def extract_external_refs(value):
    if value is None:
        return set()

    text = unicodedata.normalize("NFKC", str(value)).upper()
    refs = set()

    for match in REFERENCE_REGEX.findall(text):
        cleaned = re.sub(r"\s+", "", match)
        # Require at least one digit so generic slash phrases are ignored.
        if any(ch.isdigit() for ch in cleaned):
            refs.add(cleaned)

    return refs


def title_tokens(value):
    text = normalize_key_text(value)
    tokens = re.findall(r"[\w\u0600-\u06FF]+", text, flags=re.UNICODE)

    STOP = {
        "the", "of", "for", "and", "to", "a", "an", "in", "on",
        "qatar", "قطر", "project", "tender", "مناقصه", "توريد", "تقديم",
    }

    return {
        token for token in tokens
        if len(token) >= 3 and token not in STOP
    }


def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


work_df["_external_refs"] = work_df["title"].map(extract_external_refs)
work_df["_title_tokens"] = work_df["title"].map(title_tokens)
work_df["possible_cross_source_duplicate"] = False
work_df["cross_source_duplicate_reason"] = None

# A) shared explicit reference across different sources.
ref_to_indices = {}

for i, refs in work_df["_external_refs"].items():
    for ref in refs:
        ref_to_indices.setdefault(ref, []).append(i)

for ref, indices in ref_to_indices.items():
    sources = set(work_df.loc[indices, "source"].dropna().tolist())

    if len(sources) > 1:
        for i in indices:
            work_df.at[i, "possible_cross_source_duplicate"] = True
            existing = work_df.at[i, "cross_source_duplicate_reason"]
            reason = f"shared_reference:{ref}"
            work_df.at[i, "cross_source_duplicate_reason"] = (
                reason if not existing else f"{existing}|{reason}"
            )

# B) high title similarity within same country and across different sources.
# This is intentionally conservative and only creates a review flag.
#
# Performance:
#   The old version compared EVERY pair of rows in a country with a Python
#   loop (O(n^2)) and ran on all Bronze history BEFORE dedupe, so it slowed
#   down quadratically as history grew. Now:
#     1) only one representative row per tender_id is compared
#        (snapshots of the same tender are identical for this purpose),
#     2) candidate pairs come from an inverted token index, so only titles
#        that share at least one token are ever compared.
#   A pair with Jaccard >= 0.82 must share most of its tokens, so no real
#   match is lost. Flags are copied back to every row with that tender_id.
CROSS_SOURCE_SIMILARITY = 0.82
MAX_TOKEN_POSTINGS = 400   # tokens in more titles than this are too common to be useful

work_df["_country_norm_for_cross_source"] = (
    work_df["country"].map(normalize_key_text)
)

_rep = work_df.drop_duplicates(subset="tender_id", keep="first")
_similarity_hits = {}   # tender_id -> list of reasons

for country_norm, country_group in _rep.groupby(
    "_country_norm_for_cross_source",
    dropna=False,
):
    if not country_norm:
        continue
    if country_group["source"].nunique(dropna=True) < 2:
        continue

    ids = country_group["tender_id"].tolist()
    sources = country_group["source"].tolist()
    tokens = country_group["_title_tokens"].tolist()
    closes = country_group["closing_date"].tolist()

    postings = {}
    for pos, toks in enumerate(tokens):
        for tok in toks:
            postings.setdefault(tok, []).append(pos)

    candidate_pairs = set()
    for tok, plist in postings.items():
        if len(plist) < 2 or len(plist) > MAX_TOKEN_POSTINGS:
            continue
        for a_i in range(len(plist)):
            a = plist[a_i]
            for b in plist[a_i + 1:]:
                if sources[a] != sources[b]:
                    candidate_pairs.add((a, b))

    for a, b in candidate_pairs:
        similarity = jaccard(tokens[a], tokens[b])
        if similarity < CROSS_SOURCE_SIMILARITY:
            continue

        close_a, close_b = closes[a], closes[b]
        if pd.notna(close_a) and pd.notna(close_b):
            dates_compatible = abs(
                (pd.Timestamp(close_a) - pd.Timestamp(close_b)).days
            ) <= 7
        else:
            dates_compatible = pd.isna(close_a) and pd.isna(close_b)

        if not dates_compatible:
            continue

        reason = f"high_title_similarity:{similarity:.2f}"
        for pos in (a, b):
            _similarity_hits.setdefault(ids[pos], []).append(reason)

if _similarity_hits:
    for i, tid in work_df["tender_id"].items():
        reasons = _similarity_hits.get(tid)
        if not reasons:
            continue
        work_df.at[i, "possible_cross_source_duplicate"] = True
        existing = work_df.at[i, "cross_source_duplicate_reason"]
        joined = "|".join(reasons)
        work_df.at[i, "cross_source_duplicate_reason"] = (
            joined if not existing else f"{existing}|{joined}"
        )


# ============================================================
# 8. QUARANTINE / DUPLICATE SPLIT
# ============================================================

rejected_rows = work_df[
    work_df["quality_status"].eq("REJECT")
].copy()

# Exact duplicates are considered only among otherwise-valid PASS rows.
duplicate_rows = work_df[
    work_df["quality_status"].eq("PASS")
    & work_df["is_exact_duplicate"]
].copy()

# ------------------------------------------------------------
# Historical snapshot collapse
# ------------------------------------------------------------
# After exact-duplicate removal, the same stable tender_id may still have
# multiple historical states (for example a status/value update on a later
# scrape). Silver is the current clean analytical state, so keep the newest
# scrape for that SAME tender_id and retain older states in an audit report.
# True reissues that received a different tender_id are untouched.

work_df["is_superseded_snapshot"] = False

pass_non_exact = work_df[
    work_df["quality_status"].eq("PASS")
    & ~work_df["is_exact_duplicate"]
].copy()

if not pass_non_exact.empty:
    pass_non_exact["_snapshot_original_order"] = np.arange(
        len(pass_non_exact)
    )

    snapshot_sort = pass_non_exact.sort_values(
        by=[
            "tender_id",
            "scraped_at",
            "bronze_run_date",
            "bronze_run_id",
            "_snapshot_original_order",
        ],
        ascending=[True, False, False, False, True],
        na_position="last",
    ).copy()

    snapshot_sort["_snapshot_rank"] = (
        snapshot_sort.groupby("tender_id", dropna=False).cumcount()
    )

    superseded_indices = snapshot_sort.loc[
        snapshot_sort["_snapshot_rank"].gt(0)
    ].index

    work_df.loc[superseded_indices, "is_superseded_snapshot"] = True

superseded_rows = work_df[
    work_df["quality_status"].eq("PASS")
    & ~work_df["is_exact_duplicate"]
    & work_df["is_superseded_snapshot"]
].copy()

final_silver_df = work_df[
    work_df["quality_status"].eq("PASS")
    & ~work_df["is_exact_duplicate"]
    & ~work_df["is_superseded_snapshot"]
].copy()


# ============================================================
# 9. FINAL SILVER COLUMN ORDER
# ============================================================

FINAL_FRONT_COLUMNS = [
    "tender_id",
    "tender_code",
    "source_tender_id",
    "tender_number",
    "title",
    "authority",
    "published_date",
    "closing_date",
    "sector_type",
    "tender_type",
    "document_fee",
    "currency",
    "source",
    "country",
    "source_url",
    "scraped_at",
]

FINAL_EXTRA_COLUMNS = [
    "source_status",
    "document_fee_original",
    "currency_original",
    "tender_value_min_original",
    "tender_value_max_original",
    "tender_value_min_sar",
    "tender_value_max_sar",
    "temporary_bond_original",
    "temporary_bond_sar",
    "fx_rate_to_sar",
    "fx_rate_date",
    "classification",
    "classification_method",
    "classification_reason",
    "critical_quality_flags",
    "warning_quality_flags",
    "has_quality_warning",
    "quality_status",
    "possible_cross_source_duplicate",
    "cross_source_duplicate_reason",
    "source_id_group_size",
    "source_id_reused",
    "business_key",
    "record_fingerprint",
    "published_date_raw",
    "closing_date_raw",
    "scraped_at_raw",
    "bronze_file_path",
    "bronze_run_id",
    "bronze_run_date",
    "bronze_blob_name",
    "source_page",
    "source_record_json",
]

ordered_columns = [
    column
    for column in FINAL_FRONT_COLUMNS + FINAL_EXTRA_COLUMNS
    if column in final_silver_df.columns
]

remaining_columns = [
    column
    for column in final_silver_df.columns
    if column not in ordered_columns
    and not column.startswith("_")
    and column not in {"exact_duplicate_rank", "is_exact_duplicate"}
]

final_silver_df = final_silver_df[
    ordered_columns + remaining_columns
].reset_index(drop=True)

possible_cross_source_duplicates = final_silver_df[
    final_silver_df["possible_cross_source_duplicate"]
].copy()


# ============================================================
# 10. FINAL QUALITY GATES
# ============================================================

assert (
    len(final_silver_df)
    + len(duplicate_rows)
    + len(superseded_rows)
    + len(rejected_rows)
    == input_rows
), (
    "❌ Reconciliation failed: input != accepted + exact_duplicates "
    "+ superseded_snapshots + rejected"
)

assert final_silver_df["classification"].notna().all(), (
    "❌ Missing classification in Final Silver"
)

assert set(final_silver_df["classification"].unique()).issubset(
    ALLOWED_CLASSIFICATIONS
), "❌ Unexpected classification value in Final Silver"

assert final_silver_df["quality_status"].eq("PASS").all(), (
    "❌ Non-PASS row found in Final Silver"
)

assert final_silver_df["title"].notna().all(), (
    "❌ Final Silver contains missing title"
)

assert final_silver_df["tender_id"].notna().all(), (
    "❌ Final Silver contains missing tender_id"
)

assert final_silver_df["tender_id"].is_unique, (
    "❌ Final Silver tender_id is not unique"
)

assert final_silver_df["tender_code"].notna().all(), (
    "❌ Final Silver contains missing tender_code"
)

assert final_silver_df["tender_code"].astype(str).str.strip().ne("").all(), (
    "❌ Final Silver contains blank tender_code"
)

assert final_silver_df["tender_code"].is_unique, (
    "❌ Final Silver tender_code is not unique"
)

assert final_silver_df["record_fingerprint"].is_unique, (
    "❌ Exact duplicate remains in Final Silver"
)

# Original monetary lineage must still be preserved for every monetary record.
original_money_cols = [
    "document_fee_original",
    "tender_value_min_original",
    "tender_value_max_original",
    "temporary_bond_original",
]

has_original_money = final_silver_df[original_money_cols].notna().any(axis=1)

assert final_silver_df.loc[has_original_money, "currency_original"].notna().all(), (
    "❌ Original currency missing from monetary record"
)

assert final_silver_df.loc[has_original_money, "fx_rate_to_sar"].notna().all(), (
    "❌ FX rate missing from monetary record"
)


# ============================================================
# 11. REPORTS + RECONCILIATION
# ============================================================

reconciliation_report = pd.DataFrame({
    "metric": [
        "cleaned_input_rows",
        "silver_accepted_rows",
        "duplicates_removed",
        "superseded_historical_snapshots",
        "quarantined_or_rejected_rows",
        "reconciled_total",
        "reconciliation_pass",
    ],
    "value": [
        input_rows,
        len(final_silver_df),
        len(duplicate_rows),
        len(superseded_rows),
        len(rejected_rows),
        (
            len(final_silver_df)
            + len(duplicate_rows)
            + len(superseded_rows)
            + len(rejected_rows)
        ),
        (
            input_rows
            == len(final_silver_df)
            + len(duplicate_rows)
            + len(superseded_rows)
            + len(rejected_rows)
        ),
    ],
})

final_quality_report = pd.DataFrame({
    "metric": [
        "Final Silver rows",
        "Unique tender_id",
        "Exact duplicates remaining",
        "Duplicates removed",
        "Superseded historical snapshots",
        "Rejected rows",
        "Rows with source warnings",
        "Possible cross-source duplicate rows",
        "Reused source tender ID rows",
        "Technology rows",
        "Review rows",
        "Non-Tech rows",
        "Non-PASS rows",
        "Missing title",
        "Missing tender_id",
        "Missing published date",
        "Missing closing date",
        "Missing authority",
        "Missing source URL",
    ],
    "value": [
        len(final_silver_df),
        final_silver_df["tender_id"].nunique(),
        int(final_silver_df["record_fingerprint"].duplicated().sum()),
        len(duplicate_rows),
        len(superseded_rows),
        len(rejected_rows),
        int(final_silver_df["has_quality_warning"].sum()),
        int(final_silver_df["possible_cross_source_duplicate"].sum()),
        int(final_silver_df["source_id_reused"].sum()),
        int(final_silver_df["classification"].eq("Technology").sum()),
        int(final_silver_df["classification"].eq("Review").sum()),
        int(final_silver_df["classification"].eq("Non-Tech").sum()),
        int((~final_silver_df["quality_status"].eq("PASS")).sum()),
        int(final_silver_df["title"].isna().sum()),
        int(final_silver_df["tender_id"].isna().sum()),
        int(final_silver_df["published_date"].isna().sum()),
        int(final_silver_df["closing_date"].isna().sum()),
        int(final_silver_df["authority"].isna().sum()),
        int(final_silver_df["source_url"].isna().sum()),
    ],
})


# ============================================================
# 12. FINAL OUTPUT
# ============================================================

print("=" * 78)
print("VERIFIED FINAL SILVER — ID + DEDUPE + RECONCILIATION + QUALITY GATE")
print("=" * 78)
print("Cleaned input rows:", input_rows)
print("Final Silver rows:", len(final_silver_df))
print("Unique tender_id:", final_silver_df["tender_id"].nunique())
print("Exact duplicates removed:", len(duplicate_rows))
print("Rejected/quarantined rows:", len(rejected_rows))
print(
    "Possible cross-source duplicate rows:",
    int(final_silver_df["possible_cross_source_duplicate"].sum()),
)
print("-" * 78)
print("✅ Stable collision-safe tender_id created")
print("✅ Human-friendly tender_code created for Gold/dashboard display")
print("✅ Reused source tender numbers do not collide")
print("✅ Exact same-source duplicates checked")
print("✅ Different versions/reissues preserved")
print("✅ Cross-source matches flagged only — never auto-deleted")
print("✅ Critical rows quarantined instead of silently deleted")
print("✅ Reconciliation passed")
print("✅ Final Silver retains Technology + Review + Non-Tech PASS rows")
print("✅ No Azure write performed")
print("=" * 78)

# Databricks display() can try to convert a mixed-type pandas "value" column
# (integers + boolean) through Arrow and raise PySparkTypeError.
# Use pandas text rendering here so reporting never affects the pipeline result.
print("\nFINAL QUALITY REPORT")
print(final_quality_report.to_string(index=False))

print("\nRECONCILIATION REPORT")
print(reconciliation_report.to_string(index=False))

if not possible_cross_source_duplicates.empty:
    print("\nPOSSIBLE CROSS-SOURCE DUPLICATES (FLAGGED ONLY)")
    print(
        possible_cross_source_duplicates[
            [
                "tender_id",
                "tender_code",
                "source",
                "source_tender_id",
                "title",
                "closing_date",
                "cross_source_duplicate_reason",
            ]
        ].to_string(index=False)
    )

# ============================================================
# SILVER STEP 6 — CSV BASELINE / INCREMENTAL UPSERT (no Delta)
#
# One logical Silver dataset:
#   Technology + Review + Non-Tech
#
# Production file:
#   tendersilver/tenders/silver_tenders.csv
#
# Local file:
#   ./local_silver_output/silver_tenders.csv
#
# Modes:
#   rebuild     -> overwrite the CSV from all Bronze history
#   incremental -> read the existing CSV, upsert ONLY the new batch by
#                  tender_id (newest scrape wins), write it back
#
# Gold owns Open/Closed status. Silver keeps source_status only.
# ============================================================

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd


# ============================================================
# 1. RUN / VERSION LINEAGE
# ============================================================

MODEL_VERSION = "technology-rules-2026-09-25-v2-all-classes"
SCHEMA_VERSION = "silver-csv-schema-v1-tender-code"

requested_silver_run_id = None

try:
    try:
        dbutils.widgets.get("silver_run_id")
    except Exception:
        dbutils.widgets.text("silver_run_id", "")

    widget_value = dbutils.widgets.get("silver_run_id").strip()
    if widget_value:
        requested_silver_run_id = widget_value
except Exception:
    pass

if not requested_silver_run_id:
    env_run_id = os.getenv("SILVER_RUN_ID", "").strip()
    if env_run_id:
        requested_silver_run_id = env_run_id

silver_run_id = (
    requested_silver_run_id
    or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%f+00-00")
)
silver_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ============================================================
# 2. PRE-WRITE QUALITY GATES
# ============================================================

if selection_mode == "rebuild_all_history":
    assert len(final_silver_df) > 0, (
        "❌ Baseline rebuild cannot create an empty Silver table"
    )

assert final_silver_df["quality_status"].eq("PASS").all(), (
    "❌ Non-PASS row found in Final Silver"
)

assert final_silver_df["tender_id"].notna().all(), (
    "❌ Missing tender_id"
)

assert final_silver_df["tender_id"].is_unique, (
    "❌ Duplicate tender_id found in current Silver batch"
)

assert final_silver_df["tender_code"].notna().all(), (
    "❌ Missing tender_code"
)

assert final_silver_df["tender_code"].astype(str).str.strip().ne("").all(), (
    "❌ Blank tender_code"
)

assert final_silver_df["tender_code"].is_unique, (
    "❌ Duplicate tender_code found in current Silver batch"
)

assert set(final_silver_df["classification"].dropna().unique()).issubset(
    ALLOWED_CLASSIFICATIONS
), "❌ Unexpected classification value before Silver write"

# Gold derives Open/Closed. Silver must not publish a derived status column.
assert "status" not in final_silver_df.columns, (
    "❌ Derived status belongs in Gold, not Silver"
)

# Source-provided status may still be preserved for lineage.
if "source_status" not in final_silver_df.columns:
    final_silver_df["source_status"] = None

recon_lookup = dict(
    zip(
        reconciliation_report["metric"].astype(str),
        reconciliation_report["value"],
    )
)

assert bool(recon_lookup.get("reconciliation_pass")) is True, (
    "❌ Silver reconciliation has not passed"
)

assert int(recon_lookup.get("silver_accepted_rows", -1)) == len(final_silver_df), (
    "❌ Reconciliation accepted-row count differs from Final Silver"
)


# ============================================================
# 3. ADD EXPLICIT SILVER LINEAGE
# ============================================================

silver_output_df = final_silver_df.copy()

for required_lineage_column in [
    "bronze_run_id",
    "bronze_run_date",
    "bronze_blob_name",
]:
    if required_lineage_column not in silver_output_df.columns:
        raise RuntimeError(
            "Missing row-level Bronze lineage column in Final Silver: "
            + required_lineage_column
        )

    if silver_output_df[required_lineage_column].isna().any():
        raise RuntimeError(
            "Null row-level Bronze lineage found in Final Silver: "
            + required_lineage_column
        )

silver_output_df["silver_run_id"] = silver_run_id
silver_output_df["silver_run_date"] = silver_run_date
silver_output_df["model_version"] = MODEL_VERSION
silver_output_df["schema_version"] = SCHEMA_VERSION


# ============================================================
# 5. AUDIT / STATE HELPERS
# ============================================================

def _json_safe(value):
    if value is None:
        return None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def _dataframe_to_csv_bytes(df):
    return df.to_csv(
        index=False,
        encoding="utf-8-sig",
    ).encode("utf-8-sig")


def _write_local_audit(filename, payload_bytes):
    audit_dir = (
        LOCAL_OUTPUT_ROOT
        / "audit"
        / f"run_date={silver_run_date}"
        / f"run_id={silver_run_id}"
    )
    audit_dir.mkdir(parents=True, exist_ok=True)
    (audit_dir / filename).write_bytes(payload_bytes)


def _write_audit_bytes(filename, payload_bytes):
    if SILVER_LOCAL_MODE:
        _write_local_audit(filename, payload_bytes)
        return

    silver_container_client = blob_service_client.get_container_client(
        SILVER_CONTAINER
    )
    audit_blob = (
        f"_audit/run_date={silver_run_date}/"
        f"run_id={silver_run_id}/{filename}"
    )
    silver_container_client.get_blob_client(audit_blob).upload_blob(
        payload_bytes,
        overwrite=False,
    )


def _save_processed_blob_state(processed_names):
    payload = {
        "processed_blobs": sorted(processed_names),
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "silver_run_id": silver_run_id,
        "silver_mode": selection_mode,
    }
    data = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    if SILVER_LOCAL_MODE:
        LOCAL_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_STATE_PATH.write_bytes(data)
        return

    silver_container_client = blob_service_client.get_container_client(
        SILVER_CONTAINER
    )
    silver_container_client.get_blob_client(
        SILVER_STATE_BLOB
    ).upload_blob(
        data,
        overwrite=True,
    )
    # ============================================================
# SILVER CUSTOM STEP — توحيد النطاق الجغرافي (محلية / عالمية)
# ============================================================

def classify_tender_scope(row):
    """
    توحيد النطاق الجغرافي بناءً على عمود نوع المناقصة أو النص الأصلي
    ليصبح إما 'محلية (Local)' أو 'عالمية (International / External)'
    """
    # تجميع الحقول التي قد تحتوي على مؤشرات النطاق
    text_to_check = " ".join([
        str(row.get("tender_type") or ""),
        str(row.get("title") or ""),
        str(row.get("source_record_json") or "")
    ]).casefold()
    
    # الكلمات المفتاحية للعالمية / الخارجية
    external_keywords = ["external", "عالمية", "international"]
    for kw in external_keywords:
        if kw in text_to_check:
            return "عالمية (International)"
            
    # الافتراضي أو ما يحتوي على Local / محليّة / عامة يعتبر محلية
    return "محلية (Local)"

# تطبيق التوحيد على DataFrame النهائي قبل التصدير
if "final_silver_df" in globals() and not final_silver_df.empty:
    final_silver_df["tender_scope"] = final_silver_df.apply(classify_tender_scope, axis=1)
    print("✅ تم تطبيق توحيد النطاق الجغرافي (tender_scope) بنجاح قبل التصدير.")

# ============================================================
# SILVER CUSTOM STEP — توحيد وتصنيف حالات المناقصات (source_status)
# ============================================================

def clean_source_status(value):
    """
    توحيد وتنظيف حالات المناقصات القادمة من المصادر
    """
    if value is None or pd.isna(value):
        return "unknown"
    
    val_str = str(value).strip().lower()
    
    # توحيد القيم المتشابهة
    if val_str in ["active", "open"]:
        return "active"
    elif val_str in ["closed"]:
        return "closed"
    elif val_str in ["amended"]:
        return "amended"
    elif val_str in ["pending"]:
        return "pending"
    elif val_str in ["(blanks)", "", "nan", "none"]:
        return "unknown"
    
    return val_str

# تطبيق التوحيد على الـ DataFrame النهائي قبل التصدير
if "final_silver_df" in globals() and not final_silver_df.empty:
    if "source_status" in final_silver_df.columns:
        # نحتفظ بالقيمة الأصلية في عمود خام للتأكد، ونحدث العمود الموحد
        final_silver_df["raw_source_status"] = final_silver_df["source_status"]
        final_silver_df["source_status"] = final_silver_df["source_status"].map(clean_source_status)
        print("✅ تم توحيد حالات المناقصات (source_status) بنجاح قبل التصدير.")

# ============================================================
# SILVER CUSTOM STEP — حذف الأعمدة غير الضرورية
# ============================================================

columns_to_drop = [
    "tender_number",
     "tender_type", 
    "critical_quality_flags",
    "published_date_raw",
    "closing_date_raw",
    "scraped_at_raw",
    "source_record_json"
]

if "final_silver_df" in globals() and not final_silver_df.empty:
    # التأكد من وجود الأعمدة قبل محاولة حذفها لتجنب الأخطاء
    existing_cols_to_drop = [col for col in columns_to_drop if col in final_silver_df.columns]
    
    if existing_cols_to_drop:
        final_silver_df = final_silver_df.drop(columns=existing_cols_to_drop)
        print(f"✅ تم حذف الأعمدة التالية بنجاح: {existing_cols_to_drop}")
    else:
        print("⚠️ لم يتم العثور على الأعمدة المحددة في الجدول النهائي.")

#--------------------------------------------------------------------R------------

# ============================================================
# ============================================================
# 6. UPSERT + WRITE (Delta table + CSV backup)
#
# Same rule as the old Delta MERGE, done in pandas:
#   match on tender_id; the row with the newest scraped_at wins
#   (ties -> newest bronze_run_date -> the new batch).
# New columns are allowed (schema evolution); older rows get NULL.
#
# Existing Silver is read from the Delta table (source of truth). If the
# Delta table is missing but the CSV backup exists, the backup is used and
# the Delta table is recreated from it.
# ============================================================
for col in ["tender_scope", "raw_source_status", "source_status"]:
    if col in final_silver_df.columns:
        silver_output_df[col] = final_silver_df[col].values

silver_output_df = silver_output_df.drop(
    columns=[c for c in columns_to_drop if c in silver_output_df.columns]
)
print("✅ Custom columns synced to silver_output_df")

import io

if SILVER_LOCAL_MODE:
    silver_csv_target = str(LOCAL_SILVER_CSV_PATH)
else:
    silver_csv_target = (
        f"{storage_account_name}/{SILVER_CONTAINER}/{SILVER_OUTPUT_BLOB}"
    )
silver_target = f"Delta: {SILVER_DELTA_URI} | CSV backup: {silver_csv_target}"


def _format_for_csv(df):
    """Dates -> YYYY-MM-DD, scraped_at -> ISO UTC, NaN -> None."""
    out = df.copy()

    for column in ["published_date", "closing_date"]:
        if column in out.columns:
            parsed = pd.to_datetime(out[column], errors="coerce")
            out[column] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), None)

    if "scraped_at" in out.columns:
        out["scraped_at"] = out["scraped_at"].map(parse_utc_timestamp).map(
            lambda v: v.isoformat() if pd.notna(v) else None
        )

    out = out.astype(object)
    return out.where(out.notna(), None)


def _read_silver_csv_bytes(payload):
    df = pd.read_csv(
        io.BytesIO(payload),
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
    )
    return df.replace({"": None})


def _read_existing_silver_csv():
    if SILVER_LOCAL_MODE:
        if not LOCAL_SILVER_CSV_PATH.exists():
            return None
        return _read_silver_csv_bytes(LOCAL_SILVER_CSV_PATH.read_bytes())

    blob = (
        blob_service_client
        .get_container_client(SILVER_CONTAINER)
        .get_blob_client(SILVER_OUTPUT_BLOB)
    )
    if not blob.exists():
        return None
    return _read_silver_csv_bytes(blob.download_blob().readall())


def _read_existing_silver():
    """Delta first (source of truth), CSV backup second."""
    if delta_exists(SILVER_DELTA_URI, SILVER_DELTA_OPTIONS):
        print("Reading existing Silver from Delta:", SILVER_DELTA_URI)
        return read_delta_text(SILVER_DELTA_URI, SILVER_DELTA_OPTIONS), "delta"
    backup = _read_existing_silver_csv()
    if backup is not None:
        print("⚠️ Silver Delta table not found — using the CSV backup:", silver_csv_target)
        return backup, "csv_backup"
    return None, None


def _write_silver_csv(df):
    payload = _dataframe_to_csv_bytes(df)

    if SILVER_LOCAL_MODE:
        LOCAL_SILVER_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_SILVER_CSV_PATH.write_bytes(payload)
        return _read_silver_csv_bytes(LOCAL_SILVER_CSV_PATH.read_bytes()), len(payload)

    from azure.storage.blob import ContentSettings

    container = blob_service_client.get_container_client(SILVER_CONTAINER)
    if not container.exists():
        container.create_container()

    blob = container.get_blob_client(SILVER_OUTPUT_BLOB)
    blob.upload_blob(
        payload,
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv; charset=utf-8"),
    )
    return _read_silver_csv_bytes(blob.download_blob().readall()), len(payload)


batch_df = frame_to_text(_format_for_csv(silver_output_df))
silver_before_rows = None
silver_after_rows = None
silver_unique_ids = None
silver_classification_counts = {}
upsert_stats = {}

if selection_mode == "rebuild_all_history":
    result_df = batch_df.copy()
    silver_operation = "REBUILD_OVERWRITE"
else:
    existing_df, existing_origin = _read_existing_silver()
    if existing_df is None:
        raise RuntimeError(
            "Incremental/explicit Silver upsert requires an existing Silver CSV "
            f"({silver_target}). Run with silver_mode=rebuild first."
        )

    silver_before_rows = int(len(existing_df))

    # Union of columns: existing order first, then any new batch columns.
    all_columns = list(existing_df.columns) + [
        c for c in batch_df.columns if c not in existing_df.columns
    ]
    existing_df = existing_df.reindex(columns=all_columns)
    batch_aligned = batch_df.reindex(columns=all_columns)

    existing_df["_is_new"] = 0
    batch_aligned["_is_new"] = 1

    combined = pd.concat([existing_df, batch_aligned], ignore_index=True)
    combined["_scraped_sort"] = pd.to_datetime(
        combined["scraped_at"].map(parse_utc_timestamp), errors="coerce", utc=True
    )
    combined["_run_date_sort"] = combined["bronze_run_date"].astype(str)

    combined = combined.sort_values(
        by=["tender_id", "_scraped_sort", "_run_date_sort", "_is_new"],
        ascending=[True, False, False, False],
        na_position="last",
        kind="stable",
    )

    winners = combined.drop_duplicates(subset="tender_id", keep="first")

    existing_ids = set(existing_df["tender_id"].dropna())
    batch_ids = set(batch_aligned["tender_id"].dropna())
    upsert_stats = {
        "inserted": int(len(batch_ids - existing_ids)),
        "matched": int(len(batch_ids & existing_ids)),
        "updated": int(
            winners.loc[
                winners["_is_new"].eq(1)
                & winners["tender_id"].isin(existing_ids)
            ].shape[0]
        ),
        "kept_existing_newer": int(
            winners.loc[
                winners["_is_new"].eq(0)
                & winners["tender_id"].isin(batch_ids)
            ].shape[0]
        ),
    }

    result_df = (
        winners
        .drop(columns=["_is_new", "_scraped_sort", "_run_date_sort"])
        .reset_index(drop=True)
    )
    silver_operation = "INCREMENTAL_UPSERT"


# ------------------------------------------------------------
# Pre-write checks on the FULL Silver dataset
# ------------------------------------------------------------

assert result_df["tender_id"].notna().all(), "❌ Missing tender_id in Silver"
assert result_df["tender_id"].is_unique, "❌ Duplicate tender_id in Silver"
assert "status" not in result_df.columns, (
    "❌ Gold-derived status leaked into Silver"
)
assert set(result_df["classification"].dropna().unique()).issubset(
    ALLOWED_CLASSIFICATIONS
), "❌ Unexpected classification in Silver"
assert result_df["quality_status"].eq("PASS").all(), (
    "❌ Non-PASS row found in Silver"
)

silver_delta_version = None

if SILVER_DRY_RUN:
    verified_df = result_df
    written_bytes = 0
else:
    # 1) Delta table (used by Gold / the pipeline)
    silver_delta_version = write_delta_overwrite(
        result_df,
        SILVER_DELTA_URI,
        SILVER_DELTA_OPTIONS,
        vacuum_hours=SILVER_DELTA_VACUUM_HOURS,
    )
    verified_df = read_delta_text(SILVER_DELTA_URI, SILVER_DELTA_OPTIONS)
    assert len(verified_df) == len(result_df), (
        f"❌ Silver Delta verification failed: wrote {len(result_df)} rows, "
        f"read back {len(verified_df)}"
    )
    assert verified_df["tender_id"].is_unique, (
        "❌ Silver Delta contains duplicate tender_id values after write"
    )
    print(f"  ✅ Delta written: {SILVER_DELTA_URI} (version {silver_delta_version}, "
          f"{len(verified_df)} rows) — verified")

    # 2) CSV backup (full copy of the same data)
    csv_check, written_bytes = _write_silver_csv(result_df)
    assert len(csv_check) == len(result_df), (
        f"❌ Silver CSV backup verification failed: wrote {len(result_df)} rows, "
        f"read back {len(csv_check)}"
    )
    print(f"  ✅ CSV backup written: {silver_csv_target} "
          f"({len(csv_check)} rows, {written_bytes:,} bytes) — verified")

silver_after_rows = int(len(verified_df))
silver_unique_ids = int(verified_df["tender_id"].nunique())
silver_classification_counts = {
    str(k): int(v)
    for k, v in verified_df["classification"].value_counts().items()
}


# ============================================================
# 7. WRITE IMMUTABLE AUDIT REPORTS
# ============================================================

report_objects = {
    "final_quality_report.csv": final_quality_report,
    "reconciliation_report.csv": reconciliation_report,
}

for variable_name, filename in [
    ("classification_summary", "classification_summary.csv"),
    ("four_v_report", "four_v_report.csv"),
    ("standardization_report", "standardization_report.csv"),
    ("source_standardization_report", "source_standardization_report.csv"),
    ("cleaning_report", "cleaning_report.csv"),
    ("source_cleaning_report", "source_cleaning_report.csv"),
]:
    value = globals().get(variable_name)
    if isinstance(value, pd.DataFrame):
        report_objects[filename] = value

if isinstance(globals().get("possible_cross_source_duplicates"), pd.DataFrame):
    report_objects[
        "possible_cross_source_duplicates.csv"
    ] = possible_cross_source_duplicates

if isinstance(globals().get("duplicate_rows"), pd.DataFrame):
    report_objects["duplicates_removed.csv"] = duplicate_rows

if isinstance(globals().get("superseded_rows"), pd.DataFrame):
    report_objects["superseded_snapshots.csv"] = superseded_rows

if isinstance(globals().get("rejected_rows"), pd.DataFrame):
    report_objects["rejected_rows.csv"] = rejected_rows

if not SILVER_DRY_RUN:
    for filename, dataframe in report_objects.items():
        _write_audit_bytes(
            filename,
            _dataframe_to_csv_bytes(dataframe),
        )


# ============================================================
# 8. STATE UPDATE — ONLY AFTER SUCCESSFUL SILVER CSV VERIFICATION
# ============================================================

selected_blob_names = set(
    bronze_run_manifest["blob_name"].astype(str).tolist()
)

if selection_mode == "rebuild_all_history":
    next_processed_blob_names = {
        blob.name
        for blob in json_blobs
        if (
            len(PurePosixPath(blob.name).parts) == 3
            and PurePosixPath(blob.name).parts[0] in TARGET_SOURCE_SET
            and blob.name.endswith(".json")
        )
    }
else:
    next_processed_blob_names = (
        set(processed_blob_names)
        | selected_blob_names
    )

if not SILVER_DRY_RUN and selection_mode != "explicit_run":
    _save_processed_blob_state(next_processed_blob_names)

# ============================================================
# 9. RUN SUMMARY
# ============================================================

batch_classification_counts = (
    silver_output_df["classification"]
    .value_counts()
    .to_dict()
)

run_summary = {
    "bronze_selection_mode": str(selection_mode),
    "requested_silver_mode": str(SILVER_MODE),
    "resolved_silver_mode": str(resolved_silver_mode),
    "bronze_run_id": str(selected_bronze_run_id),
    "bronze_run_date": str(selected_bronze_run_date),
    "bronze_run_count": int(len(selected_run_ids)),
    "bronze_run_ids": list(selected_run_ids),
    "bronze_date_count": int(len(selected_run_dates)),
    "bronze_blob_count": int(len(bronze_run_manifest)),
    "bronze_rows_in_batch": int(total_bronze_rows),
    "silver_run_id": silver_run_id,
    "silver_run_date": silver_run_date,
    "model_version": MODEL_VERSION,
    "schema_version": SCHEMA_VERSION,
    "batch_final_rows": int(len(silver_output_df)),
    "batch_unique_tender_ids": int(silver_output_df["tender_id"].nunique()),
    "batch_classification_counts": {
        str(k): int(v) for k, v in batch_classification_counts.items()
    },
    "duplicates_removed": int(len(globals().get("duplicate_rows", []))),
    "superseded_historical_snapshots": int(
        len(globals().get("superseded_rows", []))
    ),
    "rejected_rows": int(len(globals().get("rejected_rows", []))),
    "silver_operation": silver_operation,
    "silver_target": silver_target,
    "silver_delta_version": silver_delta_version,
    "silver_before_rows": silver_before_rows,
    "silver_after_rows": silver_after_rows,
    "silver_unique_tender_ids": silver_unique_ids,
    "silver_classification_counts": silver_classification_counts,
    "upsert_stats": upsert_stats,
    "silver_csv_bytes": int(written_bytes),
    "dry_run": bool(SILVER_DRY_RUN),
    "local_mode": bool(SILVER_LOCAL_MODE),
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "quality_gate": "PASS",
}

if not SILVER_DRY_RUN:
    _write_audit_bytes(
        "run_summary.json",
        json.dumps(
            run_summary,
            ensure_ascii=False,
            indent=2,
            default=_json_safe,
        ).encode("utf-8"),
    )


# ============================================================
# 10. OUTPUT
# ============================================================

print()
print("=" * 78)
print("SILVER PIPELINE COMPLETED (Delta + CSV backup, pure pandas)")
print("=" * 78)
print("Selection mode:", selection_mode)
print("Bronze blobs processed:", len(bronze_run_manifest))
print("Bronze rows processed:", total_bronze_rows)
print("Accepted batch rows:", len(silver_output_df))
print("Batch classifications:", batch_classification_counts)
print("Silver operation:", silver_operation)
if upsert_stats:
    print("Upsert stats:", upsert_stats)
print("Silver Delta:", SILVER_DELTA_URI, "| version:", silver_delta_version)
print("Silver CSV backup:", silver_csv_target)
if silver_before_rows is not None:
    print("Silver rows before run:", silver_before_rows)
print("Silver rows after run:", silver_after_rows)
print("Unique tender_id:", silver_unique_ids)
print("Silver classifications:", silver_classification_counts)
print("Quality gate: PASS")
print("-" * 78)
print("✅ Technology + Review + Non-Tech retained")
print("✅ tender_id is the upsert key (newest scrape wins)")
print("✅ Open/Closed status NOT derived in Silver")
print("✅ Bronze remains read-only")
print("✅ No Spark / Unity Catalog used (Delta written with delta-rs)")
if SILVER_DRY_RUN:
    print("✅ DRY RUN: no Silver Delta/CSV/state/audit write performed")
elif SILVER_LOCAL_MODE:
    print("✅ Local test: Azure Silver was NOT modified")
    print("✅ Local Delta/CSV/state/audit written under:", LOCAL_OUTPUT_ROOT)
else:
    print("✅ Silver Delta + CSV backup written and verified by read-back")
print("=" * 78)

if not SILVER_DRY_RUN:
    from deltalake import DeltaTable

    dt = DeltaTable(
        SILVER_DELTA_URI,
        storage_options=SILVER_DELTA_OPTIONS,
    )
    deleted = dt.vacuum(
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
    )
    print(f"🗑️ Deleted {len(deleted)} old files")
    print("Files now in table:", dt.files())
else:
    print("DRY RUN: skipped deletion of old Delta files")