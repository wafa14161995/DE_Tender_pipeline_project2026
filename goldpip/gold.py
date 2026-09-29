# ============================================================
# TENDER PIPELINE — GOLD LAYER (pure pandas, Delta + CSV backup)
#
# Silver already does ALL the data processing. Gold only:
#   1) reads Silver   tendersilver/delta/silver_tenders      (Delta)
#                     fallback: tendersilver/tenders/silver_tenders.csv
#   2) selects the dashboard columns and derives status
#   3) writes         tendergold/delta/gold_tenders           (Delta, for Power BI)
#                     tendergold/tenders/gold_tenders.csv     (CSV backup)
#      and verifies both by reading them back.
#
# Status rule (team decision):
#   closing_date >= today (Asia/Riyadh) -> Open
#   closing_date <  today               -> Closed
#   closing_date missing                -> Open
#
# NO Spark: Delta is read/written with the `deltalake` package (delta-rs).
# On Databricks serverless run first:  %pip install deltalake azure-storage-blob
#
# Settings (notebook widget with the lower-case name, or env var):
#   GOLD_TIMEZONE           default Asia/Riyadh
#   GOLD_AS_OF_DATE         YYYY-MM-DD, default today in GOLD_TIMEZONE
#   SILVER_CONTAINER        default tendersilver
#   SILVER_DELTA_PATH       default delta/silver_tenders
#   SILVER_CSV_BLOB         default tenders/silver_tenders.csv
#   GOLD_CONTAINER          default tendergold
#   GOLD_DELTA_PATH         default delta/gold_tenders
#   GOLD_CSV_BLOB           default tenders/gold_tenders.csv
#   GOLD_DELTA_VACUUM_HOURS default 168 (keep 7 days of old Delta versions)
#   GOLD_LOCAL_ROOT         local testing: read/write under this folder
# ============================================================

import io
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


def _setting(name, default=""):
    try:
        value = dbutils.widgets.get(name.lower()).strip()  # noqa: F821
        if value:
            return value
    except Exception:
        pass
    return os.getenv(name, default).strip()


GOLD_TIMEZONE = _setting("GOLD_TIMEZONE", "Asia/Riyadh")
GOLD_AS_OF_DATE = _setting("GOLD_AS_OF_DATE")
SILVER_CONTAINER = _setting("SILVER_CONTAINER", "tendersilver")
SILVER_DELTA_PATH = _setting("SILVER_DELTA_PATH", "delta/silver_tenders").strip("/")
SILVER_CSV_BLOB = _setting("SILVER_CSV_BLOB", "tenders/silver_tenders.csv")
GOLD_CONTAINER = _setting("GOLD_CONTAINER", "tendergold")
GOLD_DELTA_PATH = _setting("GOLD_DELTA_PATH", "delta/gold_tenders").strip("/")
GOLD_CSV_BLOB = _setting("GOLD_CSV_BLOB", "tenders/gold_tenders.csv")
GOLD_DELTA_VACUUM_HOURS = int(_setting("GOLD_DELTA_VACUUM_HOURS", "168"))
GOLD_LOCAL_ROOT = _setting("GOLD_LOCAL_ROOT")

LOCAL_MODE = bool(GOLD_LOCAL_ROOT)

# Output contract used by the Power BI report — keep names AND order stable.
GOLD_COLUMNS = [
    "tender_id",
    "tender_code",
    "title",
    "published_date",
    "closing_date",
    "country",
    "source_name",
    "source_url",
    "classification",
    "status",
]
GOLD_DATE_COLUMNS = {"published_date", "closing_date"}

REQUIRED_SILVER_COLUMNS = [
    "tender_id", "tender_code", "title", "published_date", "closing_date",
    "country", "source", "source_url", "classification",
]

# ============================================================
# STORAGE HELPERS
# ============================================================

def _connection_string():
    try:
        return dbutils.secrets.get(scope="azure-storage", key="connection-string")  # noqa: F821
    except Exception:
        pass
    value = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
    if value:
        return value
    raise RuntimeError(
        "Azure Storage connection string not found (secret scope 'azure-storage' "
        "/ key 'connection-string', or env AZURE_STORAGE_CONNECTION_STRING)."
    )


def _connection_parts():
    parts = {}
    for p in _connection_string().strip().split(";"):
        if "=" in p:
            k, v = p.split("=", 1)
            parts[k.strip().lower()] = v.strip()
    return parts


def _blob_service():
    from azure.storage.blob import BlobServiceClient

    clean = ";".join(
        f"{k.strip()}={v.strip()}"
        for k, v in (p.split("=", 1) for p in _connection_string().strip().split(";") if "=" in p)
    )
    return BlobServiceClient.from_connection_string(clean)


def _delta_location(container, path):
    """(uri, storage_options) for delta-rs."""
    if LOCAL_MODE:
        return str(Path(GOLD_LOCAL_ROOT).resolve() / container / path), None
    parts = _connection_parts()
    if not parts.get("accountname") or not parts.get("accountkey"):
        raise RuntimeError("Connection string must contain AccountName and AccountKey for Delta.")
    return (
        f"az://{container}/{path}",
        {"account_name": parts["accountname"], "account_key": parts["accountkey"]},
    )


def _require_deltalake():
    try:
        import deltalake  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Package 'deltalake' is missing. On Databricks run "
            "`%pip install deltalake` first (or add it to the job environment)."
        ) from exc


def _delta_exists(uri, opts):
    from deltalake import DeltaTable
    try:
        return bool(DeltaTable.is_deltatable(uri, storage_options=opts))
    except AttributeError:
        try:
            DeltaTable(uri, storage_options=opts)
            return True
        except Exception:
            return False


def _is_null(v):
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except Exception:
        return False


def _to_text(df):
    """All values -> text (dates as YYYY-MM-DD), missing -> None."""
    import datetime as _dt

    def conv(v):
        if _is_null(v):
            return None
        if isinstance(v, (pd.Timestamp, _dt.datetime)):
            ts = pd.Timestamp(v)
            return ts.tz_convert("UTC").isoformat() if ts.tzinfo else ts.isoformat()
        if isinstance(v, _dt.date):
            return v.isoformat()
        return str(v)

    return pd.DataFrame(
        {c: [conv(v) for v in df[c].tolist()] for c in df.columns}, index=df.index
    ).astype(object)


def _read_csv(payload):
    df = pd.read_csv(io.BytesIO(payload), dtype=str, encoding="utf-8-sig", keep_default_na=False)
    return df.replace({"": None})


def _to_csv_bytes(df):
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

# ============================================================
# READ SILVER
# ============================================================

def load_silver():
    _require_deltalake()
    from deltalake import DeltaTable

    uri, opts = _delta_location(SILVER_CONTAINER, SILVER_DELTA_PATH)
    if _delta_exists(uri, opts):
        dt = DeltaTable(uri, storage_options=opts)
        print(f"Reading Silver Delta: {uri} (version {dt.version()})")
        return _to_text(dt.to_pandas())

    print(f"⚠️ Silver Delta not found at {uri} — falling back to the CSV backup.")
    if LOCAL_MODE:
        path = Path(GOLD_LOCAL_ROOT).resolve() / SILVER_CONTAINER / SILVER_CSV_BLOB
        if not path.exists():
            raise RuntimeError(f"No Silver Delta and no CSV backup ({path}). Run Silver first.")
        return _read_csv(path.read_bytes())

    blob = _blob_service().get_container_client(SILVER_CONTAINER).get_blob_client(SILVER_CSV_BLOB)
    if not blob.exists():
        raise RuntimeError(
            f"No Silver Delta and no CSV backup ({SILVER_CONTAINER}/{SILVER_CSV_BLOB}). "
            "Run silver/silverreem.py first."
        )
    return _read_csv(blob.download_blob().readall())

# ============================================================
# TRANSFORM (business logic only)
# ============================================================

def build_gold(silver_df, as_of):
    missing = [c for c in REQUIRED_SILVER_COLUMNS if c not in silver_df.columns]
    if missing:
        raise RuntimeError(
            f"Silver is missing columns {missing}. Is it the output of the current silverreem.py?"
        )
    if not silver_df["tender_id"].is_unique:
        raise RuntimeError("❌ Silver contains duplicate tender_id values.")

    gold = silver_df.rename(columns={"source": "source_name"}).copy()

    closing = pd.to_datetime(
        gold["closing_date"].map(lambda v: None if _is_null(v) else str(v)[:10]),
        format="%Y-%m-%d", errors="coerce",
    ).dt.date
    gold["status"] = ["Open" if _is_null(d) or d >= as_of else "Closed" for d in closing]

    for c in GOLD_DATE_COLUMNS:
        gold[c] = gold[c].map(lambda v: None if _is_null(v) else str(v)[:10])

    gold = gold[GOLD_COLUMNS]
    return gold.astype(object).where(gold.notna(), None)

# ============================================================
# WRITE GOLD
# ============================================================

def _gold_arrow(gold_df):
    import pyarrow as pa

    arrays, fields = [], []
    for c in GOLD_COLUMNS:
        vals = gold_df[c].tolist()
        if c in GOLD_DATE_COLUMNS:
            typ = pa.date32()
            data = [None if _is_null(v) else datetime.strptime(str(v)[:10], "%Y-%m-%d").date() for v in vals]
        else:
            typ = pa.string()
            data = [None if _is_null(v) else str(v) for v in vals]
        arrays.append(pa.array(data, type=typ))
        fields.append(pa.field(c, typ, nullable=True))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def write_gold_delta(gold_df):
    from deltalake import DeltaTable, write_deltalake

    uri, opts = _delta_location(GOLD_CONTAINER, GOLD_DELTA_PATH)
    table = _gold_arrow(gold_df)
    try:
        write_deltalake(uri, table, mode="overwrite", schema_mode="overwrite", storage_options=opts)
    except TypeError:  # older deltalake versions
        write_deltalake(uri, table, mode="overwrite", overwrite_schema=True, storage_options=opts)

    dt = DeltaTable(uri, storage_options=opts)
    if GOLD_DELTA_VACUUM_HOURS > 0:
        try:
            dt.vacuum(retention_hours=GOLD_DELTA_VACUUM_HOURS, dry_run=False,
                      enforce_retention_duration=False)
        except Exception as exc:
            print(f"  ⚠️ Delta VACUUM skipped: {exc}")

    check = DeltaTable(uri, storage_options=opts).to_pandas()
    if len(check) != len(gold_df):
        raise RuntimeError(f"❌ Gold Delta verification failed: wrote {len(gold_df)}, read back {len(check)}")
    print(f"  ✅ Delta: {uri} (version {dt.version()}, {len(check)} rows) — verified")


def write_gold_csv(gold_df):
    payload = _to_csv_bytes(gold_df)

    if LOCAL_MODE:
        path = Path(GOLD_LOCAL_ROOT).resolve() / GOLD_CONTAINER / GOLD_CSV_BLOB
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        check = _read_csv(path.read_bytes())
        target = str(path)
    else:
        from azure.storage.blob import ContentSettings

        service = _blob_service()
        container = service.get_container_client(GOLD_CONTAINER)
        if not container.exists():
            container.create_container()
        blob = container.get_blob_client(GOLD_CSV_BLOB)
        blob.upload_blob(
            payload, overwrite=True,
            content_settings=ContentSettings(content_type="text/csv; charset=utf-8"),
        )
        check = _read_csv(blob.download_blob().readall())
        target = f"{service.account_name}/{GOLD_CONTAINER}/{GOLD_CSV_BLOB}"

    if len(check) != len(gold_df):
        raise RuntimeError(f"❌ Gold CSV verification failed: wrote {len(gold_df)}, read back {len(check)}")
    print(f"  ✅ CSV backup: {target} ({len(check)} rows, {len(payload):,} bytes) — verified")

# ============================================================
# MAIN
# ============================================================

def run_gold():
    print("=" * 78)
    print("STARTING GOLD LAYER PROCESSING (pure pandas)")
    print("=" * 78)
    _require_deltalake()

    now_local = datetime.now(ZoneInfo(GOLD_TIMEZONE))
    as_of = (
        datetime.strptime(GOLD_AS_OF_DATE, "%Y-%m-%d").date()
        if GOLD_AS_OF_DATE else now_local.date()
    )
    print(f"Run started: {now_local:%Y-%m-%d %H:%M:%S} ({GOLD_TIMEZONE})")

    silver_df = load_silver()
    if silver_df.empty:
        raise RuntimeError("Silver is empty.")

    gold_df = build_gold(silver_df, as_of)
    no_close = gold_df["closing_date"].isna()

    print("-" * 78)
    print("GOLD PIPELINE SUMMARY:")
    print(f"  - As-of date: {as_of.isoformat()}")
    print(f"  - Gold rows: {len(gold_df)}")
    print(f"  - Rows per source: {gold_df['source_name'].value_counts().to_dict()}")
    print(f"  - Statuses: {gold_df['status'].value_counts().to_dict()}")
    print(f"  - Classifications: {gold_df['classification'].value_counts().to_dict()}")
    if no_close.any():
        print(f"  - Open because closing_date is missing: "
              f"{gold_df.loc[no_close, 'source_name'].value_counts().to_dict()}")
    print("-" * 78)

    print("Writing Gold:")
    write_gold_delta(gold_df)
    write_gold_csv(gold_df)
    print("=" * 78)
    return gold_df


if __name__ == "__main__":
    run_gold()