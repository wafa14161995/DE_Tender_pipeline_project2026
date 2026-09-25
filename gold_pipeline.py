# ============================================================
# TENDER PIPELINE — GOLD LAYER
#
# Current contract:
#   Silver Delta (all classifications)
#   -> Gold curated table
#   -> derive status from closing_date
#
# Gold output columns:
#   tender_id        : technical key (kept for lineage / joins)
#   tender_code      : human-friendly ID for dashboard display
#   title
#   published_date
#   closing_date
#   country
#   source_name
#   source_url
#   classification
#   status           : Open / Closed / Unknown (DERIVED HERE)
#
# IMPORTANT:
# - Gold DOES NOT filter out Closed / Non-Tech / Review rows.
# - status is derived here, not in Silver.
# - source_status from Silver is intentionally not used as Gold status.
# - Production reads the ONE logical Silver Delta table:
#       tendersilver/tenders
# - Production writes ONE logical Gold Delta table:
#       tendergold/tenders
# - Local mode reads the Silver-generated CSV preview and writes only locally.
# ============================================================

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path


# ============================================================
# RUNTIME CONFIG
# ============================================================


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


GOLD_LOCAL_MODE = _env_flag("GOLD_LOCAL_MODE", False)
GOLD_DRY_RUN = _env_flag("GOLD_DRY_RUN", False)

PROJECT_ROOT = Path(__file__).resolve().parent

LOCAL_GOLD_INPUT = Path(
    os.getenv(
        "GOLD_LOCAL_INPUT",
        str(PROJECT_ROOT / "local_silver_preview" / "gold_input_preview.csv"),
    )
).resolve()

LOCAL_GOLD_OUTPUT_DIR = Path(
    os.getenv(
        "GOLD_LOCAL_OUTPUT",
        str(PROJECT_ROOT / "local_gold_preview"),
    )
).resolve()

SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "tendersilver").strip()
SILVER_DELTA_DIR = os.getenv("SILVER_DELTA_DIR", "tenders").strip("/")

GOLD_CONTAINER = os.getenv("GOLD_CONTAINER", "tendergold").strip()
GOLD_DELTA_DIR = os.getenv("GOLD_DELTA_DIR", "tenders").strip("/")
GOLD_TABLE_NAME = os.getenv("GOLD_TABLE_NAME", "gold_tenders").strip()

# Optional deterministic date for testing, e.g. 2026-09-25.
# If omitted, Gold uses the current execution date.
GOLD_AS_OF_DATE = os.getenv("GOLD_AS_OF_DATE", "").strip()


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

GOLD_INPUT_COLUMNS = [
    "tender_id",
    "tender_code",
    "title",
    "published_date",
    "closing_date",
    "country",
    "source_url",
    "classification",
]

ALLOWED_CLASSIFICATIONS = {"Technology", "Review", "Non-Tech"}
ALLOWED_STATUSES = {"Open", "Closed", "Unknown"}


# ============================================================
# SHARED HELPERS
# ============================================================


def _resolve_as_of_date() -> date:
    """Return explicit GOLD_AS_OF_DATE or today's local execution date."""
    if not GOLD_AS_OF_DATE:
        return date.today()

    try:
        return datetime.strptime(GOLD_AS_OF_DATE, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            "GOLD_AS_OF_DATE must use YYYY-MM-DD, for example 2026-09-25"
        ) from exc



def _validate_common_columns(columns) -> None:
    columns = set(columns)

    missing = [column for column in GOLD_INPUT_COLUMNS if column not in columns]
    if missing:
        raise RuntimeError(
            "Gold input is missing required column(s): " + ", ".join(missing)
        )

    if "source_name" not in columns and "source" not in columns:
        raise RuntimeError(
            "Gold input must contain source_name or Silver's source column"
        )


# ============================================================
# LOCAL MODE — TEST DIRECTLY AFTER SILVER PREVIEW
# ============================================================


def _run_local() -> None:
    import pandas as pd

    if not LOCAL_GOLD_INPUT.exists():
        raise FileNotFoundError(
            "Silver Gold-input preview was not found:\n"
            f"  {LOCAL_GOLD_INPUT}\n\n"
            "Run this first from the project root:\n"
            "  python silver\\run_real_bronze_preview.py"
        )

    silver_df = pd.read_csv(LOCAL_GOLD_INPUT, encoding="utf-8-sig")

    if silver_df.empty:
        raise RuntimeError("Gold input preview is empty")

    _validate_common_columns(silver_df.columns)

    if "source_name" not in silver_df.columns:
        silver_df = silver_df.rename(columns={"source": "source_name"})

    if silver_df["tender_id"].isna().any():
        raise RuntimeError("Gold input contains missing tender_id")
    if not silver_df["tender_id"].is_unique:
        raise RuntimeError("Gold input contains duplicate tender_id values")

    if silver_df["tender_code"].isna().any():
        raise RuntimeError("Gold input contains missing tender_code")
    if silver_df["tender_code"].astype(str).str.strip().eq("").any():
        raise RuntimeError("Gold input contains blank tender_code")
    if not silver_df["tender_code"].is_unique:
        raise RuntimeError("Gold input contains duplicate tender_code values")

    unexpected_classes = (
        set(silver_df["classification"].dropna().astype(str).unique())
        - ALLOWED_CLASSIFICATIONS
    )
    if unexpected_classes:
        raise RuntimeError(
            "Unexpected classification value(s): "
            + ", ".join(sorted(unexpected_classes))
        )

    # Silver preview writes ISO-like dates. Invalid/unavailable values become NaT.
    silver_df["published_date"] = pd.to_datetime(
        silver_df["published_date"], errors="coerce"
    )
    silver_df["closing_date"] = pd.to_datetime(
        silver_df["closing_date"], errors="coerce"
    )

    as_of_date = _resolve_as_of_date()
    close_date = silver_df["closing_date"].dt.date

    # Agreed Gold rule:
    #   missing closing_date -> Unknown
    #   closing_date >= execution date -> Open
    #   closing_date <  execution date -> Closed
    silver_df["status"] = "Unknown"
    silver_df.loc[
        silver_df["closing_date"].notna() & (close_date >= as_of_date),
        "status",
    ] = "Open"
    silver_df.loc[
        silver_df["closing_date"].notna() & (close_date < as_of_date),
        "status",
    ] = "Closed"

    gold_df = silver_df[GOLD_COLUMNS].copy()

    if not gold_df["tender_id"].is_unique:
        raise RuntimeError("Gold output contains duplicate tender_id values")

    unexpected_statuses = set(gold_df["status"].unique()) - ALLOWED_STATUSES
    if unexpected_statuses:
        raise RuntimeError(
            "Unexpected Gold status value(s): "
            + ", ".join(sorted(unexpected_statuses))
        )

    # Export dates as YYYY-MM-DD, keeping missing values blank.
    for column in ("published_date", "closing_date"):
        gold_df[column] = gold_df[column].dt.strftime("%Y-%m-%d")

    status_counts = gold_df["status"].value_counts(dropna=False).to_dict()
    classification_counts = (
        gold_df["classification"].value_counts(dropna=False).to_dict()
    )

    print("=" * 78)
    print("GOLD LOCAL PREVIEW")
    print("=" * 78)
    print("Silver input:", LOCAL_GOLD_INPUT)
    print("As-of date:", as_of_date.isoformat())
    print("Rows in:", len(silver_df))
    print("Rows out:", len(gold_df))
    print("Statuses:", status_counts)
    print("Classifications:", classification_counts)
    print("Unique tender_id:", gold_df["tender_id"].nunique())
    print("Unique tender_code:", gold_df["tender_code"].nunique())
    print("=" * 78)

    # Local mode is always safe: no Azure Gold write.
    LOCAL_GOLD_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = LOCAL_GOLD_OUTPUT_DIR / "gold_final_preview.csv"
    summary_path = LOCAL_GOLD_OUTPUT_DIR / "gold_preview_summary.txt"

    gold_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    summary_lines = [
        "GOLD LOCAL PREVIEW",
        "=" * 60,
        f"As-of date: {as_of_date.isoformat()}",
        f"Input rows: {len(silver_df)}",
        f"Output rows: {len(gold_df)}",
        f"Unique tender_id: {gold_df['tender_id'].nunique()}",
        f"Unique tender_code: {gold_df['tender_code'].nunique()}",
        f"Status counts: {status_counts}",
        f"Classification counts: {classification_counts}",
        "",
        "Gold columns:",
        *GOLD_COLUMNS,
        "",
        "Status rule:",
        "closing_date is blank -> Unknown",
        "closing_date >= as-of date -> Open",
        "closing_date <  as-of date -> Closed",
        "",
        "Azure Gold writes: NONE (local mode)",
    ]
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")

    print("✅ Gold preview:", output_path)
    print("✅ Summary:", summary_path)
    print("✅ Nothing was written to Azure Gold")


# ============================================================
# PRODUCTION / DATABRICKS MODE
# ============================================================


def _get_connection_string() -> str:
    # Databricks secret convention used by the project.
    try:
        return dbutils.secrets.get(  # type: ignore[name-defined]
            scope="azure-storage",
            key="connection-string",
        )
    except Exception:
        pass

    value = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
    if value:
        return value

    raise RuntimeError(
        "Azure Storage connection string not found. "
        "In Databricks use secret scope azure-storage / connection-string."
    )



def _clean_connection_string(raw: str) -> str:
    clean_parts = []
    for part in raw.strip().split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            clean_parts.append(f"{key.strip()}={value.strip()}")
    return ";".join(clean_parts)



def _extract_account_key(connection_string: str) -> str | None:
    for part in connection_string.split(";"):
        if part.strip().lower().startswith("accountkey="):
            return part.split("=", 1)[1].strip()
    return None



def _run_production() -> None:
    from azure.storage.blob import BlobServiceClient
    from pyspark.sql import functions as F

    try:
        spark  # type: ignore[name-defined]
    except NameError as exc:
        raise RuntimeError(
            "Production Gold requires Databricks/Spark. "
            "For a local test set GOLD_LOCAL_MODE=1."
        ) from exc

    raw_connection_string = _get_connection_string()
    clean_connection_string = _clean_connection_string(raw_connection_string)

    blob_service_client = BlobServiceClient.from_connection_string(
        clean_connection_string
    )
    storage_account_name = blob_service_client.account_name

    # Fail early with a clear message if required containers do not exist.
    silver_container_client = blob_service_client.get_container_client(
        SILVER_CONTAINER
    )
    silver_container_client.get_container_properties()

    gold_container_client = blob_service_client.get_container_client(
        GOLD_CONTAINER
    )
    gold_container_client.get_container_properties()

    account_key = _extract_account_key(clean_connection_string)
    if not account_key:
        raise RuntimeError(
            f"Connection string for {storage_account_name} is missing AccountKey "
            "required for Spark wasbs access."
        )

    spark.conf.set(  # type: ignore[name-defined]
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
        account_key,
    )

    silver_path = (
        f"wasbs://{SILVER_CONTAINER}@{storage_account_name}"
        f".blob.core.windows.net/{SILVER_DELTA_DIR}"
    )
    gold_path = (
        f"wasbs://{GOLD_CONTAINER}@{storage_account_name}"
        f".blob.core.windows.net/{GOLD_DELTA_DIR}"
    )

    print("=" * 78)
    print("GOLD PRODUCTION")
    print("=" * 78)
    print("Silver Delta:", silver_path)
    print("Gold Delta:", gold_path)
    print("Dry run:", GOLD_DRY_RUN)
    print("=" * 78)

    # NEW architecture: read the ONE logical Silver Delta table.
    silver_df = spark.read.format("delta").load(silver_path)  # type: ignore[name-defined]

    _validate_common_columns(silver_df.columns)

    if "source_name" not in silver_df.columns:
        silver_df = silver_df.withColumn("source_name", F.col("source"))

    # Quality gates inherited from the Silver contract.
    if silver_df.filter(F.col("tender_id").isNull()).limit(1).count():
        raise RuntimeError("Silver Delta contains missing tender_id")

    total_rows = silver_df.count()
    unique_tender_ids = silver_df.select("tender_id").distinct().count()
    if total_rows != unique_tender_ids:
        raise RuntimeError("Silver Delta contains duplicate tender_id values")

    missing_tender_code = (
        silver_df
        .filter(
            F.col("tender_code").isNull()
            | (F.trim(F.col("tender_code")) == "")
        )
        .limit(1)
        .count()
    )
    if missing_tender_code:
        raise RuntimeError("Silver Delta contains missing/blank tender_code")

    unique_tender_codes = silver_df.select("tender_code").distinct().count()
    if total_rows != unique_tender_codes:
        raise RuntimeError("Silver Delta contains duplicate tender_code values")

    unexpected_classes = [
        row["classification"]
        for row in (
            silver_df
            .select("classification")
            .where(F.col("classification").isNotNull())
            .distinct()
            .collect()
        )
        if row["classification"] not in ALLOWED_CLASSIFICATIONS
    ]
    if unexpected_classes:
        raise RuntimeError(
            "Unexpected classification value(s): "
            + ", ".join(sorted(map(str, unexpected_classes)))
        )

    # Silver stores these as DateType, but to_date is harmless and makes Gold
    # resilient if an upstream representation changes later.
    silver_df = (
        silver_df
        .withColumn("published_date", F.to_date(F.col("published_date")))
        .withColumn("closing_date", F.to_date(F.col("closing_date")))
    )

    if GOLD_AS_OF_DATE:
        as_of_date = _resolve_as_of_date()
        as_of_expr = F.to_date(F.lit(as_of_date.isoformat()))
        as_of_label = as_of_date.isoformat()
    else:
        as_of_expr = F.current_date()
        as_of_label = "current_date()"

    gold_df = (
        silver_df
        .withColumn(
            "status",
            F.when(F.col("closing_date").isNull(), F.lit("Unknown"))
            .when(F.col("closing_date") >= as_of_expr, F.lit("Open"))
            .otherwise(F.lit("Closed")),
        )
        .select(*GOLD_COLUMNS)
    )

    gold_rows = gold_df.count()
    if gold_rows != total_rows:
        raise RuntimeError(
            "Gold row-count mismatch: Gold must retain every accepted Silver row"
        )

    gold_unique_ids = gold_df.select("tender_id").distinct().count()
    if gold_rows != gold_unique_ids:
        raise RuntimeError("Gold output contains duplicate tender_id values")

    status_rows = gold_df.groupBy("status").count().collect()
    status_counts = {row["status"]: int(row["count"]) for row in status_rows}
    unexpected_statuses = set(status_counts) - ALLOWED_STATUSES
    if unexpected_statuses:
        raise RuntimeError(
            "Unexpected Gold status value(s): "
            + ", ".join(sorted(unexpected_statuses))
        )

    class_rows = gold_df.groupBy("classification").count().collect()
    classification_counts = {
        row["classification"]: int(row["count"]) for row in class_rows
    }

    print("Silver rows:", total_rows)
    print("Gold rows:", gold_rows)
    print("As-of date:", as_of_label)
    print("Statuses:", status_counts)
    print("Classifications:", classification_counts)

    if GOLD_DRY_RUN:
        print("✅ GOLD DRY RUN — no Azure Gold write performed")
        gold_df.show(20, truncate=False)
        return

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )

    # Verify the persisted Delta table, not just the in-memory DataFrame.
    verified_gold_df = spark.read.format("delta").load(gold_path)  # type: ignore[name-defined]
    verified_rows = verified_gold_df.count()
    verified_unique_ids = verified_gold_df.select("tender_id").distinct().count()

    if verified_rows != gold_rows:
        raise RuntimeError(
            f"Gold Delta verification failed: wrote {gold_rows}, read back {verified_rows}"
        )
    if verified_rows != verified_unique_ids:
        raise RuntimeError("Gold Delta verification found duplicate tender_id values")

    spark.sql(  # type: ignore[name-defined]
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE_NAME}
        USING DELTA
        LOCATION '{gold_path}'
        """
    )
    spark.sql(f"REFRESH TABLE {GOLD_TABLE_NAME}")  # type: ignore[name-defined]

    print("✅ Gold Delta written and verified:", gold_path)
    print("✅ SQL table:", GOLD_TABLE_NAME)
    print("✅ Rows:", verified_rows)


# ============================================================
# ENTRY POINT
# ============================================================


def main() -> None:
    if GOLD_LOCAL_MODE:
        _run_local()
    else:
        _run_production()


if __name__ == "__main__":
    main()
