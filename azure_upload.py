import hashlib
import json
import os

from datetime import datetime, timezone
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContentSettings


try:
    PROJECT_ROOT = Path(__file__).resolve().parent
except NameError:
    # __file__ غير موجود لما يشتغل الكود كنوتبوك Databricks
    PROJECT_ROOT = Path(os.getcwd())

RAW_DIR = PROJECT_ROOT / "results" / "raw"

AZURE_STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING"
)
BLOB_CONTAINER_NAME = os.environ.get(
    "BLOB_CONTAINER_NAME", "tenderbronze"
)


# auto   -> restore a source's JSON from Bronze only if results/raw/<source>.json
#           is missing (fresh cloud container). Local runs are untouched.
# always -> always overwrite local files with the latest Bronze copy
# never  -> never restore
RESTORE_RAW_FROM_BRONZE = os.environ.get(
    "RESTORE_RAW_FROM_BRONZE", "auto"
).strip().lower()

# Skip uploading a source whose content is identical to its latest Bronze
# blob (e.g. the extractor failed and the restored file was left unchanged).
SKIP_UNCHANGED_UPLOADS = os.environ.get(
    "SKIP_UNCHANGED_UPLOADS", "true"
).strip().lower() in {"1", "true", "yes", "y", "on"}


def _content_hash(records):
    """Hash of the exact text we upload, so equal data -> equal hash."""
    text = json.dumps(records, ensure_ascii=False, indent=2)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _latest_bronze_blobs(container_client):
    """
    {source: newest blob name}. Blob names are
    <source>/<YYYY-MM-DD>/<ISO timestamp>.json, which sort chronologically.
    """
    latest = {}
    for blob in container_client.list_blobs():
        parts = blob.name.split("/")
        if len(parts) != 3 or not parts[2].endswith(".json"):
            continue
        source = parts[0]
        if source not in latest or blob.name > latest[source]:
            latest[source] = blob.name
    return latest


def restore_raw_from_bronze():
    """
    The Container Apps job starts with an EMPTY results/raw folder on every
    run, so extractors that read their previous JSON (e.g. kw_source_01, om_source_01) would
    behave differently than on a laptop. This downloads the newest Bronze
    copy of each source first, so the job and local runs behave the same.
    Bronze itself is only read, never changed.
    """
    if not is_configured() or RESTORE_RAW_FROM_BRONZE == "never":
        return {"restored": [], "kept_local": []}

    container_client = get_container_client()
    latest = _latest_bronze_blobs(container_client)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    restored, kept_local = [], []
    print("\n" + "=" * 60)
    print(f"RESTORE results/raw FROM BRONZE (mode={RESTORE_RAW_FROM_BRONZE})")
    print("=" * 60)

    for source, blob_name in sorted(latest.items()):
        target = RAW_DIR / f"{source}.json"
        if target.exists() and RESTORE_RAW_FROM_BRONZE != "always":
            kept_local.append(source)
            continue
        try:
            payload = container_client.get_blob_client(blob_name).download_blob().readall()
            json.loads(payload)  # validate before writing
            target.write_bytes(payload)
            restored.append(source)
            print(f"[azure] {source}: restored from {blob_name}")
        except Exception as exc:
            print(f"[azure] {source}: restore FAILED ({exc}) -- extractor starts empty")

    print(f"Restored: {len(restored)} | Kept local file: {len(kept_local)}")
    print("=" * 60)
    return {"restored": restored, "kept_local": kept_local}


def is_configured():
    """
    True if Azure upload should run at all. Lets local/dev runs work
    fine with no Azure credentials set -- upload is simply skipped.
    """
    return bool(AZURE_STORAGE_CONNECTION_STRING)


def get_container_client():

    service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    container_client = service_client.get_container_client(
        BLOB_CONTAINER_NAME
    )

    if not container_client.exists():
        container_client.create_container()

    return container_client



# ============================================================
# ACTIVE -- uploads results/raw/*.json, unfiltered.
# ============================================================
def upload_raw_results():
    """
    Uploads every results/raw/<source>.json file to the Azure Blob
    container, one blob per source per run. NO FILTERING -- everything
    each extractor scraped goes up as-is.
    """

    if not is_configured():
        print(
            "\n[azure] AZURE_STORAGE_CONNECTION_STRING not set -- "
            "skipping Blob upload. Expected for local runs without "
            "Azure configured."
        )
        return {"uploaded": [], "skipped": [], "failed": []}

    if not RAW_DIR.exists():
        print(
            "\n[azure] No results/raw directory found -- "
            "nothing to upload."
        )
        return {"uploaded": [], "skipped": [], "failed": []}

    container_client = get_container_client()
    latest = _latest_bronze_blobs(container_client) if SKIP_UNCHANGED_UPLOADS else {}

    run_timestamp = (
        datetime.now(timezone.utc)
        .isoformat()
        .replace(":", "-")
        .replace(".", "-")
    )

    uploaded = []
    skipped = []
    failed = []

    print("\n" + "=" * 60)
    print(f"AZURE BLOB UPLOAD ({BLOB_CONTAINER_NAME} container) -- raw, unfiltered")
    print("=" * 60)

    for json_file in sorted(RAW_DIR.glob("*.json")):

        source_name = json_file.stem

        try:
            records = json.loads(
                json_file.read_text(encoding="utf-8")
            )

        except Exception as exc:
            print(f"[azure] Could not read {json_file.name}: {exc}")
            failed.append(source_name)
            continue

        if not records:
            print(
                f"[azure] {source_name}: 0 records -- "
                f"nothing to upload."
            )
            skipped.append(source_name)
            continue

        if source_name in latest:
            try:
                previous = json.loads(
                    container_client.get_blob_client(latest[source_name])
                    .download_blob().readall()
                )
                if _content_hash(previous) == _content_hash(records):
                    print(
                        f"[azure] {source_name}: unchanged since "
                        f"{latest[source_name]} -- skipping upload."
                    )
                    skipped.append(source_name)
                    continue
            except Exception as exc:
                print(f"[azure] {source_name}: could not compare with last Bronze blob ({exc}) -- uploading.")

        blob_name = (
            f"{source_name}/{run_timestamp[:10]}/{run_timestamp}.json"
        )

        try:
            content = json.dumps(
                records, ensure_ascii=False, indent=2
            )

            container_client.upload_blob(
                name=blob_name,
                data=content,
                overwrite=True,
                content_settings=ContentSettings(
                    content_type="application/json"
                ),
            )

            print(
                f"[azure] {source_name}: uploaded "
                f"{len(records)} records -> "
                f"{BLOB_CONTAINER_NAME}/{blob_name}"
            )
            uploaded.append(source_name)

        except Exception as exc:
            print(f"[azure] {source_name}: upload FAILED: {exc}")
            failed.append(source_name)

    print("-" * 60)
    print(
        f"Uploaded: {len(uploaded)} | "
        f"Skipped (empty/unchanged): {len(skipped)} | "
        f"Failed: {len(failed)}"
    )

    if failed:
        print("Failed sources:", ", ".join(failed))

    print("=" * 60)

    return {
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
    }


if __name__ == "__main__":
    upload_raw_results()