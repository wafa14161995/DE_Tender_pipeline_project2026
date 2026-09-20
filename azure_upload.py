import json
import os

from datetime import datetime, timezone
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContentSettings


PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DIR = PROJECT_ROOT / "results" / "raw"

AZURE_STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING"
)
BLOB_CONTAINER_NAME = os.environ.get(
    "BLOB_CONTAINER_NAME", "bronze"
)


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
        f"Skipped (empty): {len(skipped)} | "
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
