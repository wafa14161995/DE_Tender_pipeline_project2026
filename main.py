import subprocess
import sys
import threading
import os
from pathlib import Path

# نجلب connection string من Databricks secrets لو dbutils موجود
# (يعني نشتغل داخل Databricks). لو ما موجود (تشغيل محلي)، نتخطى
# هذا السطر و azure_upload.py نفسه يتعامل مع غيابه بدون خطأ.
try:
    dbutils
except NameError:
    pass
else:
    os.environ["AZURE_STORAGE_CONNECTION_STRING"] = dbutils.secrets.get(
        scope="azure-storage",
        key="connection-string"
    )

import azure_upload

# ============================================================
# PATHS
# ============================================================
try:
    PROJECT_ROOT = Path(__file__).resolve().parent
except NameError:
    # __file__ غير موجود لما يشتغل الكود كنوتبوك Databricks
    PROJECT_ROOT = Path(os.getcwd())

EXTRACTORS_DIR = PROJECT_ROOT / "extractors"
RAW_DIR = PROJECT_ROOT / "results" / "raw"

# ============================================================
# ACTIVE -- raw extraction + upload only, no filtering
# ============================================================
def discover_extractors():
    if not EXTRACTORS_DIR.exists():
        return []
    return [
        p for p in sorted(EXTRACTORS_DIR.glob("*.py"))
        if p.name != "__init__.py" and not p.name.startswith("_")
    ]


def run_extractor(extractor):
    print("\n" + "=" * 60)
    print("EXTRACTING:", extractor.name)
    print("=" * 60)

    # نضيف مجلد extractors صراحة لـ PYTHONPATH عشان ملفات
    # مثل _date_utils.py تُلقى دائماً، ونمنع buffering
    # عشان تطلع مخرجات print() أول بأول.
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(EXTRACTORS_DIR)
        + (os.pathsep + existing_pythonpath if existing_pythonpath else "")
    )
    env["PYTHONUNBUFFERED"] = "1"

    result = subprocess.run(
        [sys.executable, "-u", str(extractor)],
        cwd=PROJECT_ROOT,
        env=env,
    )
    return result.returncode == 0


def run_batch(thread_label, extractors, failed_list, lock):
    for extractor in extractors:
        print(f"[{thread_label}] starting {extractor.name}")
        if not run_extractor(extractor):
            with lock:
                failed_list.append(extractor.name)
            print(f"WARNING: {extractor.name} extraction failed.")


def main():
    print("=" * 60)
    print("TENDER PIPELINE - RAW EXTRACTION ONLY (no filtering)")
    print("=" * 60)

    # Cloud job starts with an empty disk: bring back each source's last
    # Bronze JSON first so extractors behave exactly like a local run.
    azure_upload.restore_raw_from_bronze()

    extractors = discover_extractors()
    print(f"Extractors found: {len(extractors)}")
    for extractor in extractors:
        print(" -", extractor.name)

    #  NUM_THREADS to use multi threads to run the 10 portals.
    NUM_THREADS = 4
    batches = [[] for _ in range(NUM_THREADS)]
    for i, extractor in enumerate(extractors):
        batches[i % NUM_THREADS].append(extractor)

    for i, batch in enumerate(batches, start=1):
        print(f"\nThread {i} ({len(batch)}): {[e.name for e in batch]}")

    extraction_failed = []
    lock = threading.Lock()

    threads = [
        threading.Thread(
            target=run_batch,
            args=(f"thread-{i}", batch, extraction_failed, lock),
        )
        for i, batch in enumerate(batches, start=1)
        if batch  # skip empty batches (e.g. fewer extractors than threads)
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)

    if extraction_failed:
        print("Extraction failed:", ", ".join(extraction_failed))
    else:
        print("All extractors completed without error.")

    print("\nRaw files:", RAW_DIR)
    print("=" * 60)

    azure_result = azure_upload.upload_raw_results()

    print("=" * 60)
    print(
        "Pipeline finished"
        + (
            " with warnings."
            if extraction_failed or azure_result["failed"]
            else " successfully."
        )
    )
    print("=" * 60)


if __name__ == "__main__":
    main()