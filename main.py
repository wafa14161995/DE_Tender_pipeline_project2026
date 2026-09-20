import subprocess
import sys
import threading
from pathlib import Path

import azure_upload

# ============================================================
# PATHS
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parent
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
    result = subprocess.run([sys.executable, str(extractor)], cwd=PROJECT_ROOT)
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

    extractors = discover_extractors()
    print(f"Extractors found: {len(extractors)}")
    for extractor in extractors:
        print(" -", extractor.name)

    # Split into two batches, each run by its own thread concurrently.
    # Static split (first half / second half), not a dynamic work queue —
    # matches "thread 1 takes 5, thread 2 takes 5" .
    mid = (len(extractors) + 1) // 2
    batch_1 = extractors[:mid]
    batch_2 = extractors[mid:]

    print(f"\nThread 1 ({len(batch_1)}): {[e.name for e in batch_1]}")
    print(f"Thread 2 ({len(batch_2)}): {[e.name for e in batch_2]}")

    extraction_failed = []
    lock = threading.Lock()

    thread_1 = threading.Thread(
        target=run_batch, args=("thread-1", batch_1, extraction_failed, lock)
    )
    thread_2 = threading.Thread(
        target=run_batch, args=("thread-2", batch_2, extraction_failed, lock)
    )

    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()

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
