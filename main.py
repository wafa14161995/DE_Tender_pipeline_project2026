import subprocess
import sys
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


def main():
    print("=" * 60)
    print("TENDER PIPELINE - RAW EXTRACTION ONLY (no filtering)")
    print("=" * 60)

    extractors = discover_extractors()
    print(f"Extractors found: {len(extractors)}")
    for extractor in extractors:
        print(" -", extractor.name)

    extraction_failed = []

    for extractor in extractors:
        if not run_extractor(extractor):
            extraction_failed.append(extractor.name)
            print(f"WARNING: {extractor.name} extraction failed.")

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
