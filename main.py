import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
EXTRACTORS_DIR = PROJECT_ROOT / "extractors"


def get_sources():
    """Return all runnable extractor files in alphabetical order."""
    sources = []

    for file in EXTRACTORS_DIR.glob("*.py"):
        if file.name == "__init__.py":
            continue

        if file.name.startswith("_"):
            continue

        sources.append(file)

    return sorted(sources)


def run_source(source):
    """Run one extractor without stopping the rest if it fails."""

    print("\n" + "=" * 60)
    print(f"Running source: {source.name}")
    print("=" * 60)

    try:
        result = subprocess.run(
            [sys.executable, "-u", str(source)],
            cwd=PROJECT_ROOT,
            check=False,
        )

        if result.returncode == 0:
            print(f"✅ {source.name} completed successfully.")
            return True

        print(
            f"❌ {source.name} failed "
            f"with exit code {result.returncode}."
        )

        return False

    except KeyboardInterrupt:
        print("\n⛔ Execution stopped by user.")
        raise

    except Exception as error:
        print(
            f"❌ Unexpected error in "
            f"{source.name}: {error}"
        )

        return False


def main():

    print("\nTechnology Tender Extraction Pipeline")
    print("=" * 60)

    sources = get_sources()

    if not sources:
        print(
            "❌ No extractor files found "
            "inside extractors/"
        )

        sys.exit(1)

    print(
        f"Found {len(sources)} source(s):"
    )

    for source in sources:
        print(f" - {source.name}")

    successful_sources = []
    failed_sources = []

    for source in sources:

        if run_source(source):
            successful_sources.append(
                source.name
            )

        else:
            failed_sources.append(
                source.name
            )

    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)

    print(
        f"Total sources: {len(sources)}"
    )

    print(
        f"Successful:    "
        f"{len(successful_sources)}"
    )

    print(
        f"Failed:        "
        f"{len(failed_sources)}"
    )

    if successful_sources:

        print("\n✅ Successful sources:")

        for source in successful_sources:
            print(f" - {source}")

    if failed_sources:

        print("\n❌ Failed sources:")

        for source in failed_sources:
            print(f" - {source}")

    print("\n" + "=" * 60)

    if failed_sources:

        print(
            "⚠️ Extraction finished, "
            "but some sources failed."
        )

        sys.exit(1)

    print(
        "✅ All sources completed successfully."
    )


if __name__ == "__main__":
    main()