if __package__:
    from ._date_utils import all_records_before_today
else:
    from _date_utils import all_records_before_today

import json
import sys

from datetime import datetime, timezone
from pathlib import Path

import requests


SOURCE_NAME = "ksa_forsah"

# NOTE: this is an undocumented API, found via browser network capture,
# not an officially published endpoint. Same caveat as noted elsewhere in
# السورس ماخخوذ من جيسون فايل موجود في الباكاند يتحدث مع كل مناقصه جديده تنوجد في الفرونت اند 
API_URL = "https://forsah-api.910ths.sa/api/v1/opportunities"

# --- FILTER DISABLED (team decision: bronze layer should be fully raw) ---
# IT-related category filter, confirmed working in earlier testing.
# CATEGORY_IDS = [
#     "7b458ed1-12b6-49bb-b34d-ee5b32a7ecc6",  # Communications & IT Devices
#     "01d1775a-8083-408a-b5a2-9bc603933503",  # Communications & IT Services
# ]
CATEGORY_IDS = []  # empty — no category filter applied, fetches all opportunities

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "results"
    / "raw"
    / "forsah.json"
)

DEBUG_DIR = (
    PROJECT_ROOT
    / "results"
    / "debug"
)

REQUEST_TIMEOUT_S = 30

PER_PAGE = 50

MAX_PAGES = 5


def clean(value):
    return " ".join(str(value or "").split())


def load_existing_records():
    # --- DEDUP DISABLED (team decision: no cross-run dedup, every run
    # is treated as fully fresh) --- original logic preserved below as
    # dead code for easy re-enabling; just delete the line above it.
    return []  # noqa: this line is INTENTIONAL, see comment above

    if not OUTPUT_FILE.exists():
        return []

    try:
        data = json.loads(
            OUTPUT_FILE.read_text(encoding="utf-8")
        )

    except Exception as exc:
        raise RuntimeError(
            f"Could not read existing Forsah JSON: {exc}"
        ) from exc

    if not isinstance(data, list):
        raise RuntimeError(
            "forsah.json must contain a JSON list."
        )

    return data


def save_json_atomic(records):

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    temp_file = OUTPUT_FILE.with_suffix(".json.tmp")

    temp_file.write_text(
        json.dumps(records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    temp_file.replace(OUTPUT_FILE)


def record_key(record):

    return clean(
        record.get("Item ID")
        or
        record.get("Detail URL")
    )


def save_debug(name, content):

    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    try:
        (DEBUG_DIR / f"{name}.txt").write_text(
            content,
            encoding="utf-8"
        )
    except Exception:
        pass


def build_params(page_number):

    params = [
        ("page", str(page_number)),
        ("perPage", str(PER_PAGE)),
    ]

    for category_id in CATEGORY_IDS:
        params.append(("category[]", category_id))

    return params


def parse_response(payload, page_number):

    results = payload.get("result") or []

    page_count = (
        (payload.get("pagination") or {})
        .get("pageCount")
    )

    extracted_at = (
        datetime.now(timezone.utc).isoformat()
    )

    records = []

    for item in results:

        item_id = clean(item.get("id") or "")

        if not item_id:
            continue

        value_range = item.get("valueRange") or {}

        categories = [
            (c.get("name") or {}).get("en")
            for c in (item.get("categories") or [])
            if (c.get("name") or {}).get("en")
        ]

        record = {

            "Item ID": item_id,

            "Title": clean(item.get("title")),

            "Country": "Saudi Arabia",

            "Published Date": clean(item.get("publishDate")),

            "Closing Date": clean(item.get("closeDate")),

            "Detail URL":
                f"https://forsah.sa/marketplace/opportunities/{item_id}",

            "Status": clean(item.get("statusKey")),

            "Type": (item.get("type") or {}).get("key") or "",

            "Due Date": clean(item.get("dueDate")),

            "Value Min": value_range.get("min"),

            "Value Max": value_range.get("max"),

            "Value Label": value_range.get("nameEn"),

            "Categories": categories,

            "Bids Count": item.get("bidsCount"),

            "View Count": item.get("viewCount"),

            "_source": SOURCE_NAME,

            "_page": page_number,

            "_extracted_at": extracted_at,
        }

        records.append(record)

    return records, page_count


def scrape_all_pages(existing_records):

    stored_records = list(existing_records)

    seen_ids = {
        record_key(record)
        for record in existing_records
        if record_key(record)
    }

    total_new = 0

    session = requests.Session()

    session.headers.update({"Accept": "application/json"})

    page_number = 1

    while page_number <= MAX_PAGES:

        print(f"[{SOURCE_NAME}] Fetching page {page_number}...")

        try:
            response = session.get(
                API_URL,
                params=build_params(page_number),
                timeout=REQUEST_TIMEOUT_S,
            )

        except requests.RequestException as exc:
            raise RuntimeError(
                f"Request failed on page "
                f"{page_number}: {exc}"
            ) from exc

        if response.status_code >= 400:

            save_debug(
                f"forsah_page{page_number}_error",
                response.text[:2000]
            )

            raise RuntimeError(
                f"HTTP error {response.status_code} "
                f"on page {page_number}"
            )

        try:
            payload = response.json()

        except ValueError as exc:
            save_debug(
                f"forsah_page{page_number}_bad_json",
                response.text[:2000]
            )
            raise RuntimeError(
                f"Non-JSON response on page "
                f"{page_number}: {exc}"
            ) from exc

        records, page_count = parse_response(payload, page_number)

        page_new = 0
        page_duplicates = 0

        for record in records:

            item_id = record_key(record)

            if not item_id:
                continue

            if item_id in seen_ids:
                page_duplicates += 1
                continue

            seen_ids.add(item_id)
            stored_records.append(record)

            page_new += 1
            total_new += 1

        if page_new > 0 or not OUTPUT_FILE.exists():
            save_json_atomic(stored_records)

        print(
            f"[{SOURCE_NAME}] page={page_number}: "
            f"rows={len(records)} | new={page_new} | "
            f"duplicates={page_duplicates}"
        )

        if page_count and page_number >= page_count:
            print(f"[{SOURCE_NAME}] Reached last page ({page_count}).")
            break

        # Forsah lists newest-first, so a fully-duplicate page means
        # everything after it is old too — stop early.
         #يشوف اخر اي دي اذا موجود يوقف سكرابنق 

        if page_duplicates == len(records) and len(records) > 0:
            print(f"[{SOURCE_NAME}] Page fully duplicate — stopping early.")
            break

        # Speed optimization: Forsah lists newest-first, so once a whole
        # page's "Published Date" is confirmed to be before today, every
        # page after it is too — stop, since only today's data matters
        # for the archive/Blob upload anyway. Safe by design: if any
        # record's date can't be parsed, this simply doesn't trigger.
        if all_records_before_today(records, "Published Date"):
            print(f"[{SOURCE_NAME}] Reached yesterday's date — stopping early.")
            break

        page_number += 1

    return stored_records, total_new, page_number


def main():

    existing_records = load_existing_records()

    print(
        f"[{SOURCE_NAME}] Existing records: "
        f"{len(existing_records)}"
    )

    stored_records, total_new, pages_checked = scrape_all_pages(
        existing_records
    )

    if not OUTPUT_FILE.exists():
        save_json_atomic(stored_records)

    print("\n" + "=" * 60)
    print(f"[{SOURCE_NAME}] SUCCESS")
    print(f"Pages checked: {pages_checked}")
    print(f"Existing records: {len(existing_records)}")
    print(f"New records added: {total_new}")
    print(f"Total stored: {len(stored_records)}")
    print(f"JSON: {OUTPUT_FILE}")
    print("=" * 60)


if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:
        print(f"\n[{SOURCE_NAME}] Stopped by user.")
        sys.exit(130)

    except Exception as exc:
        print(f"\n[{SOURCE_NAME}] FAILED")
        print(f"{type(exc).__name__}: {exc}")
        sys.exit(1)
