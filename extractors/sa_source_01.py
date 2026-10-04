if __package__:
    from ._date_utils import all_records_before_today
    from ._sources_config import get_source_setting
else:
    from _date_utils import all_records_before_today
    from _sources_config import get_source_setting

import json
import sys
import time

from datetime import datetime, timezone
from pathlib import Path

import requests


SOURCE_NAME = "sa_source_01"

# NOTE: undocumented endpoint, found via browser network capture
# (DevTools → Network → Fetch/XHR → click page 2 in the pagination).
# ⚠️ تأكدي من الرابط والـ parameters من تبويب Headers → Request URL
# Real endpoint + referer are private (NDA) -> config/sources.local.json
API_URL = get_source_setting(SOURCE_NAME, "api_url")
REFERER = get_source_setting(SOURCE_NAME, "referer_url")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "results" / "raw" / f"{SOURCE_NAME}.json"
DEBUG_DIR = PROJECT_ROOT / "results" / "debug"

REQUEST_TIMEOUT_S = 30
PER_PAGE = 6            # جربي 24 أو 50 — إذا الـ API قبلها يقل عدد الطلبات
MAX_PAGES = 20
DELAY_BETWEEN_PAGES_S = 3   # مهم: بدونه يرجع 429
MAX_RETRIES_429 = 3


def clean(value):
    return " ".join(str(value or "").split())


def load_existing_records():
    # --- DEDUP DISABLED (team decision: no cross-run dedup) ---
    return []  # noqa: this line is INTENTIONAL


def save_json_atomic(records):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_file = OUTPUT_FILE.with_suffix(".json.tmp")
    temp_file.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_file.replace(OUTPUT_FILE)


def save_debug(name, content):
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        (DEBUG_DIR / f"{name}.txt").write_text(content, encoding="utf-8")
    except Exception:
        pass


def record_key(record):
    return clean(record.get("Item ID"))


def build_params(page_number):
    # نفس الـ parameters اللي كان يستخدمها الكود القديم في رابط الصفحة
    return {
        "PublishDateId": "5",
        "SortDirection": "DESC",
        "Sort": "SubmitionDate",
        "PageSize": str(PER_PAGE),
        "IsSearch": "true",
        "PageNumber": str(page_number),
    }


def parse_response(payload, page_number):
    items = payload.get("data") or []
    total_count = payload.get("totalCount")
    extracted_at = datetime.now(timezone.utc).isoformat()

    records = []
    for item in items:
        tender_id = item.get("tenderId")
        if tender_id is None:
            continue

        records.append({
            # نفس أسماء الحقول القديمة عشان الـ Silver ما يتأثر
            "Item ID": str(tender_id),
            "Title": clean(item.get("tenderName")),
            "Country": "Saudi Arabia",
            "Published Date": clean(item.get("submitionDate")),
            "Closing Date": clean(item.get("lastOfferPresentationDate")),
            "Detail URL": "",  # الكود القديم كان ياخذه من الصفحة؛ الـ API ما يرجعه مباشرة
            "Agency": clean(item.get("agencyName")),
            "Activity": clean(item.get("tenderActivityName")),
            "Tender Type": clean(item.get("tenderTypeName")),

            # حقول إضافية مفيدة من الـ API
            "Reference Number": clean(item.get("referenceNumber")),
            "Tender Number": clean(item.get("tenderNumber")),
            "Branch": clean(item.get("branchName")),
            "Last Enquiries Date": clean(item.get("lastEnqueriesDate")),
            "Offers Opening Date": clean(item.get("offersOpeningDate")),
            "Booklet Price": item.get("condetionalBookletPrice"),
            "Financial Fees": item.get("financialFees"),
            "Tender ID String": item.get("tenderIdString"),

            "_raw": item,  # bronze = raw
            "_source": SOURCE_NAME,
            "_page": page_number,
            "_extracted_at": extracted_at,
        })

    return records, total_count


def fetch_page(session, page_number):
    for attempt in range(1, MAX_RETRIES_429 + 1):
        response = session.get(API_URL, params=build_params(page_number), timeout=REQUEST_TIMEOUT_S)

        if response.status_code == 429:
            wait = 30 * attempt
            print(f"[{SOURCE_NAME}] 429 Too Many Requests — waiting {wait}s (attempt {attempt}/{MAX_RETRIES_429})")
            time.sleep(wait)
            continue

        if response.status_code >= 400:
            save_debug(f"{SOURCE_NAME}_page{page_number}_error", response.text[:3000])
            raise RuntimeError(f"HTTP error {response.status_code} on page {page_number}")

        try:
            return response.json()
        except ValueError:
            # غالباً صفحة CAPTCHA بدل JSON
            save_debug(f"{SOURCE_NAME}_page{page_number}_not_json", response.text[:3000])
            raise RuntimeError(
                f"Non-JSON response on page {page_number} "
                f"(possibly CAPTCHA). See results/debug/."
            )

    raise RuntimeError(f"Still rate-limited (429) on page {page_number}")


def scrape_all_pages(existing_records):
    stored_records = list(existing_records)
    seen_ids = {record_key(r) for r in existing_records if record_key(r)}
    total_new = 0

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": REFERER,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    })

    page_number = 1
    while page_number <= MAX_PAGES:
        print(f"[{SOURCE_NAME}] Fetching page {page_number}...")

        payload = fetch_page(session, page_number)
        records, total_count = parse_response(payload, page_number)

        if not records:
            print(f"[{SOURCE_NAME}] Empty page — stopping.")
            break

        page_new = 0
        page_duplicates = 0
        for record in records:
            key = record_key(record)
            if key in seen_ids:
                page_duplicates += 1
                continue
            seen_ids.add(key)
            stored_records.append(record)
            page_new += 1
            total_new += 1

        if page_new > 0 or not OUTPUT_FILE.exists():
            save_json_atomic(stored_records)

        print(
            f"[{SOURCE_NAME}] page={page_number}: rows={len(records)} | "
            f"new={page_new} | duplicates={page_duplicates} | totalCount={total_count}"
        )

        # حماية: لو الـ API رجّع نفس الصفحة (نفس مشكلة الكود القديم) نوقف
        if page_duplicates == len(records):
            print(f"[{SOURCE_NAME}] Page fully duplicate — stopping.")
            break

        # آخر صفحة حسب totalCount
        if total_count and page_number * PER_PAGE >= total_count:
            print(f"[{SOURCE_NAME}] Reached last page.")
            break

        if all_records_before_today(records, "Published Date"):
            print(f"[{SOURCE_NAME}] Reached yesterday's date — stopping early.")
            break

        page_number += 1
        time.sleep(DELAY_BETWEEN_PAGES_S)

    return stored_records, total_new, page_number


def main():
    existing_records = load_existing_records()
    print(f"[{SOURCE_NAME}] Existing records: {len(existing_records)}")

    stored_records, total_new, pages_checked = scrape_all_pages(existing_records)

    if not OUTPUT_FILE.exists():
        save_json_atomic(stored_records)

    print("\n" + "=" * 60)
    print(f"[{SOURCE_NAME}] SUCCESS")
    print(f"Pages checked: {pages_checked}")
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