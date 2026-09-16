import json
import sys

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)


SOURCE_NAME = "ksa_etimad"

BASE_URL = "https://tenders.etimad.sa/Tender/AllTendersForVisitor"

# IT-related activity filter, confirmed working via the site's own
# "النشاط الأساسي" (Primary Activity) dropdown filter.
# NOTE: ID 9 "Communications & IT Devices" 

ACTIVITY_IDS = ["9"]

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
    / "etimad.json"
)

DEBUG_DIR = (
    PROJECT_ROOT
    / "results"
    / "debug"
)

PAGE_TIMEOUT_MS = 60_000

MAX_PAGES_PER_ACTIVITY = 5

BLOCK_MARKERS = [
    "captcha",
    "verify you are human",
    "access denied",
    "forbidden",
    "unusual traffic",
    "human visitor",
]


def clean(value):
    return " ".join(
        str(value or "").split()
    )


def load_existing_records():

    if not OUTPUT_FILE.exists():
        return []

    try:
        data = json.loads(
            OUTPUT_FILE.read_text(
                encoding="utf-8"
            )
        )

    except Exception as exc:
        raise RuntimeError(
            f"Could not read existing "
            f"Etimad JSON: {exc}"
        ) from exc

    if not isinstance(data, list):
        raise RuntimeError(
            "etimad.json must contain "
            "a JSON list."
        )

    return data


def save_json_atomic(records):

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temp_file = (
        OUTPUT_FILE
        .with_suffix(".json.tmp")
    )

    temp_file.write_text(
        json.dumps(
            records,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8",
    )

    temp_file.replace(OUTPUT_FILE)


def record_key(record):

    return clean(
        record.get("Item ID")
        or
        record.get("Detail URL")
    )


def save_debug(page, name):

    DEBUG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    try:
        (DEBUG_DIR / f"{name}.html").write_text(
            page.content(),
            encoding="utf-8"
        )

    except Exception:
        pass

    try:
        page.screenshot(
            path=str(DEBUG_DIR / f"{name}.png"),
            full_page=True
        )

    except Exception:
        pass


def check_for_block(page):

    try:
        body_text = (
            page
            .locator("body")
            .inner_text(timeout=8_000)
            .lower()
        )

    except Exception:
        return

    for marker in BLOCK_MARKERS:

        if marker in body_text:
            raise RuntimeError(
                f"Possible block/CAPTCHA "
                f"detected: {marker}"
            )


def build_page_url(activity_id, page_number):

    # Full param set confirmed working via a real browser session
    params = {
        "TenderActivityId": activity_id,
        "PublishDateId": "5",
        "SortDirection": "DESC",
        "Sort": "SubmitionDate",
        "PageSize": "6",
        "IsSearch": "true",
        "PageNumber": str(page_number),
    }

    return f"{BASE_URL}?{urlencode(params)}"


def wait_for_cards(page):

    try:
        page.wait_for_selector(
            ".tender-card",
            timeout=PAGE_TIMEOUT_MS,
        )

    except PlaywrightTimeoutError:

        save_debug(page, "etimad_cards_not_found")

        raise RuntimeError(
            "Etimad tender cards did not appear."
        )

    return page.locator(".tender-card")


def extract_current_page(page, activity_id, page_number):

    check_for_block(page)

    cards = wait_for_cards(page)

    records = []

    extracted_at = (
        datetime.now(timezone.utc).isoformat()
    )

    for index in range(cards.count()):

        card = cards.nth(index)

        item_id = clean(
            card.get_attribute("data-ref") or ""
        )

        title_link = card.locator("h3 a").first

        title = ""
        detail_url = ""

        if card.locator("h3 a").count() > 0:

            title = clean(title_link.inner_text())

            href = title_link.get_attribute("href") or ""

            if href:
                detail_url = (
                    f"https://tenders.etimad.sa{href}"
                    if href.startswith("/")
                    else href
                )

        publish_date = ""

        if card.locator(".col-6 span").count() > 0:
            publish_date = clean(
                card.locator(".col-6 span").first.inner_text()
            )

        tender_type = ""

        if card.locator(".badge-primary").count() > 0:
            tender_type = clean(
                card.locator(".badge-primary").first.inner_text()
            )

        activity = ""

        if card.locator(".col-12.pt-2 span").count() > 0:
            activity = clean(
                card.locator(".col-12.pt-2 span").first.inner_text()
            )

        agency = ""

        agency_p = card.locator(".tender-metadata p.pb-2")

        if agency_p.count() > 0:

            agency = clean(agency_p.first.inner_text())

            details_link = agency_p.first.locator("a")

            if details_link.count() > 0:

                link_text = clean(
                    details_link.first.inner_text()
                )

                if link_text:
                    agency = clean(
                        agency.replace(link_text, "")
                    )

        # Reference/deadline fields block — generic label -> value
        # capture, same approach as the Node.js version used.
        fields = {}

        date_blocks = card.locator(
            ".tender-date .col-12.col-md-3"
        )

        for block_index in range(date_blocks.count()):

            block = date_blocks.nth(block_index)

            label_el = block.locator("label")

            if label_el.count() == 0:
                continue

            label = clean(label_el.first.inner_text())

            value_spans = block.locator("span")

            values = [
                clean(value_spans.nth(i).inner_text())
                for i in range(value_spans.count())
            ]

            values = [v for v in values if v]

            if label:
                fields[label] = " ".join(values)

        if not item_id and not detail_url:
            continue

        record = {

            "Item ID": item_id,

            "Title": title,

            "Country": "Saudi Arabia",

            "Published Date": publish_date,

            "Closing Date":
                fields.get("آخر موعد لتقديم العروض", ""),

            "Detail URL": detail_url,

            "Agency": agency,

            "Activity": activity,

            "Tender Type": tender_type,

            "Reference Fields": fields,

            "_source": SOURCE_NAME,

            "_page": page_number,

            "_activity_id": activity_id,

            "_extracted_at": extracted_at,
        }

        records.append(record)

    return records


def scrape_activity(activity_id, seen_ids, stored_records):

    total_new = 0

    with sync_playwright() as playwright:

        browser = playwright.chromium.launch(headless=True)

        context = browser.new_context(
            locale="ar-SA",
            viewport={"width": 1440, "height": 1100},
        )

        page = context.new_page()

        page.set_default_timeout(PAGE_TIMEOUT_MS)

        try:

            page_number = 1

            while page_number <= MAX_PAGES_PER_ACTIVITY:

                url = build_page_url(activity_id, page_number)

                print(
                    f"[{SOURCE_NAME}] activity={activity_id} "
                    f"Loading page {page_number}..."
                )

                response = page.goto(
                    url,
                    wait_until="networkidle",
                    timeout=PAGE_TIMEOUT_MS,
                )

                if response is not None and response.status >= 400:
                    raise RuntimeError(
                        f"HTTP error: {response.status}"
                    )

                records = extract_current_page(
                    page, activity_id, page_number
                )

                if not records:
                    print(
                        f"[{SOURCE_NAME}] No cards on page "
                        f"{page_number} — stopping this activity."
                    )
                    break

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
                    f"[{SOURCE_NAME}] activity={activity_id} "
                    f"page={page_number}: rows={len(records)} | "
                    f"new={page_new} | duplicates={page_duplicates}"
                )

                # Etimad lists newest-first, so a fully-duplicate page
                # means everything after it is old too — stop early.
                #يشوف اخر اي دي اذا موجود يوقف سكرابنق 
                if page_duplicates == len(records) and len(records) > 0:
                    print(
                        f"[{SOURCE_NAME}] Page fully duplicate — "
                        f"stopping this activity early."
                    )
                    break

                page_number += 1

        finally:

            context.close()
            browser.close()

    return total_new


def main():

    existing_records = load_existing_records()

    print(
        f"[{SOURCE_NAME}] Existing records: "
        f"{len(existing_records)}"
    )

    stored_records = list(existing_records)

    seen_ids = {
        record_key(record)
        for record in existing_records
        if record_key(record)
    }

    total_new = 0

    for activity_id in ACTIVITY_IDS:
        total_new += scrape_activity(
            activity_id, seen_ids, stored_records
        )

    if not OUTPUT_FILE.exists():
        save_json_atomic(stored_records)

    print("\n" + "=" * 60)
    print(f"[{SOURCE_NAME}] SUCCESS")
    print(f"Activities checked: {len(ACTIVITY_IDS)}")
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

    except PlaywrightTimeoutError as exc:
        print(f"\n[{SOURCE_NAME}] PLAYWRIGHT TIMEOUT")
        print(exc)
        sys.exit(1)

    except Exception as exc:
        print(f"\n[{SOURCE_NAME}] FAILED")
        print(f"{type(exc).__name__}: {exc}")
        sys.exit(1)
