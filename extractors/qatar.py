import json
import re
import sys
import time

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)


SOURCE_NAME = "qatar_globaltenders"

START_URL = (
    "https://www.globaltenders.com/"
    "qatar-tenders"
)

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
    / "qatar.json"
)

DEBUG_DIR = (
    PROJECT_ROOT
    / "results"
    / "debug"
)

PAGE_TIMEOUT_MS = 60_000

# GlobalTenders يسمح للضيف بأول 100 نتيجة فقط.
# 20 نتيجة × 5 صفحات = 100
MAX_GUEST_PAGES = 5


BLOCK_MARKERS = [
    "captcha",
    "verify you are human",
    "access denied",
    "forbidden",
    "unusual traffic",
    "cloudflare",
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
            f"Qatar JSON: {exc}"
        ) from exc

    if not isinstance(
        data,
        list
    ):
        raise RuntimeError(
            "qatar.json must contain "
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
        .with_suffix(
            ".json.tmp"
        )
    )

    temp_file.write_text(
        json.dumps(
            records,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8",
    )

    temp_file.replace(
        OUTPUT_FILE
    )


def record_key(record):

    return clean(
        record.get(
            "Item ID"
        )
        or
        record.get(
            "Detail URL"
        )
    )


def save_debug(
    page,
    name
):

    DEBUG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    try:
        (
            DEBUG_DIR
            / f"{name}.html"
        ).write_text(
            page.content(),
            encoding="utf-8"
        )

    except Exception:
        pass

    try:
        page.screenshot(
            path=str(
                DEBUG_DIR
                / f"{name}.png"
            ),
            full_page=True
        )

    except Exception:
        pass


def check_for_block(page):

    try:
        body_text = (
            page
            .locator("body")
            .inner_text(
                timeout=8_000
            )
            .lower()
        )

    except Exception:
        return

    for marker in BLOCK_MARKERS:

        if marker in body_text:
            raise RuntimeError(
                f"Possible block/"
                f"CAPTCHA detected: "
                f"{marker}"
            )


def card_locator(page):

    return page.locator(
        '[id^="tender_"]'
    )


def wait_for_cards(page):

    deadline = (
        time.time()
        +
        PAGE_TIMEOUT_MS / 1000
    )

    while time.time() < deadline:

        check_for_block(
            page
        )

        cards = card_locator(
            page
        )

        if cards.count() > 0:
            return cards

        page.wait_for_timeout(
            400
        )

    save_debug(
        page,
        "qatar_cards_not_found"
    )

    raise RuntimeError(
        "Qatar tender cards "
        "did not appear."
    )


def extract_current_page(
    page,
    page_number
):

    cards = wait_for_cards(
        page
    )

    records = []

    extracted_at = (
        datetime
        .now(timezone.utc)
        .isoformat()
    )

    for index in range(
        cards.count()
    ):

        card = cards.nth(
            index
        )

        item_id = clean(
            card.get_attribute(
                "id"
            )
            or ""
        ).removeprefix(
            "tender_"
        )

        full_text = clean(
            card.inner_text()
        )

        detail_url = ""

        detail_link = (
            card
            .locator(
                'a[href*="tender-detail"], '
                'a:has-text("View Detail")'
            )
            .first
        )

        if detail_link.count() > 0:

            href = (
                detail_link
                .get_attribute(
                    "href"
                )
                or ""
            )

            if href:
                detail_url = urljoin(
                    page.url,
                    href
                )

        dates = re.findall(
            r"\b\d{1,2}\s+"
            r"[A-Za-z]{3}\s+"
            r"\d{4}\b",
            full_text
        )

        published_date = (
            dates[0]
            if len(dates) >= 1
            else ""
        )

        closing_date = (
            dates[1]
            if len(dates) >= 2
            else ""
        )

        title = ""

        title_candidates = (
            card.locator(
                "h2, h3, h4, h5, "
                ".tender-title, "
                ".title"
            )
        )

        for title_index in range(
            title_candidates.count()
        ):

            candidate = clean(
                title_candidates
                .nth(title_index)
                .inner_text()
            )

            if candidate:
                title = candidate
                break

        if not title:

            prefix = full_text

            if (
                published_date
                and
                published_date
                in prefix
            ):
                prefix = (
                    prefix.split(
                        published_date,
                        1
                    )[0]
                )

            title = clean(
                prefix.removesuffix(
                    "Qatar"
                )
            )

        if (
            not item_id
            and
            not detail_url
        ):
            continue

        record = {

            "Item ID":
                item_id,

            "Title":
                title,

            "Country":
                "Qatar",

            "Published Date":
                published_date,

            "Closing Date":
                closing_date,

            "Detail URL":
                detail_url,

            "_source":
                SOURCE_NAME,

            "_page":
                page_number,

            "_extracted_at":
                extracted_at,
        }

        records.append(
            record
        )

    return records


def page_fingerprint(page):

    records = (
        extract_current_page(
            page,
            0
        )
    )

    return tuple(
        (
            record_key(record),
            record.get(
                "Title",
                ""
            )
        )

        for record
        in records[:5]
    )


def find_next_button(page):

    next_pattern = re.compile(
        r"^\s*Next\s*$",
        re.IGNORECASE
    )

    candidates = [

        page.get_by_role(
            "link",
            name=next_pattern
        ),

        page.get_by_role(
            "button",
            name=next_pattern
        ),

        page.locator(
            'a[rel="next"], '
            'a[aria-label*="Next" i], '
            'button[aria-label*="Next" i]'
        ),

        page.locator(
            "li.next a, "
            "li.next button, "
            ".next a, "
            ".next button"
        ),
    ]

    for locator in candidates:

        for index in range(
            locator.count()
        ):

            candidate = (
                locator.nth(
                    index
                )
            )

            try:

                if candidate.is_visible():
                    return candidate

            except Exception:
                pass

    return None


def is_disabled(element):

    try:
        return bool(
            element.evaluate(
                """
                (el) => {

                    const ownDisabled =
                        el.disabled === true
                        ||
                        el.hasAttribute(
                            'disabled'
                        );

                    const ariaDisabled =
                        (
                            el.getAttribute(
                                'aria-disabled'
                            )
                            || ''
                        )
                        .toLowerCase()
                        === 'true';

                    const classDisabled =
                        el.classList
                        .contains(
                            'disabled'
                        );

                    const parentDisabled =
                        !!el.closest(
                            '.disabled, '
                            '[aria-disabled="true"]'
                        );

                    return (
                        ownDisabled
                        ||
                        ariaDisabled
                        ||
                        classDisabled
                        ||
                        parentDisabled
                    );
                }
                """
            )
        )

    except Exception:
        return False


def go_to_next_page(
    page,
    previous_fingerprint
):

    next_button = (
        find_next_button(
            page
        )
    )

    if (
        next_button is None
        or
        is_disabled(
            next_button
        )
    ):
        return False

    next_button.scroll_into_view_if_needed()

    next_button.click(
        timeout=15_000
    )

    deadline = (
        time.time()
        +
        20
    )

    while time.time() < deadline:

        check_for_block(
            page
        )

        try:

            current = (
                page_fingerprint(
                    page
                )
            )

            if (
                current
                and
                current
                != previous_fingerprint
            ):
                return True

        except Exception:
            pass

        page.wait_for_timeout(
            350
        )

    return False


def open_website(page):

    last_error = None

    for attempt in range(
        1,
        4
    ):

        try:

            print(
                f"[{SOURCE_NAME}] "
                f"Opening website attempt "
                f"{attempt}/3..."
            )

            response = page.goto(
                START_URL,
                wait_until=
                    "domcontentloaded",
                timeout=
                    PAGE_TIMEOUT_MS,
            )

            if (
                response is not None
                and
                response.status >= 400
            ):
                raise RuntimeError(
                    f"HTTP error: "
                    f"{response.status}"
                )

            check_for_block(
                page
            )

            wait_for_cards(
                page
            )

            return

        except Exception as exc:

            last_error = exc

            print(
                f"[{SOURCE_NAME}] "
                f"Open failed: "
                f"{exc}"
            )

            if attempt < 3:

                page.wait_for_timeout(
                    4_000
                )

    raise RuntimeError(
        f"Could not open "
        f"GlobalTenders Qatar: "
        f"{last_error}"
    )


def scrape_guest_pages(
    existing_records
):

    stored_records = list(
        existing_records
    )

    seen_ids = {
        record_key(record)

        for record
        in existing_records

        if record_key(
            record
        )
    }

    seen_pages = set()

    total_new = 0

    with sync_playwright() as playwright:

        browser = (
            playwright
            .chromium
            .launch(
                headless=True
            )
        )

        context = (
            browser
            .new_context(

                locale="en-US",

                viewport={
                    "width": 1440,
                    "height": 1100
                },
            )
        )

        page = (
            context.new_page()
        )

        page.set_default_timeout(
            PAGE_TIMEOUT_MS
        )

        try:

            open_website(
                page
            )

            page_number = 1

            while (
                page_number
                <= MAX_GUEST_PAGES
            ):

                print(
                    f"[{SOURCE_NAME}] "
                    f"Reading page "
                    f"{page_number}/"
                    f"{MAX_GUEST_PAGES}..."
                )

                current_fingerprint = (
                    page_fingerprint(
                        page
                    )
                )

                if not current_fingerprint:

                    raise RuntimeError(
                        f"No Qatar records "
                        f"found on page "
                        f"{page_number}."
                    )

                if (
                    current_fingerprint
                    in seen_pages
                ):
                    raise RuntimeError(
                        f"Page {page_number} "
                        f"repeated earlier "
                        f"Qatar content."
                    )

                seen_pages.add(
                    current_fingerprint
                )

                records = (
                    extract_current_page(
                        page,
                        page_number
                    )
                )

                page_new = 0
                page_duplicates = 0

                for record in records:

                    item_id = (
                        record_key(
                            record
                        )
                    )

                    if not item_id:
                        continue

                    if item_id in seen_ids:

                        page_duplicates += 1
                        continue

                    seen_ids.add(
                        item_id
                    )

                    stored_records.append(
                        record
                    )

                    page_new += 1
                    total_new += 1

                # نحفظ بعد كل صفحة.
                # لو صار شيء بعدها،
                # ما تضيع الصفحات السابقة.
                if (
                    page_new > 0
                    or
                    not OUTPUT_FILE.exists()
                ):

                    save_json_atomic(
                        stored_records
                    )

                print(
                    f"[{SOURCE_NAME}] "
                    f"Page {page_number}: "
                    f"rows={len(records)} | "
                    f"new={page_new} | "
                    f"duplicates="
                    f"{page_duplicates}"
                )

                # أهم جزء:
                # وصلنا الصفحة الخامسة؟
                # نتوقف بنجاح.
                if (
                    page_number
                    ==
                    MAX_GUEST_PAGES
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        "Guest limit reached "
                        "by design: "
                        "5 pages / "
                        "100 visible tenders."
                    )

                    break

                moved = (
                    go_to_next_page(
                        page,
                        current_fingerprint
                    )
                )

                if not moved:

                    raise RuntimeError(
                        f"Could not move "
                        f"from Qatar page "
                        f"{page_number} "
                        f"to "
                        f"{page_number + 1}."
                    )

                page_number += 1

            return (
                stored_records,
                total_new,
                page_number
            )

        finally:

            context.close()

            browser.close()


def main():

    existing_records = (
        load_existing_records()
    )

    print(
        f"[{SOURCE_NAME}] "
        f"Existing records: "
        f"{len(existing_records)}"
    )

    (
        stored_records,
        total_new,
        pages_checked

    ) = scrape_guest_pages(
        existing_records
    )

    if not OUTPUT_FILE.exists():

        save_json_atomic(
            stored_records
        )

    print(
        "\n"
        + "=" * 60
    )

    print(
        f"[{SOURCE_NAME}] "
        "SUCCESS"
    )

    print(
        f"Pages checked: "
        f"{pages_checked}"
    )

    print(
        "Guest-visible scope: "
        "first 100 tenders "
        "(5 pages)"
    )

    print(
        f"Existing records: "
        f"{len(existing_records)}"
    )

    print(
        f"New records added: "
        f"{total_new}"
    )

    print(
        f"Total stored: "
        f"{len(stored_records)}"
    )

    print(
        f"JSON: "
        f"{OUTPUT_FILE}"
    )

    print(
        "=" * 60
    )


if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print(
            f"\n[{SOURCE_NAME}] "
            "Stopped by user."
        )

        sys.exit(130)

    except PlaywrightTimeoutError as exc:

        print(
            f"\n[{SOURCE_NAME}] "
            "PLAYWRIGHT TIMEOUT"
        )

        print(exc)

        sys.exit(1)

    except Exception as exc:

        print(
            f"\n[{SOURCE_NAME}] "
            "FAILED"
        )

        print(
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        sys.exit(1)
