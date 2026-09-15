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


SOURCE_NAME = "uae_mof"

START_URL = (
    "https://mof.gov.ae/en/public-finance/"
    "government-procurement/"
    "current-business-opportunities/"
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
    / "uae_mof.json"
)

PAGE_TIMEOUT_MS = 60_000
MAX_PAGES = 500


EXPECTED_HEADERS = [
    "RFQ Number",
    "Entity Name",
    "Title",
    "Open Date",
    "Close Date",
    "Link",
]


BLOCK_MARKERS = [
    "captcha",
    "verify you are human",
    "are you human",
    "access denied",
    "unusual traffic",
    "cloudflare",
]


def clean(value):

    return " ".join(
        str(value or "").split()
    )


def load_existing():

    if not OUTPUT_FILE.exists():
        return []

    data = json.loads(
        OUTPUT_FILE.read_text(
            encoding="utf-8"
        )
    )

    if not isinstance(
        data,
        list
    ):

        raise RuntimeError(
            "uae_mof.json must "
            "contain a JSON list"
        )

    return data


def save_atomic(records):

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temp = (
        OUTPUT_FILE
        .with_suffix(
            ".json.tmp"
        )
    )

    temp.write_text(
        json.dumps(
            records,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8",
    )

    temp.replace(
        OUTPUT_FILE
    )


def record_key(record):

    return clean(
        record.get(
            "RFQ Number"
        )
    )


def check_block(page):

    try:

        text = (
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

        if marker in text:

            raise RuntimeError(
                f"Possible block/"
                f"CAPTCHA detected: "
                f"{marker}"
            )


def find_tender_table(page):

    tables = page.locator(
        "table"
    )

    for i in range(
        tables.count()
    ):

        table = tables.nth(i)

        try:

            header_text = clean(
                table
                .locator("thead")
                .inner_text(
                    timeout=1_500
                )
            )

        except Exception:

            header_text = ""

        if not header_text:

            try:

                header_text = clean(
                    table
                    .locator("tr")
                    .first
                    .inner_text(
                        timeout=1_500
                    )
                )

            except Exception:
                continue

        if all(
            h.lower()
            in header_text.lower()

            for h
            in EXPECTED_HEADERS
        ):

            return table

    return None


def wait_for_table(page):

    deadline = (
        time.time()
        +
        PAGE_TIMEOUT_MS / 1000
    )

    while time.time() < deadline:

        check_block(page)

        table = (
            find_tender_table(
                page
            )
        )

        if table is not None:

            try:

                if table.is_visible():
                    return table

            except Exception:
                pass

        page.wait_for_timeout(
            400
        )

    raise RuntimeError(
        "UAE MoF tender table "
        "did not appear before timeout"
    )


def extract_page(
    page,
    page_number
):

    table = wait_for_table(
        page
    )

    rows = table.locator(
        "tbody tr"
    )

    extracted_at = (
        datetime
        .now(timezone.utc)
        .isoformat()
    )

    records = []

    for i in range(
        rows.count()
    ):

        cells = (
            rows.nth(i)
            .locator("td")
        )

        if cells.count() < 6:
            continue

        link = ""

        link_el = (
            cells.nth(5)
            .locator("a")
            .first
        )

        if link_el.count() > 0:

            href = (
                link_el
                .get_attribute(
                    "href"
                )
                or ""
            )

            if href:

                link = urljoin(
                    START_URL,
                    href
                )

        record = {

            "RFQ Number":
                clean(
                    cells.nth(0)
                    .inner_text()
                ),

            "Entity Name":
                clean(
                    cells.nth(1)
                    .inner_text()
                ),

            "Title":
                clean(
                    cells.nth(2)
                    .inner_text()
                ),

            "Open Date":
                clean(
                    cells.nth(3)
                    .inner_text()
                ),

            "Close Date":
                clean(
                    cells.nth(4)
                    .inner_text()
                ),

            "Link":
                link,

            "_source":
                SOURCE_NAME,

            "_page":
                page_number,

            "_extracted_at":
                extracted_at,
        }

        if (
            record["RFQ Number"]
            or
            record["Title"]
        ):

            records.append(
                record
            )

    return records


def fingerprint(records):

    return tuple(
        (
            record_key(r),
            r.get(
                "Title",
                ""
            )
        )

        for r
        in records[:5]
    )


def pagination_container(page):

    selectors = [
        "ul.pagination",
        "nav[aria-label*='pagination' i]",
        ".pagination",
        ".pager",
    ]

    for selector in selectors:

        loc = page.locator(
            selector
        )

        for i in range(
            loc.count()
        ):

            candidate = (
                loc.nth(i)
            )

            try:

                if candidate.is_visible():

                    text = clean(
                        candidate.inner_text()
                    )

                    if (
                        "Next" in text
                        or
                        "Last" in text
                        or
                        re.search(
                            r"\b\d+\b",
                            text
                        )
                    ):

                        return candidate

            except Exception:
                pass

    return None


def find_next(page):

    pager = (
        pagination_container(
            page
        )
    )

    exact = re.compile(
        r"^\s*Next\s*$",
        re.IGNORECASE
    )

    root = (
        pager
        if pager is not None
        else page
    )

    candidates = [
        root.get_by_role(
            "link",
            name=exact
        ),

        root.get_by_role(
            "button",
            name=exact
        ),

        root.locator(
            'a[rel="next"], '
            'a[aria-label*="Next" i], '
            'button[aria-label*="Next" i], '
            'li.next a, '
            'li.next button, '
            '.next a, '
            '.next button'
        ),
    ]

    for locator in candidates:

        for i in range(
            locator.count()
        ):

            item = locator.nth(i)

            try:

                if item.is_visible():
                    return item

            except Exception:
                pass

    return None


def is_disabled(element):

    try:

        return bool(
            element.evaluate(
                """
                el =>
                    el.disabled === true ||
                    el.hasAttribute(
                        'disabled'
                    ) ||
                    (
                        (
                            el.getAttribute(
                                'aria-disabled'
                            )
                            || ''
                        ).toLowerCase()
                        === 'true'
                    ) ||
                    el.classList.contains(
                        'disabled'
                    ) ||
                    !!el.closest(
                        '.disabled,'
                        '[aria-disabled="true"]'
                    )
                """
            )
        )

    except Exception:
        return False


def move_next(
    page,
    old_fingerprint
):

    next_button = (
        find_next(
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

    # نجرب مرتين فقط.
    for attempt in range(
        1,
        3
    ):

        try:

            next_button.scroll_into_view_if_needed()

            next_button.click(
                timeout=15_000
            )

        except Exception:

            if attempt == 2:
                return False

        deadline = (
            time.time()
            +
            8
        )

        while time.time() < deadline:

            check_block(page)

            try:

                current = fingerprint(
                    extract_page(
                        page,
                        0
                    )
                )

                if (
                    current
                    and
                    current
                    != old_fingerprint
                ):
                    return True

            except Exception:
                pass

            page.wait_for_timeout(
                350
            )

        # DOM ممكن تغير،
        # فنجيب Next من جديد.
        next_button = (
            find_next(
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

    # أهم تعديل:
    # إذا ضغطنا Next مرتين
    # والجدول ما تغير،
    # نعتبر الصفحة الحالية هي الأخيرة.
    return False


def open_site(page):

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

            check_block(
                page
            )

            wait_for_table(
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
        f"Could not open UAE MoF: "
        f"{last_error}"
    )


def run():

    existing = (
        load_existing()
    )

    seen_ids = {
        record_key(r)
        for r
        in existing
        if record_key(r)
    }

    stored = list(
        existing
    )

    seen_pages = set()

    added = 0

    print(
        f"[{SOURCE_NAME}] "
        f"Existing records: "
        f"{len(existing)}"
    )

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

            open_site(
                page
            )

            page_number = 1

            while (
                page_number
                <= MAX_PAGES
            ):

                print(
                    f"[{SOURCE_NAME}] "
                    f"Reading page "
                    f"{page_number}..."
                )

                records = (
                    extract_page(
                        page,
                        page_number
                    )
                )

                if not records:

                    raise RuntimeError(
                        f"No UAE tenders "
                        f"found on page "
                        f"{page_number}"
                    )

                current_fp = (
                    fingerprint(
                        records
                    )
                )

                if (
                    current_fp
                    in seen_pages
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        "Repeated page "
                        "detected; "
                        "stopping safely."
                    )

                    break

                seen_pages.add(
                    current_fp
                )

                page_new = 0

                page_duplicates = 0

                for record in records:

                    rid = (
                        record_key(
                            record
                        )
                    )

                    if not rid:
                        continue

                    if rid in seen_ids:

                        page_duplicates += 1
                        continue

                    seen_ids.add(
                        rid
                    )

                    stored.append(
                        record
                    )

                    page_new += 1
                    added += 1

                if (
                    page_new
                    or
                    not OUTPUT_FILE.exists()
                ):

                    save_atomic(
                        stored
                    )

                print(
                    f"[{SOURCE_NAME}] "
                    f"Page {page_number}: "
                    f"rows={len(records)} | "
                    f"new={page_new} | "
                    f"duplicates="
                    f"{page_duplicates}"
                )

                if not move_next(
                    page,
                    current_fp
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        f"Reached final page: "
                        f"{page_number}"
                    )

                    break

                page_number += 1

            else:

                raise RuntimeError(
                    "UAE pagination "
                    "safety limit reached"
                )

        finally:

            context.close()

            browser.close()

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
        f"{page_number}"
    )

    print(
        f"Existing records: "
        f"{len(existing)}"
    )

    print(
        f"New records added: "
        f"{added}"
    )

    print(
        f"Total stored: "
        f"{len(stored)}"
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

        run()

    except KeyboardInterrupt:

        print(
            f"\n[{SOURCE_NAME}] "
            "Stopped by user."
        )

        sys.exit(130)

    except PlaywrightTimeoutError as exc:

        print(
            f"\n[{SOURCE_NAME}] "
            f"PLAYWRIGHT TIMEOUT\n"
            f"{exc}"
        )

        sys.exit(1)

    except Exception as exc:

        print(
            f"\n[{SOURCE_NAME}] "
            f"FAILED\n"
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        sys.exit(1)
