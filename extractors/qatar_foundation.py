if __package__:
    from ._browser_fallback import open_with_fallback
else:
    from _browser_fallback import open_with_fallback

import json
import re
import sys
import time

from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)


# ============================================================
# SOURCE
# ============================================================

SOURCE_NAME = "qatar_foundation"

START_URL = "https://suppliers.qf.org.qa/abstract"


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

OUTPUT_FILE = (
    PROJECT_ROOT
    / "results"
    / "raw"
    / "qatar_foundation.json"
)


# ============================================================
# SETTINGS
# ============================================================

PAGE_TIMEOUT_MS = 60_000

ROWS_WAIT_SECONDS = 40

PAGE_DELAY_MS = 1500

MAX_PAGES = 50

HEADLESS = True


# ============================================================
# BLOCK MARKERS
# ============================================================

BLOCK_MARKERS = [
    "captcha",
    "verify you are human",
    "access denied",
    "forbidden",
    "too many requests",
    "unusual traffic",
]


# ============================================================
# REGEX
# ============================================================

# مثال:
#
# CO-RFQ-530
# PUE-RFQ-386
# PUE-RFQ-381,1
# QF-RFQ-1409
# HBKU-RFQ-2377

NEGOTIATION_NUMBER_PATTERN = (
    r"[A-Z][A-Z0-9]*-"
    r"(?:RFQ|RFP|RFI|ITT|EOI|RFX)-"
    r"[A-Z0-9][A-Z0-9,./-]*"
)


# مثال:
#
# 13-Sep-2026 9:10 AM

DATE_TIME_PATTERN = (
    r"\d{1,2}-"
    r"[A-Za-z]{3}-"
    r"\d{4}"
    r"\s+"
    r"\d{1,2}:\d{2}"
    r"\s+"
    r"(?:AM|PM)"
)


# الصف الكامل:
#
# Number
# Title
# RFQ
# Active
# Posting Date
# Open Date
# Close Date

TENDER_PATTERN = re.compile(

    rf"(?P<number>"
    rf"{NEGOTIATION_NUMBER_PATTERN}"
    rf")"

    rf"\s+"

    rf"(?P<title>.*?)"

    rf"\s+"

    rf"(?P<type>"
    rf"RFQ|RFP|RFI|ITT|EOI|RFX"
    rf")"

    rf"\s+"

    # Status
    #
    # Active
    # Closed
    # Amended
    # وغيرها
    rf"(?P<status>"
    rf"[A-Za-z][A-Za-z ]{{0,30}}?"
    rf")"

    rf"\s+"

    rf"(?P<posting>"
    rf"{DATE_TIME_PATTERN}"
    rf")"

    rf"\s+"

    rf"(?P<open>"
    rf"{DATE_TIME_PATTERN}"
    rf")"

    rf"\s+"

    rf"(?P<close>"
    rf"{DATE_TIME_PATTERN}"
    rf")",

    re.IGNORECASE,
)


# ============================================================
# CLEAN
# ============================================================

def clean(value):

    return " ".join(
        str(value or "").split()
    )


# ============================================================
# VALID NEGOTIATION NUMBER
# ============================================================

def valid_number(value):

    value = clean(value)

    return bool(
        re.fullmatch(
            NEGOTIATION_NUMBER_PATTERN,
            value,
            re.IGNORECASE,
        )
    )


# ============================================================
# LOAD EXISTING
# ============================================================

def load_existing():
    # --- DEDUP DISABLED -- original logic preserved below as
    # dead code for easy re-enabling; just delete the line above it.
    return []  # noqa: this line is INTENTIONAL, see comment above

    if not OUTPUT_FILE.exists():
        return []


    try:

        data = json.loads(
            OUTPUT_FILE.read_text(
                encoding="utf-8"
            )
        )


        if not isinstance(
            data,
            list
        ):
            return []


        # ====================================================
        # مهم جدًا
        #
        # الملف القديم عندنا فيه سجلات خاطئة:
        #
        # Negotiation Number = ""
        #
        # والـTitle عبارة عن الصفحة كلها.
        #
        # هنا نتجاهلها تلقائيًا.
        # ====================================================

        good_records = []


        for record in data:

            number = clean(
                record.get(
                    "Negotiation Number"
                )
            )


            if valid_number(
                number
            ):

                good_records.append(
                    record
                )


        return good_records


    except Exception:

        return []


# ============================================================
# SAVE
# ============================================================

def save_atomic(records):

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
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
            indent=2,
        ),

        encoding="utf-8",
    )


    temp_file.replace(
        OUTPUT_FILE
    )


# ============================================================
# UNIQUE KEY
# ============================================================

def record_key(record):

    return clean(
        record.get(
            "Negotiation Number"
        )
    ).casefold()


# ============================================================
# CHECK BLOCK
# ============================================================

def check_block(page):

    try:

        text = (
            page
            .locator("body")
            .inner_text(
                timeout=5_000
            )
            .lower()
        )


    except Exception:

        return


    for marker in BLOCK_MARKERS:

        if marker in text:

            raise RuntimeError(
                "Possible Qatar Foundation "
                "access restriction: "
                f"{marker}"
            )


# ============================================================
# READ PAGE TEXT
# ============================================================

def get_page_text(page):

    try:

        text = (
            page
            .locator("body")
            .inner_text(
                timeout=10_000
            )
        )

        return clean(
            text
        )


    except Exception:

        return ""


# ============================================================
# PARSE TENDERS FROM PAGE TEXT
# ============================================================

def parse_tenders_from_text(
    text,
    page_number,
):

    records = []


    for match in TENDER_PATTERN.finditer(
        text
    ):

        number = clean(
            match.group(
                "number"
            )
        )


        title = clean(
            match.group(
                "title"
            )
        )


        negotiation_type = clean(
            match.group(
                "type"
            )
        ).upper()


        status = clean(
            match.group(
                "status"
            )
        )


        posting_date = clean(
            match.group(
                "posting"
            )
        )


        open_date = clean(
            match.group(
                "open"
            )
        )


        close_date = clean(
            match.group(
                "close"
            )
        )


        # ----------------------------------------------------
        # حماية من أي نتيجة غريبة
        # ----------------------------------------------------

        if not valid_number(
            number
        ):
            continue


        if not title:
            continue


        if len(
            title
        ) > 500:

            continue


        record = {

            "Negotiation Number":
                number,

            "Title":
                title,

            "Negotiation Type":
                negotiation_type,

            "Status":
                status,

            "Posting Date":
                posting_date,

            "Open Date":
                open_date,

            "Close Date":
                close_date,

            "_source":
                SOURCE_NAME,

            "_page":
                page_number,

            "_extracted_at":
                datetime
                .now(
                    timezone.utc
                )
                .isoformat(),
        }


        records.append(
            record
        )


    # ========================================================
    # REMOVE DUPLICATES FROM SAME PAGE
    # ========================================================

    unique = []

    seen = set()


    for record in records:

        key = record_key(
            record
        )


        if not key:
            continue


        if key in seen:
            continue


        seen.add(
            key
        )


        unique.append(
            record
        )


    return unique


# ============================================================
# EXTRACT CURRENT PAGE
# ============================================================

def extract_current_page(
    page,
    page_number,
):

    text = get_page_text(
        page
    )


    if not text:
        return []


    return parse_tenders_from_text(
        text,
        page_number,
    )


# ============================================================
# WAIT UNTIL TENDERS LOAD
# ============================================================

def wait_for_records(
    page,
    page_number,
):

    deadline = (
        time.time()
        +
        ROWS_WAIT_SECONDS
    )


    while (
        time.time()
        <
        deadline
    ):

        check_block(
            page
        )


        records = extract_current_page(
            page,
            page_number,
        )


        if records:

            return records


        page.wait_for_timeout(
            750
        )


    raise RuntimeError(
        "Qatar Foundation page loaded, "
        "but no individual tender records "
        "could be parsed."
    )


# ============================================================
# PAGE FINGERPRINT
# ============================================================

def fingerprint(records):

    return tuple(

        record_key(
            record
        )

        for record
        in records[:5]

        if record_key(
            record
        )
    )


# ============================================================
# FIND NEXT BUTTON
# ============================================================

def find_next_button(page):

    selectors = [

        'a[title*="Next" i]',

        'button[title*="Next" i]',

        '[role="button"][title*="Next" i]',

        'a[aria-label*="Next" i]',

        'button[aria-label*="Next" i]',

        '[role="button"][aria-label*="Next" i]',
    ]


    for selector in selectors:

        locator = page.locator(
            selector
        )


        for index in range(
            locator.count()
        ):

            item = locator.nth(
                index
            )


            try:

                if item.is_visible():

                    return item


            except Exception:

                pass


    return None


# ============================================================
# CHECK DISABLED
# ============================================================

def is_disabled(control):

    try:

        return bool(
            control.evaluate(
                """
                el =>
                    el.disabled === true
                    ||
                    el.hasAttribute("disabled")
                    ||
                    (
                        el.getAttribute(
                            "aria-disabled"
                        )
                        || ""
                    ).toLowerCase()
                    === "true"
                    ||
                    el.classList.contains(
                        "disabled"
                    )
                    ||
                    !!el.closest(
                        '.disabled,'
                        '[aria-disabled="true"]'
                    )
                """
            )
        )


    except Exception:

        return False


# ============================================================
# MOVE TO NEXT PAGE
# ============================================================

def move_next(
    page,
    old_fingerprint,
    next_page_number,
):

    control = find_next_button(
        page
    )


    if control is None:

        return False


    if is_disabled(
        control
    ):

        return False


    try:

        control.click(
            timeout=15_000
        )


    except Exception:

        try:

            control.evaluate(
                "(el) => el.click()"
            )


        except Exception:

            return False


    deadline = (
        time.time()
        +
        25
    )


    while (
        time.time()
        <
        deadline
    ):

        page.wait_for_timeout(
            500
        )


        check_block(
            page
        )


        records = extract_current_page(
            page,
            next_page_number,
        )


        new_fingerprint = (
            fingerprint(
                records
            )
        )


        if (
            new_fingerprint
            and
            new_fingerprint
            != old_fingerprint
        ):

            return True


    return False


# ============================================================
# LAUNCH BROWSER
# ============================================================



# ============================================================
# RUN
# ============================================================

def run():

    existing = load_existing()


    # القديم السيئ سيتم تجاهله تلقائيًا.
    stored = list(
        existing
    )


    seen = {

        record_key(
            record
        )

        for record
        in stored

        if record_key(
            record
        )
    }


    print(
        f"[{SOURCE_NAME}] "
        f"Valid existing records: "
        f"{len(existing)}"
    )


    added = 0

    pages_checked = 0

    seen_pages = set()


    with sync_playwright() as playwright:

        def prepare_first_page(page):
            response = page.goto(START_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            if response is not None and response.status >= 400:
                raise RuntimeError(f"HTTP error: {response.status}")
            page.wait_for_timeout(5000)
            check_block(page)
            return wait_for_records(page, 1)

        browser, context, page, first_records = open_with_fallback(
            playwright, source=SOURCE_NAME, prepare=prepare_first_page,
            context_options={'locale': 'en-US', 'viewport': {'width': 1440, 'height': 1000}},
            timeout=PAGE_TIMEOUT_MS, preferred='chrome',
            launch_options={"headless": HEADLESS},
        )

        try:

            page_number = 1


            while (
                page_number
                <=
                MAX_PAGES
            ):

                print(
                    f"[{SOURCE_NAME}] "
                    f"Reading page "
                    f"{page_number}..."
                )


                records = (first_records if page_number == 1
                           else wait_for_records(page, page_number))


                current_fingerprint = (
                    fingerprint(
                        records
                    )
                )


                if not current_fingerprint:

                    raise RuntimeError(
                        "No valid Qatar Foundation "
                        "tenders found."
                    )


                if (
                    current_fingerprint
                    in seen_pages
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        "Repeated page detected."
                    )

                    break


                seen_pages.add(
                    current_fingerprint
                )


                pages_checked += 1

                new_on_page = 0


                print(
                    f"[{SOURCE_NAME}] "
                    f"Found {len(records)} "
                    f"real tender rows."
                )


                for record in records:

                    key = record_key(
                        record
                    )


                    if key in seen:
                        continue


                    seen.add(
                        key
                    )


                    stored.append(
                        record
                    )


                    added += 1

                    new_on_page += 1


                    print(
                        "  +",
                        record[
                            "Negotiation Number"
                        ],
                        "|",
                        record[
                            "Title"
                        ],
                    )


                print(
                    f"[{SOURCE_NAME}] "
                    f"New on page: "
                    f"{new_on_page}"
                )


                save_atomic(
                    stored
                )


                page.wait_for_timeout(
                    PAGE_DELAY_MS
                )


                moved = move_next(

                    page,

                    current_fingerprint,

                    page_number + 1,
                )


                if not moved:

                    print(
                        f"[{SOURCE_NAME}] "
                        "No additional page."
                    )

                    break


                page_number += 1


        finally:

            if context:

                try:

                    context.close()

                except Exception:

                    pass


            try:

                browser.close()

            except Exception:

                pass


    # ========================================================
    # FINAL SAVE
    #
    # حتى لو الملف القديم كان فيه السجلين
    # الخاطئين، الآن يتم استبداله بالقائمة النظيفة.
    # ========================================================

    save_atomic(
        stored
    )


    print(
        "\n"
        +
        "=" * 60
    )


    print(
        f"[{SOURCE_NAME}] SUCCESS"
    )


    print(
        f"Pages checked: "
        f"{pages_checked}"
    )


    print(
        f"Valid old records: "
        f"{len(existing)}"
    )


    print(
        f"New records: "
        f"{added}"
    )


    print(
        f"Total valid tenders: "
        f"{len(stored)}"
    )


    print(
        f"JSON: "
        f"{OUTPUT_FILE}"
    )


    print(
        "=" * 60
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        run()


    except KeyboardInterrupt:

        print(
            f"\n[{SOURCE_NAME}] "
            "Stopped by user."
        )

        sys.exit(
            130
        )


    except PlaywrightTimeoutError as error:

        print(
            f"\n[{SOURCE_NAME}] "
            "PLAYWRIGHT TIMEOUT\n"
            f"{error}"
        )

        sys.exit(
            1
        )


    except Exception as error:

        print(
            f"\n[{SOURCE_NAME}] "
            "FAILED\n"
            f"{type(error).__name__}: "
            f"{error}"
        )

        sys.exit(
            1
        )