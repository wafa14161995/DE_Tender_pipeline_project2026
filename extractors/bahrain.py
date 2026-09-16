import json
import re
import sys
import time

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import (
    urljoin,
    urlparse,
    parse_qs,
)

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)


# ============================================================
# SOURCE SETTINGS
# ============================================================

SOURCE_NAME = "bahrain_tender_board"

START_URL = (
    "https://www.tenderboard.gov.bh/"
    "Tenders/PublicTenders/"
)


# ============================================================
# PROJECT PATHS
# ============================================================

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
    / "bahrain.json"
)

DEBUG_DIR = (
    PROJECT_ROOT
    / "results"
    / "debug"
)


# ============================================================
# SETTINGS
# ============================================================

PAGE_TIMEOUT_MS = 60_000

MAX_PAGES = 500


# ============================================================
# REGEX
# ============================================================

DATE_RE = re.compile(
    r"\b\d{1,2}\s*,?\s*"
    r"[A-Za-z]{3}\s*,?\s*"
    r"\d{4}\b"
)

TYPE_RE = re.compile(
    r"\b(Internal|External)\b",
    re.IGNORECASE,
)


# ============================================================
# BLOCK / CAPTCHA WORDS
# ============================================================

BLOCK_MARKERS = [
    "captcha",
    "verify you are human",
    "access denied",
    "forbidden",
    "unusual traffic",
    "cloudflare",
]


# ============================================================
# CLEAN TEXT
# ============================================================

def clean(value):

    return " ".join(
        str(value or "").split()
    )


# ============================================================
# LOAD EXISTING JSON
# ============================================================

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
            "bahrain.json must contain "
            "a JSON list."
        )


    return data


# ============================================================
# SAVE JSON SAFELY
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

    # Bahrain Detail URL is the most stable key.

    return clean(
        record.get(
            "Detail URL"
        )
        or
        record.get(
            "Tender Number"
        )
        or
        record.get(
            "No./Tender Subject"
        )
    )


# ============================================================
# SAVE DEBUG FILES
# ============================================================

def save_debug(
    page,
    name
):

    DEBUG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


    try:

        html_file = (
            DEBUG_DIR
            / f"{name}.html"
        )

        html_file.write_text(
            page.content(),
            encoding="utf-8",
        )

    except Exception:

        pass


    try:

        screenshot_file = (
            DEBUG_DIR
            / f"{name}.png"
        )

        page.screenshot(
            path=str(
                screenshot_file
            ),
            full_page=True,
        )

    except Exception:

        pass


# ============================================================
# CHECK BLOCK / CAPTCHA
# ============================================================

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

            save_debug(
                page,
                "bahrain_possible_block"
            )

            raise RuntimeError(
                "Possible block/CAPTCHA "
                f"detected: {marker}"
            )


# ============================================================
# DISCOVER TENDER ROWS
# ============================================================

def discover_rows(page):

    rows = page.evaluate(
        r"""
        () => {

            const clean = s =>
                (s || '')
                .replace(/\s+/g, ' ')
                .trim();


            const dateRe =
                /\b\d{1,2}\s*,?\s*[A-Za-z]{3}\s*,?\s*\d{4}\b/g;


            const typeRe =
                /\b(Internal|External)\b/i;


            const isTenderRow = el => {

                const text =
                    clean(
                        el.innerText
                    );


                if (!text)
                    return false;


                if (
                    text.length > 3500
                )
                    return false;


                const dates =
                    text.match(
                        dateRe
                    ) || [];


                const detailLink =
                    el.querySelector(
                        'a[href*="TenderDetails" i]'
                    );


                return (
                    dates.length >= 3
                    &&
                    typeRe.test(text)
                    &&
                    !!detailLink
                );
            };


            let candidates =
                Array.from(
                    document.querySelectorAll(
                        '#cphBaseBody_CphInnerBody_TenderDetailsBlock tr, ' +
                        '#cphBaseBody_CphInnerBody_TenderDetailsBlock .row, ' +
                        '#cphBaseBody_CphInnerBody_TenderDetailsBlock .card, ' +
                        '#cphBaseBody_CphInnerBody_TenderDetailsBlock [class*="tender" i]'
                    )
                )
                .filter(
                    isTenderRow
                );


            // إذا structure الموقع تغير شوي،
            // نجرب كل العناصر داخل tender block.

            if (!candidates.length) {

                candidates =
                    Array
                    .from(
                        document.querySelectorAll(
                            '#cphBaseBody_CphInnerBody_TenderDetailsBlock *'
                        )
                    )
                    .filter(
                        el => {

                            const children =
                                (
                                    el.children
                                    || []
                                )
                                .length;


                            return (
                                children >= 4
                                &&
                                children <= 15
                                &&
                                isTenderRow(el)
                            );
                        }
                    );
            }


            // نشيل parent elements
            // لو كان داخلها نفس tender row.

            candidates =
                candidates.filter(
                    el =>
                        !Array
                        .from(
                            el.children
                            || []
                        )
                        .some(
                            child =>
                                isTenderRow(child)
                        )
                );


            return candidates.map(
                el => {

                    const link =
                        el.querySelector(
                            'a[href*="TenderDetails" i]'
                        );


                    return {

                        text:
                            clean(
                                el.innerText
                            ),

                        parts:
                            Array
                            .from(
                                el.children
                                || []
                            )
                            .map(
                                x =>
                                    clean(
                                        x.innerText
                                    )
                            )
                            .filter(
                                Boolean
                            ),

                        linkText:
                            link
                                ?
                                clean(
                                    link.innerText
                                )
                                :
                                '',

                        href:
                            link
                                ?
                                (
                                    link.getAttribute(
                                        'href'
                                    )
                                    || ''
                                )
                                :
                                ''
                    };
                }
            );
        }
        """
    )


    unique = []

    seen = set()


    for row in rows:

        signature = clean(
            row.get(
                "href"
            )
            or
            row.get(
                "text"
            )
        )


        if (
            signature
            and
            signature not in seen
        ):

            seen.add(
                signature
            )

            unique.append(
                row
            )


    return unique


# ============================================================
# WAIT FOR TENDER ROWS
# ============================================================

def wait_for_rows(
    page,
    seconds=30,
):

    end_time = (
        time.time()
        +
        seconds
    )


    while (
        time.time()
        <
        end_time
    ):

        check_block(
            page
        )


        rows = discover_rows(
            page
        )


        if rows:

            return rows


        # Check whether Bahrain showed its error modal.

        try:

            modal = (
                page.locator(
                    "#myModal"
                )
            )


            if (
                modal.count()
                and
                modal.is_visible()
            ):

                modal_text = clean(
                    modal.inner_text()
                )


                save_debug(
                    page,
                    "bahrain_modal_error"
                )


                raise RuntimeError(
                    "Bahrain website displayed "
                    f"an error popup: {modal_text}"
                )

        except RuntimeError:

            raise

        except Exception:

            pass


        page.wait_for_timeout(
            500
        )


    save_debug(
        page,
        "bahrain_rows_not_found"
    )


    raise RuntimeError(
        "No Bahrain tender rows "
        "loaded after waiting."
    )


# ============================================================
# TENDER NUMBER FROM DETAIL URL
# ============================================================

def tender_number_from_url(
    detail_url
):

    if not detail_url:

        return ""


    try:

        query = parse_qs(
            urlparse(
                detail_url
            ).query
        )


        id_value = clean(
            query.get(
                "id",
                [""],
            )[0]
        )


        match = re.search(
            r"\((.*)\)\s*$",
            id_value,
        )


        if match:

            return clean(
                match.group(
                    1
                )
            )


    except Exception:

        pass


    return ""


# ============================================================
# REMOVE TENDER NUMBER FROM SUBJECT
# ============================================================

def remove_number_from_subject(
    raw_subject,
    tender_number,
):

    raw_subject = clean(
        raw_subject
    )


    tender_number = clean(
        tender_number
    )


    if not tender_number:

        return raw_subject


    if raw_subject.startswith(
        tender_number
    ):

        return clean(
            raw_subject[
                len(
                    tender_number
                ):
            ]
        )


    return raw_subject


# ============================================================
# PARSE ONE ROW
# ============================================================

def parse_row(
    row,
    page_number,
):

    parts = [

        clean(
            value
        )

        for value
        in row.get(
            "parts",
            [],
        )

        if clean(
            value
        )
    ]


    full_text = clean(
        row.get(
            "text"
        )
    )


    # --------------------------------------------------------
    # ROW NUMBER
    # --------------------------------------------------------

    row_number = ""


    if (
        parts
        and
        parts[0].isdigit()
    ):

        row_number = (
            parts.pop(
                0
            )
        )


    # --------------------------------------------------------
    # TENDER TYPE
    # --------------------------------------------------------

    tender_type = ""

    type_index = None


    for (
        index,
        part
    ) in enumerate(
        parts
    ):

        match = (
            TYPE_RE
            .fullmatch(
                part
            )
        )


        if match:

            type_index = (
                index
            )

            tender_type = (
                match
                .group(
                    1
                )
                .title()
            )

            break


    # --------------------------------------------------------
    # DATES
    # --------------------------------------------------------

    dates = (
        DATE_RE
        .findall(
            full_text
        )
    )


    # --------------------------------------------------------
    # SUBJECT
    # --------------------------------------------------------

    raw_subject = clean(
        row.get(
            "linkText"
        )
    )


    # --------------------------------------------------------
    # PURCHASING AUTHORITY
    # --------------------------------------------------------

    authority = ""


    if (
        type_index
        is not None
    ):

        before_type = [

            value

            for value
            in parts[
                :type_index
            ]

            if not DATE_RE.search(
                value
            )
        ]


        after_type = (

            parts[
                type_index + 1:
            ]
        )


        if (
            before_type
            and
            not raw_subject
        ):

            raw_subject = (
                before_type[
                    -1
                ]
            )


        for value in after_type:

            if DATE_RE.search(
                value
            ):

                continue


            if TYPE_RE.fullmatch(
                value
            ):

                continue


            authority = (
                value
            )

            break


    # --------------------------------------------------------
    # DETAIL URL
    # --------------------------------------------------------

    href = clean(
        row.get(
            "href"
        )
    )


    detail_url = (

        urljoin(
            START_URL,
            href,
        )

        if href

        else ""
    )


    # --------------------------------------------------------
    # TENDER NUMBER
    # --------------------------------------------------------

    tender_number = (
        tender_number_from_url(
            detail_url
        )
    )


    tender_subject = (
        remove_number_from_subject(
            raw_subject,
            tender_number,
        )
    )


    # --------------------------------------------------------
    # FINAL RECORD
    # --------------------------------------------------------

    return {

        "No.":
            row_number,

        "Tender Number":
            tender_number,

        "Tender Subject":
            tender_subject,

        "No./Tender Subject":
            raw_subject,

        "Tender Type":
            tender_type,

        "Purchasing Authority":
            authority,

        "Published Date":
            (
                clean(
                    dates[0]
                )
                if len(
                    dates
                ) > 0
                else ""
            ),

        "Purchase Before":
            (
                clean(
                    dates[1]
                )
                if len(
                    dates
                ) > 1
                else ""
            ),

        "Closing Date":
            (
                clean(
                    dates[2]
                )
                if len(
                    dates
                ) > 2
                else ""
            ),

        "Detail URL":
            detail_url,

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


# ============================================================
# GET PAGE RECORDS
# ============================================================

def extract_page(
    page,
    page_number,
):

    rows = wait_for_rows(
        page,
        seconds=30,
    )


    return [

        parse_row(
            row,
            page_number,
        )

        for row
        in rows
    ]


# ============================================================
# FINGERPRINT
# ============================================================

def fingerprint(
    records
):

    return tuple(

        record_key(
            record
        )

        for record
        in records[
            :5
        ]

        if record_key(
            record
        )
    )


# ============================================================
# FIND PAGE NUMBER BUTTON
# ============================================================

def find_page_number(
    page,
    number,
):

    exact = re.compile(
        rf"^\s*{number}\s*$"
    )


    candidates = [

        page.get_by_role(
            "link",
            name=exact,
        ),

        page.get_by_role(
            "button",
            name=exact,
        ),
    ]


    for locator in candidates:

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
# FIND NEXT BUTTON
# ============================================================

def find_next(page):

    candidates = [

        page.locator(
            "ul.DoctorHolder "
            "a"
        ),

        page.locator(
            'a[rel="next"]'
        ),

        page.locator(
            'a[aria-label*="Next" i]'
        ),

        page.locator(
            'button[aria-label*="Next" i]'
        ),
    ]


    for locator in candidates:

        for index in range(
            locator.count()
        ):

            item = (
                locator.nth(
                    index
                )
            )


            try:

                text = clean(
                    item.inner_text()
                )


                if (
                    item.is_visible()
                    and
                    (
                        text.lower()
                        ==
                        "next"
                        or
                        "next"
                        in text.lower()
                    )
                ):

                    return item

            except Exception:

                pass


    return None


# ============================================================
# DISABLED?
# ============================================================

def is_disabled(element):

    try:

        return bool(
            element.evaluate(
                """
                el =>
                    el.disabled === true
                    ||
                    el.hasAttribute(
                        'disabled'
                    )
                    ||
                    (
                        el.getAttribute(
                            'aria-disabled'
                        )
                        || ''
                    ).toLowerCase()
                    === 'true'
                    ||
                    el.classList.contains(
                        'disabled'
                    )
                    ||
                    !!el.closest(
                        '.disabled, '
                        '[aria-disabled="true"]'
                    )
                """
            )
        )

    except Exception:

        return False


# ============================================================
# WAIT UNTIL PAGE CHANGES
# ============================================================

def wait_change(
    page,
    old_fingerprint,
    seconds=20,
):

    end_time = (
        time.time()
        +
        seconds
    )


    while (
        time.time()
        <
        end_time
    ):

        try:

            rows = discover_rows(
                page
            )


            if rows:

                new_records = [

                    parse_row(
                        row,
                        0,
                    )

                    for row
                    in rows
                ]


                new_fingerprint = (
                    fingerprint(
                        new_records
                    )
                )


                if (
                    new_fingerprint
                    and
                    new_fingerprint
                    != old_fingerprint
                ):

                    return True


        except Exception:

            pass


        page.wait_for_timeout(
            350
        )


    return False


# ============================================================
# MOVE TO NEXT PAGE
# ============================================================

def next_page(
    page,
    current_page,
    old_fingerprint,
):

    target_number = (
        current_page
        +
        1
    )


    # --------------------------------------------------------
    # Try exact next page number
    # --------------------------------------------------------

    target = (
        find_page_number(
            page,
            target_number,
        )
    )


    if target:

        target.click(
            timeout=15_000
        )


        if wait_change(
            page,
            old_fingerprint,
        ):

            return True


    # --------------------------------------------------------
    # Try Next
    # --------------------------------------------------------

    next_control = (
        find_next(
            page
        )
    )


    if (
        not next_control
        or
        is_disabled(
            next_control
        )
    ):

        return False


    next_control.click(
        timeout=15_000
    )


    if wait_change(
        page,
        old_fingerprint,
    ):

        return True


    save_debug(
        page,
        (
            "bahrain_pagination_"
            f"after_{current_page}"
        ),
    )


    raise RuntimeError(
        "Could not move from "
        f"Bahrain page "
        f"{current_page} "
        f"to {target_number}."
    )


# ============================================================
# OPEN WEBSITE AND LOAD RESULTS
# ============================================================

def open_and_load_results(
    playwright,
):

    # --------------------------------------------------------
    # Try installed Google Chrome first.
    #
    # We are NOT manually calling Bahrain's internal API.
    # Chrome runs the public webpage's own JavaScript.
    # --------------------------------------------------------

    try:

        browser = (
            playwright
            .chromium
            .launch(
                channel="chrome",

                # نفتح المتصفح قدامك
                # في آخر تجربة عشان نشوف
                # هل الموقع يتعامل معه طبيعي.
                headless=False,
            )
        )


        print(
            f"[{SOURCE_NAME}] "
            "Using installed Google Chrome."
        )


    except Exception:

        browser = (
            playwright
            .chromium
            .launch(
                headless=False,
            )
        )


        print(
            f"[{SOURCE_NAME}] "
            "Google Chrome channel was unavailable; "
            "using Playwright Chromium."
        )


    context = (
        browser
        .new_context(
            locale="en-US",

            viewport={
                "width": 1440,
                "height": 1100,
            },
        )
    )


    page = (
        context
        .new_page()
    )


    page.set_default_timeout(
        PAGE_TIMEOUT_MS
    )


    print(
        f"[{SOURCE_NAME}] "
        "Opening website..."
    )


    response = (
        page.goto(
            START_URL,

            wait_until=
                "domcontentloaded",

            timeout=
                PAGE_TIMEOUT_MS,
        )
    )


    if (
        response
        and
        response.status
        >= 400
    ):

        raise RuntimeError(
            "Bahrain main page returned "
            f"HTTP {response.status}."
        )


    check_block(
        page
    )


    # --------------------------------------------------------
    # Wait until public page elements exist
    # --------------------------------------------------------

    page.wait_for_selector(
        "#cphBaseBody_CphInnerBody_TenderDetailsBlock",
        state="attached",
        timeout=PAGE_TIMEOUT_MS,
    )


    page.wait_for_selector(
        'button[onclick*="fnGetCurrentPublicTender"]',
        state="attached",
        timeout=PAGE_TIMEOUT_MS,
    )


    # خلي الصفحة تكمل تحميل JS
    page.wait_for_timeout(
        4_000
    )


    # --------------------------------------------------------
    # Maybe results loaded automatically.
    # --------------------------------------------------------

    rows = discover_rows(
        page
    )


    if rows:

        print(
            f"[{SOURCE_NAME}] "
            "Tender rows loaded automatically."
        )

        return (
            browser,
            context,
            page,
        )


    # --------------------------------------------------------
    # Otherwise click Search like a normal user.
    # --------------------------------------------------------

    print(
        f"[{SOURCE_NAME}] "
        "Clicking Search..."
    )


    search_button = (
        page.locator(
            'button[onclick*="fnGetCurrentPublicTender"]'
        )
        .first
    )


    search_button.click(
        timeout=15_000
    )


    # --------------------------------------------------------
    # Wait for site itself to load results.
    # --------------------------------------------------------

    wait_for_rows(
        page,
        seconds=35,
    )


    return (
        browser,
        context,
        page,
    )


# ============================================================
# MAIN
# ============================================================

def run():

    existing = (
        load_existing()
    )


    seen = {

        record_key(
            record
        )

        for record
        in existing

        if record_key(
            record
        )
    }


    stored = list(
        existing
    )


    seen_pages = set()

    added = 0

    page_number = 0


    print(
        f"[{SOURCE_NAME}] "
        f"Existing records: "
        f"{len(existing)}"
    )


    with sync_playwright() as playwright:

        browser = None
        context = None
        page = None


        try:

            (
                browser,
                context,
                page,
            ) = open_and_load_results(
                playwright
            )


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


                records = (
                    extract_page(
                        page,
                        page_number,
                    )
                )


                current_fingerprint = (
                    fingerprint(
                        records
                    )
                )


                if not current_fingerprint:

                    raise RuntimeError(
                        "No valid Bahrain records "
                        f"on page {page_number}."
                    )


                if (
                    current_fingerprint
                    in
                    seen_pages
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        "Repeated page detected; "
                        "stopping safely."
                    )

                    break


                seen_pages.add(
                    current_fingerprint
                )


                page_new = 0

                page_duplicates = 0


                for record in records:

                    record_id = (
                        record_key(
                            record
                        )
                    )


                    if not record_id:

                        continue


                    if record_id in seen:

                        page_duplicates += 1

                        continue


                    seen.add(
                        record_id
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


                # ------------------------------------------------
                # NEXT PAGE
                # ------------------------------------------------

                if not next_page(
                    page,
                    page_number,
                    current_fingerprint,
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        "Reached final page: "
                        f"{page_number}"
                    )

                    break


                page_number += 1


                # small delay
                page.wait_for_timeout(
                    500
                )


            else:

                raise RuntimeError(
                    "Bahrain pagination "
                    "safety limit reached."
                )


        finally:

            if context:

                try:

                    context.close()

                except Exception:

                    pass


            if browser:

                try:

                    browser.close()

                except Exception:

                    pass


    # ========================================================
    # SUMMARY
    # ========================================================

    print(
        "\n"
        +
        "=" * 60
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