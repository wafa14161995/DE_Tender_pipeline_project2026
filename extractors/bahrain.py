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
# SOURCE
# ============================================================

SOURCE_NAME = "bahrain_tender_board"

START_URL = (
    "https://www.tenderboard.gov.bh/"
    "Tenders/PublicTenders/"
)


# ============================================================
# PATHS
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

# يكتشف تلقائياً إذا كان الكود يشتغل على Databricks
# (متغيرات بيئة تكون موجودة فقط هناك) ويفعّل headless تلقائياً.
# على جهازك ما رح توجد هذي المتغيرات، فتبقى نافذة ظاهرة كالسابق.
_ON_DATABRICKS = bool(
    os.getenv("DATABRICKS_RUNTIME_VERSION")
    or os.getenv("DB_HOME")
)

HEADLESS = os.getenv("HEADLESS", "1" if _ON_DATABRICKS else "0") == "1"

# WebKit أولاً لأنه يشتغل مع الموقع
BROWSER_ORDER = [
    "WebKit",
    "Firefox",
    "Google Chrome",
    "Chromium",
]


# ============================================================
# SETTINGS
# ============================================================

PAGE_TIMEOUT_MS = 60_000

MAX_PAGES = 45

PAGE_DELAY_MS = 1_500

OPEN_RETRIES = 2


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
# SITE RESTRICTION MARKERS
# ============================================================

BLOCK_MARKERS = [
    "captcha",
    "verify you are human",
    "are you human",
    "access denied",
    "forbidden",
    "unusual traffic",
    "cloudflare",
]


# ============================================================
# CUSTOM EXCEPTIONS
# ============================================================

class SiteRestrictedError(RuntimeError):
    """
    الموقع نفسه رفض الوصول.
    في هذه الحالة لا نجرب متصفح ثاني.
    """
    pass


class BrowserAttemptError(RuntimeError):
    """
    مشكلة تقنية في المتصفح أو تحميل الصفحة.
    هنا نقدر نجرب المتصفح التالي.
    """
    pass


# ============================================================
# TEXT CLEANING
# ============================================================

def clean(value):

    return " ".join(
        str(value or "").split()
    )


# ============================================================
# LOAD EXISTING DATA
# ============================================================

def load_existing():
    # --- DEDUP DISABLED-- original logic preserved below as
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


        if isinstance(
            data,
            list
        ):

            return data


    except Exception as error:

        print(
            f"[{SOURCE_NAME}] "
            f"Warning: could not read old JSON: "
            f"{error}"
        )


    return []


# ============================================================
# SAFE SAVE
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
# UNIQUE RECORD KEY
# ============================================================

def record_key(record):

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
# DEBUG
# ============================================================

def save_debug(
    page,
    name,
):

    DEBUG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


    try:

        (
            DEBUG_DIR
            / f"{name}.html"
        ).write_text(
            page.content(),
            encoding="utf-8",
        )

    except Exception:

        pass


    try:

        page.screenshot(
            path=str(
                DEBUG_DIR
                / f"{name}.png"
            ),
            full_page=True,
        )

    except Exception:

        pass


# ============================================================
# CHECK PAGE FOR BLOCK / CAPTCHA
# ============================================================

def check_block(page):

    try:

        body = (
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

        if marker in body:

            save_debug(
                page,
                "bahrain_access_restricted",
            )


            raise SiteRestrictedError(
                "Bahrain website appears to "
                f"restrict access: {marker}"
            )


# ============================================================
# WATCH NETWORK RESPONSES  (FIXED)
# ============================================================
# نعتبر 403/429 رفضاً فقط إذا كان على الصفحة نفسها أو على طلب
# البيانات (document / xhr / fetch). ملفات JS و CSS والصور
# (مثل custom-isotope.js) لا توقف السكربت.

def attach_network_monitor(page, restriction_events):

    def handle_response(response):
        try:
            status = response.status
            rtype = response.request.resource_type

            if status in (403, 429) and rtype in ("document", "xhr", "fetch"):
                restriction_events.append(
                    {"status": status, "url": response.url}
                )
        except Exception:
            pass

    page.on("response", handle_response)


def check_network_restrictions(restriction_events):
    if not restriction_events:
        return

    last_event = restriction_events[-1]

    raise SiteRestrictedError(
        f"Bahrain server returned HTTP {last_event['status']} "
        f"for {last_event['url']}"
    )

# ============================================================
# CHECK NETWORK RESTRICTIONS
# ============================================================

def check_network_restrictions(
    restriction_events,
):

    if not restriction_events:

        return


    last_event = (
        restriction_events[
            -1
        ]
    )


    raise SiteRestrictedError(
        "Bahrain server returned "
        f"HTTP {last_event['status']} "
        f"for {last_event['url']}"
    )


# ============================================================
# FIND TENDER ROWS
# ============================================================

def discover_rows(page):

    rows = page.evaluate(
        r"""
        () => {

            const clean = value =>
                (value || '')
                .replace(/\s+/g, ' ')
                .trim();


            const dateRe =
                /\b\d{1,2}\s*,?\s*[A-Za-z]{3}\s*,?\s*\d{4}\b/g;


            const typeRe =
                /\b(Internal|External)\b/i;


            const isTenderRow = element => {

                const text =
                    clean(
                        element.innerText
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
                    )
                    || [];


                const detailLink =
                    element.querySelector(
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


            if (!candidates.length) {

                candidates =
                    Array.from(
                        document.querySelectorAll(
                            '#cphBaseBody_CphInnerBody_TenderDetailsBlock *'
                        )
                    )
                    .filter(
                        element => {

                            const children =
                                (
                                    element.children
                                    || []
                                ).length;


                            return (
                                children >= 4
                                &&
                                children <= 15
                                &&
                                isTenderRow(
                                    element
                                )
                            );
                        }
                    );
            }


            candidates =
                candidates.filter(
                    element =>
                        !Array.from(
                            element.children
                            || []
                        )
                        .some(
                            child =>
                                isTenderRow(
                                    child
                                )
                        )
                );


            return candidates.map(
                element => {

                    const link =
                        element.querySelector(
                            'a[href*="TenderDetails" i]'
                        );


                    return {

                        text:
                            clean(
                                element.innerText
                            ),

                        parts:
                            Array.from(
                                element.children
                                || []
                            )
                            .map(
                                child =>
                                    clean(
                                        child.innerText
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
                                "",

                        href:
                            link
                                ?
                                (
                                    link.getAttribute(
                                        "href"
                                    )
                                    || ""
                                )
                                :
                                ""
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
# WAIT FOR ROWS
# ============================================================

def wait_for_rows(
    page,
    restriction_events,
    seconds=35,
):

    deadline = (
        time.time()
        +
        seconds
    )


    while (
        time.time()
        <
        deadline
    ):

        check_network_restrictions(
            restriction_events
        )


        check_block(
            page
        )


        rows = discover_rows(
            page
        )


        if rows:

            return rows


        # Bahrain popup
        try:

            modal = page.locator(
                "#myModal"
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
                    "bahrain_error_popup",
                )


                check_network_restrictions(
                    restriction_events
                )


                raise BrowserAttemptError(
                    "Bahrain page displayed "
                    f"popup: {modal_text}"
                )


        except SiteRestrictedError:

            raise


        except BrowserAttemptError:

            raise


        except Exception:

            pass


        page.wait_for_timeout(
            500
        )


    save_debug(
        page,
        "bahrain_rows_timeout",
    )


    check_network_restrictions(
        restriction_events
    )


    raise BrowserAttemptError(
        "Tender rows did not load."
    )


# ============================================================
# TENDER NUMBER FROM URL
# ============================================================

def tender_number_from_url(
    detail_url,
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
# CLEAN SUBJECT
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
# PARSE ROW
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

            type_index = index


            tender_type = (
                match
                .group(
                    1
                )
                .title()
            )


            break


    dates = (
        DATE_RE
        .findall(
            full_text
        )
    )


    raw_subject = clean(
        row.get(
            "linkText"
        )
    )


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


            authority = value

            break


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
# EXTRACT CURRENT PAGE
# ============================================================

def extract_page(
    page,
    page_number,
    restriction_events,
):

    rows = wait_for_rows(
        page,
        restriction_events,
        seconds=35,
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
# PAGE FINGERPRINT
# ============================================================

def fingerprint(records):

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
# PAGE NUMBER CONTROL
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
# NEXT CONTROL
# ============================================================

def find_next(page):

    candidates = [

        page.locator(
            "ul.DoctorHolder a"
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

            item = locator.nth(
                index
            )


            try:

                text = clean(
                    item.inner_text()
                )


                if (
                    item.is_visible()
                    and
                    "next"
                    in text.lower()
                ):

                    return item


            except Exception:

                pass


    return None


# ============================================================
# DISABLED CONTROL?
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
# WAIT FOR PAGE CHANGE
# ============================================================

def wait_change(
    page,
    old_fingerprint,
    restriction_events,
    seconds=25,
):

    deadline = (
        time.time()
        +
        seconds
    )


    while (
        time.time()
        <
        deadline
    ):

        check_network_restrictions(
            restriction_events
        )


        check_block(
            page
        )


        try:

            rows = discover_rows(
                page
            )


            if rows:

                temporary = [

                    parse_row(
                        row,
                        0,
                    )

                    for row
                    in rows
                ]


                new_fingerprint = (
                    fingerprint(
                        temporary
                    )
                )


                if (
                    new_fingerprint
                    and
                    new_fingerprint
                    != old_fingerprint
                ):

                    return True


        except SiteRestrictedError:

            raise


        except Exception:

            pass


        page.wait_for_timeout(
            400
        )


    return False


# ============================================================
# MOVE NEXT PAGE
# ============================================================

def next_page(
    page,
    current_page,
    old_fingerprint,
    restriction_events,
):

    target_number = (
        current_page
        +
        1
    )


    target = find_page_number(
        page,
        target_number,
    )


    if target:

        try:

            target.click(
                timeout=15_000
            )


            if wait_change(
                page,
                old_fingerprint,
                restriction_events,
            ):

                return True


        except SiteRestrictedError:

            raise


        except Exception:

            pass


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
        restriction_events,
    ):

        return True


    save_debug(
        page,
        (
            "bahrain_pagination_"
            f"after_{current_page}"
        ),
    )


    raise BrowserAttemptError(
        "Could not move from "
        f"page {current_page} "
        f"to page {target_number}."
    )


# ============================================================
# BROWSER LAUNCHERS
# ============================================================
def launch_browser(playwright, browser_name):

    if browser_name == "Google Chrome":
        return playwright.chromium.launch(
            channel="chrome",
            headless=HEADLESS,
            args=["--start-minimized"],
        )

    if browser_name == "Chromium":
        return playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

    if browser_name == "Firefox":
        return playwright.firefox.launch(headless=HEADLESS)

    if browser_name == "WebKit":
        return playwright.webkit.launch(headless=HEADLESS)

    raise ValueError(f"Unknown browser: {browser_name}")


# ============================================================
# OPEN WEBSITE
# ============================================================

def open_site_with_browser(playwright, browser_name):

    print("\n" + "=" * 60)
    print(f"[{SOURCE_NAME}] Trying browser: {browser_name} (headless={HEADLESS})")
    print("=" * 60)

    try:
        browser = launch_browser(playwright, browser_name)

    except Exception as error:
        raise BrowserAttemptError(
            f"Could not launch {browser_name}: {error}"
        )

    context = None
    page = None

    try:
        context = browser.new_context(
            locale="en-US",
            viewport={"width": 1440, "height": 1100},
        )

        page = context.new_page()

        page.set_default_timeout(PAGE_TIMEOUT_MS)

        restriction_events = []

        attach_network_monitor(page, restriction_events)

        last_error = None

        for attempt in range(1, OPEN_RETRIES + 1):

            try:
                print(
                    f"[{SOURCE_NAME}] Opening website "
                    f"attempt {attempt}/{OPEN_RETRIES}..."
                )

                response = page.goto(
                    START_URL,
                    wait_until="domcontentloaded",
                    timeout=PAGE_TIMEOUT_MS,
                )

                if response and response.status in (403, 429):
                    raise SiteRestrictedError(
                        f"Bahrain main page returned HTTP {response.status}"
                    )

                if response and response.status >= 400:
                    raise BrowserAttemptError(
                        f"Bahrain main page returned HTTP {response.status}"
                    )

                check_network_restrictions(restriction_events)

                check_block(page)

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

                page.wait_for_timeout(4_000)

                check_network_restrictions(restriction_events)

                rows = discover_rows(page)

                if rows:
                    print(f"[{SOURCE_NAME}] Tender rows loaded automatically.")
                    return browser, context, page, restriction_events

                print(f"[{SOURCE_NAME}] Clicking Search...")

                search_button = page.locator(
                    'button[onclick*="fnGetCurrentPublicTender"]'
                ).first

                search_button.click(timeout=15_000)

                wait_for_rows(page, restriction_events, seconds=35)

                print(f"[{SOURCE_NAME}] {browser_name} loaded tender rows.")

                return browser, context, page, restriction_events

            except SiteRestrictedError:
                raise

            except Exception as error:
                last_error = error

                print(
                    f"[{SOURCE_NAME}] {browser_name} attempt failed: {error}"
                )

                if attempt < OPEN_RETRIES:
                    page.wait_for_timeout(3_000)

        raise BrowserAttemptError(
            f"{browser_name} failed after {OPEN_RETRIES} attempts: {last_error}"
        )

    except Exception:

        if context:
            try:
                context.close()
            except Exception:
                pass

        try:
            browser.close()
        except Exception:
            pass

        raise



# ============================================================
# GET FIRST WORKING BROWSER
# ============================================================
# إذا رفض الموقع محرك معيّن (403 على الصفحة أو طلب البيانات)
# ننتقل للمحرك التالي. إذا رفضته كل المحركات نتوقف ولا نحاول
# أي التفاف (لا تغيير user-agent ولا إخفاء headless).

def get_working_browser(playwright):

    restricted = []
    browser_errors = []

    for browser_name in BROWSER_ORDER:

        try:
            return open_site_with_browser(playwright, browser_name)

        except SiteRestrictedError as error:
            restricted.append((browser_name, str(error)))
            print(
                f"[{SOURCE_NAME}] {browser_name} was refused by the site. "
                "Moving to next engine."
            )

        except BrowserAttemptError as error:
            browser_errors.append((browser_name, str(error)))
            print(f"[{SOURCE_NAME}] Moving to next browser.")

    print("\n" + "=" * 60)
    print("BROWSER ATTEMPT SUMMARY")
    print("=" * 60)

    for browser_name, error in restricted:
        print(f"{browser_name} (restricted): {error}")

    for browser_name, error in browser_errors:
        print(f"{browser_name}: {error}")

    if restricted and not browser_errors:
        raise SiteRestrictedError(
            "All browser engines were refused by the Bahrain site."
        )

    raise RuntimeError(
        "No installed Playwright browser could load Bahrain tender rows."
    )


# ============================================================
# RUN
# ============================================================

def run():

    existing = load_existing()


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
                restriction_events,
            ) = get_working_browser(
                playwright
            )


            print(
                f"[{SOURCE_NAME}] "
                "Browser selected successfully."
            )


            page_number = 1


            while (
                page_number
                <=
                MAX_PAGES
            ):

                print(
                    f"\n[{SOURCE_NAME}] "
                    f"Reading page "
                    f"{page_number}..."
                )


                check_network_restrictions(
                    restriction_events
                )


                records = extract_page(
                    page,
                    page_number,
                    restriction_events,
                )


                current_fingerprint = (
                    fingerprint(
                        records
                    )
                )


                if not current_fingerprint:

                    raise RuntimeError(
                        "No valid Bahrain records "
                        f"on page "
                        f"{page_number}."
                    )


                if (
                    current_fingerprint
                    in seen_pages
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
                    f"Page "
                    f"{page_number}: "
                    f"rows="
                    f"{len(records)} | "
                    f"new="
                    f"{page_new} | "
                    f"duplicates="
                    f"{page_duplicates}"
                )


                if (
                    page_number
                    >=
                    MAX_PAGES
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        "Reached max pages: "
                        f"{MAX_PAGES}"
                    )

                    break


                print(
                    f"[{SOURCE_NAME}] "
                    "Waiting before "
                    "next page..."
                )


                page.wait_for_timeout(
                    PAGE_DELAY_MS
                )


                moved = next_page(
                    page,
                    page_number,
                    current_fingerprint,
                    restriction_events,
                )


                if not moved:

                    print(
                        f"[{SOURCE_NAME}] "
                        "Reached final page: "
                        f"{page_number}"
                    )

                    break


                page_number += 1


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


    except SiteRestrictedError as error:

        print(
            "\n"
            +
            "=" * 60
        )

        print(
            f"[{SOURCE_NAME}] "
            "ACCESS RESTRICTED"
        )

        print(
            str(error)
        )

        print(
            "The script will not rotate "
            "browsers after a confirmed "
            "403 / 429 / CAPTCHA."
        )

        print(
            "=" * 60
        )

        sys.exit(
            1
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