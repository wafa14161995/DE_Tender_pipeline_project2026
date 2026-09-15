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


SOURCE_NAME = "bahrain_tender_board"

START_URL = (
    "https://www.tenderboard.gov.bh/"
    "Tenders/PublicTenders/"
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
    / "bahrain.json"
)

DEBUG_DIR = (
    PROJECT_ROOT
    / "results"
    / "debug"
)

PAGE_TIMEOUT_MS = 60_000
MAX_PAGES = 500


DATE_RE = re.compile(
    r"\b\d{1,2}\s*,?\s*"
    r"[A-Za-z]{3}\s*,?\s*"
    r"\d{4}\b"
)

TYPE_RE = re.compile(
    r"\b(Internal|External)\b",
    re.IGNORECASE
)


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
            "a JSON list"
        )

    return data


def save_atomic(records):

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
            "Tender Number"
        )
        or
        record.get(
            "Detail URL"
        )
        or
        record.get(
            "No./Tender Subject"
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


          const isRow = el => {

            const text =
              clean(el.innerText);

            if (!text)
              return false;

            if (text.length > 3500)
              return false;

            const dates =
              text.match(dateRe) || [];

            return (
              dates.length >= 3
              &&
              typeRe.test(text)
            );
          };


          let candidates =
            Array.from(
              document.querySelectorAll(
                'tr, .row, .card, [class*="tender" i]'
              )
            )
            .filter(isRow);


          candidates =
            candidates.filter(
              el =>
                !Array
                .from(el.children || [])
                .some(isRow)
            );


          if (!candidates.length) {

            candidates =
              Array
              .from(
                document.querySelectorAll(
                  'body *'
                )
              )
              .filter(el => {

                const children =
                  (el.children || []).length;

                return (
                  children >= 4
                  &&
                  children <= 12
                  &&
                  isRow(el)
                );
              })
              .filter(
                el =>
                  !Array
                  .from(el.children || [])
                  .some(isRow)
              );
          }


          return candidates.map(
            el => {

              const link =
                el.querySelector(
                  'a[href*="TenderDetails" i]'
                );

              return {

                text:
                  clean(el.innerText),

                parts:
                  Array
                  .from(el.children || [])
                  .map(
                    x =>
                      clean(x.innerText)
                  )
                  .filter(Boolean),

                linkText:
                  link
                    ? clean(link.innerText)
                    : '',

                href:
                  link
                    ? (
                        link.getAttribute(
                          'href'
                        )
                        || ''
                      )
                    : ''
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
            row.get("href")
            or
            row.get("text")
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


def wait_rows(page):

    end_time = (
        time.time()
        +
        PAGE_TIMEOUT_MS / 1000
    )

    while time.time() < end_time:

        check_block(page)

        rows = discover_rows(
            page
        )

        if rows:
            return rows

        page.wait_for_timeout(
            500
        )

    save_debug(
        page,
        "bahrain_rows_not_found"
    )

    raise RuntimeError(
        "No Bahrain tender rows found."
    )


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
                [""]
            )[0]
        )

        match = re.search(
            r"\((.*)\)\s*$",
            id_value
        )

        if match:

            return clean(
                match.group(1)
            )

    except Exception:
        pass

    return ""


def remove_number_from_subject(
    raw_subject,
    tender_number
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
                len(tender_number):
            ]
        )

    return raw_subject


def parse_row(
    row,
    page_number
):

    parts = [
        clean(x)

        for x
        in row.get(
            "parts",
            []
        )

        if clean(x)
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
            parts.pop(0)
        )

    tender_type = ""
    type_index = None

    for index, part in enumerate(
        parts
    ):

        match = TYPE_RE.fullmatch(
            part
        )

        if match:

            type_index = index

            tender_type = (
                match
                .group(1)
                .title()
            )

            break

    dates = DATE_RE.findall(
        full_text
    )

    raw_subject = clean(
        row.get(
            "linkText"
        )
    )

    authority = ""

    if type_index is not None:

        before_type = [
            value

            for value
            in parts[:type_index]

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
                before_type[-1]
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
            href
        )
        if href
        else ""
    )

    # الرقم الحقيقي نأخذه من الرابط.
    tender_number = (
        tender_number_from_url(
            detail_url
        )
    )

    tender_subject = (
        remove_number_from_subject(
            raw_subject,
            tender_number
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
            clean(
                dates[0]
            )
            if len(dates) > 0
            else "",

        "Purchase Before":
            clean(
                dates[1]
            )
            if len(dates) > 1
            else "",

        "Closing Date":
            clean(
                dates[2]
            )
            if len(dates) > 2
            else "",

        "Detail URL":
            detail_url,

        "_source":
            SOURCE_NAME,

        "_page":
            page_number,

        "_extracted_at":
            datetime
            .now(timezone.utc)
            .isoformat(),
    }


def extract_page(
    page,
    page_number
):

    rows = wait_rows(
        page
    )

    return [
        parse_row(
            row,
            page_number
        )

        for row
        in rows
    ]


def fingerprint(records):

    return tuple(
        record_key(record)

        for record
        in records[:5]

        if record_key(
            record
        )
    )


def in_pager(element):

    try:

        return bool(
            element.evaluate(
                r"""
                el => {

                  let node = el;

                  for (
                    let i = 0;
                    i < 7 && node;
                    i++,
                    node = node.parentElement
                  ) {

                    const cls =
                      String(
                        node.className || ''
                      )
                      .toLowerCase();

                    const text =
                      (
                        node.innerText || ''
                      )
                      .replace(
                        /\s+/g,
                        ' '
                      )
                      .trim();

                    if (
                      cls.includes(
                        'pagination'
                      )
                      ||
                      cls.includes(
                        'pager'
                      )
                      ||
                      /\bNext\b/i.test(text)
                      ||
                      /\bLast\b/i.test(text)
                    ) {
                      return true;
                    }
                  }

                  return false;
                }
                """
            )
        )

    except Exception:
        return False


def find_number(
    page,
    number
):

    exact = re.compile(
        rf"^\s*{number}\s*$"
    )

    candidates = [
        page.get_by_role(
            "link",
            name=exact
        ),

        page.get_by_role(
            "button",
            name=exact
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

                if (
                    item.is_visible()
                    and
                    in_pager(item)
                ):

                    return item

            except Exception:
                pass

    return None


def find_next(page):

    exact = re.compile(
        r"^\s*Next\s*$",
        re.IGNORECASE
    )

    candidates = [
        page.get_by_role(
            "link",
            name=exact
        ),

        page.get_by_role(
            "button",
            name=exact
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

            item = locator.nth(
                index
            )

            try:

                if (
                    item.is_visible()
                    and
                    in_pager(item)
                ):

                    return item

            except Exception:
                pass

    return None


def disabled(element):

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
                    )
                    .toLowerCase()
                    === 'true'
                    ||
                    el.classList
                    .contains(
                        'disabled'
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


def wait_change(
    page,
    old_fingerprint,
    seconds=20
):

    end_time = (
        time.time()
        +
        seconds
    )

    while time.time() < end_time:

        try:

            new_records = (
                extract_page(
                    page,
                    0
                )
            )

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


def next_page(
    page,
    current_page,
    old_fingerprint
):

    target_number = (
        current_page + 1
    )

    target = find_number(
        page,
        target_number
    )

    if target:

        target.click(
            timeout=15_000
        )

        if wait_change(
            page,
            old_fingerprint
        ):

            return True

        raise RuntimeError(
            f"Clicked Bahrain page "
            f"{target_number}, "
            f"but rows did not change."
        )

    next_control = find_next(
        page
    )

    if (
        not next_control
        or
        disabled(
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
        seconds=6
    ):

        return True

    page.wait_for_timeout(
        800
    )

    target = find_number(
        page,
        target_number
    )

    if target:

        target.click(
            timeout=15_000
        )

        if wait_change(
            page,
            old_fingerprint
        ):

            return True

    save_debug(
        page,
        (
            f"bahrain_pagination_"
            f"after_{current_page}"
        )
    )

    raise RuntimeError(
        f"Could not move from "
        f"Bahrain page "
        f"{current_page} "
        f"to {target_number}."
    )


def run():

    existing = (
        load_existing()
    )

    seen = {
        record_key(record)

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

                user_agent=(
                    "Mozilla/5.0 "
                    "(Windows NT 10.0; "
                    "Win64; x64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) "
                    "Chrome/131.0 "
                    "Safari/537.36"
                ),
            )
        )

        page = (
            context.new_page()
        )

        page.set_default_timeout(
            PAGE_TIMEOUT_MS
        )

        try:

            last_error = None

            for attempt in range(
                1,
                4
            ):

                try:

                    print(
                        f"[{SOURCE_NAME}] "
                        f"Opening website "
                        f"attempt "
                        f"{attempt}/3..."
                    )

                    response = page.goto(
                        START_URL,
                        wait_until=
                            "domcontentloaded",
                        timeout=
                            PAGE_TIMEOUT_MS
                    )

                    if (
                        response
                        and
                        response.status
                        >= 400
                    ):

                        raise RuntimeError(
                            f"HTTP "
                            f"{response.status}"
                        )

                    wait_rows(
                        page
                    )

                    last_error = None

                    break

                except Exception as error:

                    last_error = (
                        error
                    )

                    if attempt < 3:

                        page.wait_for_timeout(
                            4_000
                        )

            if last_error:

                raise RuntimeError(
                    "Could not open "
                    "Bahrain Tender Board: "
                    f"{last_error}"
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

                current_fingerprint = (
                    fingerprint(
                        records
                    )
                )

                if not current_fingerprint:

                    raise RuntimeError(
                        f"No valid Bahrain "
                        f"records on page "
                        f"{page_number}."
                    )

                if (
                    current_fingerprint
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

                if not next_page(
                    page,
                    page_number,
                    current_fingerprint
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        f"Reached final "
                        f"page: "
                        f"{page_number}"
                    )

                    break

                page_number += 1

            else:

                raise RuntimeError(
                    "Bahrain pagination "
                    "safety limit reached."
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

    except PlaywrightTimeoutError as error:

        print(
            f"\n[{SOURCE_NAME}] "
            f"PLAYWRIGHT TIMEOUT\n"
            f"{error}"
        )

        sys.exit(1)

    except Exception as error:

        print(
            f"\n[{SOURCE_NAME}] "
            f"FAILED\n"
            f"{type(error).__name__}: "
            f"{error}"
        )

        sys.exit(1)