if __package__:
    from ._browser_fallback import open_with_fallback
else:
    from _browser_fallback import open_with_fallback

import json
import sys

from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)


SOURCE_NAME = "qatar_monaqasat"
SOURCE_CURRENCY = "QAR"

BASE_URL = (
    "https://monaqasat.mof.gov.qa/"
    "TendersOnlineServices/"
    "AvailableMinistriesTenders/"
)


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "results"
    / "raw"
)

OUTPUT_FILE = (
    OUTPUT_DIR
    / "qatar_monaqasat.json"
)


PAGE_TIMEOUT_MS = 60_000

PAGE_RETRIES = 2

MAX_PAGES_SAFETY = 500


FIELDS = {

    "تاريخ الطرح":
        "تاريخ الطرح",

    "نوع القطاع المطلوب":
        "نوع القطاع المطلوب",

    "التأمين المؤقت (رق)":
        "التأمين المؤقت (رق)",

    "قيمة الوثائق (رق)":
        "قيمة الوثائق (رق)",

    "الجهة":
        "الجهة",

    "النوع":
        "النوع",
}


BLOCK_MARKERS = [
    "captcha",
    "verify you are human",
    "access denied",
    "forbidden",
    "unusual traffic",
    "cloudflare",
]


def clean_text(value):

    if value is None:
        return ""

    return " ".join(
        str(value).split()
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
            "Could not read existing "
            "Qatar monaqasat JSON: "
            f"{exc}"
        ) from exc

    if not isinstance(
        data,
        list
    ):

        raise RuntimeError(
            "Existing Qatar monaqasat"
            "JSON must contain a list."
        )

    return data


def save_json_atomic(records):

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = (
        OUTPUT_FILE
        .with_suffix(".json.tmp")
    )

    temporary.write_text(
        json.dumps(
            records,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8",
    )

    temporary.replace(
        OUTPUT_FILE
    )


def record_key(record):

    return clean_text(
        record.get(
            "رقم المناقصة",
            ""
        )
    )


def check_for_block(page):

    try:

        text = (
            page
            .locator("body")
            .inner_text(
                timeout=10_000
            )
            .lower()
        )

    except Exception:
        return

    for marker in BLOCK_MARKERS:

        if marker in text:

            raise RuntimeError(
                "Possible block/"
                "CAPTCHA detected: "
                f"{marker}"
            )


def read_card(
    card,
    page_number,
    page_url
):

    record = {

        "رقم المناقصة":
            "",

        "موضوع المناقصة":
            "",

        "تاريخ الطرح":
            "",

        "نوع القطاع المطلوب":
            "",

        "التأمين المؤقت (رق)":
            "",

        "قيمة الوثائق (رق)":
            "",

        "الجهة":
            "",

        "النوع":
            "",

        "تاريخ الإغلاق":
            "",

        "_currency":
            SOURCE_CURRENCY,

        "_source":
            SOURCE_NAME,

        "_page":
            page_number,

        "_page_url":
            page_url,

        "_extracted_at":
            datetime
            .now(timezone.utc)
            .isoformat(),
    }

    labels = card.select(
        "span.card-label"
    )

    titles = card.select(
        "span.card-title"
    )

    for (
        label,
        title
    ) in zip(
        labels,
        titles
    ):

        label_name = clean_text(
            label.get_text(
                " ",
                strip=True
            )
        )

        value = clean_text(
            title.get_text(
                " ",
                strip=True
            )
        )

        if label_name in FIELDS:

            record[
                FIELDS[label_name]
            ] = value

    header = (
        card.select_one(
            "div.col-md-7 "
            "div.col-header"
        )
    )

    if header:

        number = (
            header.select_one(
                "span.card-label"
            )
        )

        subject = (
            header.select_one(
                "span.card-title"
            )
        )

        if number:

            record[
                "رقم المناقصة"
            ] = clean_text(
                number.get_text(
                    " ",
                    strip=True
                )
            )

        if subject:

            record[
                "موضوع المناقصة"
            ] = clean_text(
                subject.get_text(
                    " ",
                    strip=True
                )
            )

    circle = (
        card.select_one(
            "div.circle-container "
            "span.card-label"
        )
    )

    if circle:

        spans = circle.select(
            "span"
        )

        if spans:

            record[
                "تاريخ الإغلاق"
            ] = clean_text(
                spans[-1]
                .get_text(
                    " ",
                    strip=True
                )
            )

    return record


def extract_page(
    page,
    page_number
):

    url = (
        BASE_URL
        +
        str(page_number)
    )

    last_error = None

    for attempt in range(
        1,
        PAGE_RETRIES + 1
    ):

        try:

            print(
                f"[{SOURCE_NAME}] "
                "Opening page "
                f"{page_number} "
                f"attempt "
                f"{attempt}/"
                f"{PAGE_RETRIES}..."
            )

            response = page.goto(
                url,
                wait_until=
                    "domcontentloaded",
                timeout=
                    PAGE_TIMEOUT_MS
            )

            if (
                response is not None
                and
                response.status >= 400
            ):

                raise RuntimeError(
                    "HTTP error: "
                    f"{response.status}"
                )

            check_for_block(
                page
            )

            try:

                page.wait_for_selector(
                    "div.row.custom-cards",
                    timeout=15_000
                )

            except PlaywrightTimeoutError:

                return []

            soup = BeautifulSoup(
                page.content(),
                "html.parser"
            )

            cards = soup.select(
                "div.row.custom-cards"
            )

            return [

                read_card(
                    card,
                    page_number,
                    url
                )

                for card
                in cards
            ]

        except Exception as exc:

            last_error = exc

            print(
                f"[{SOURCE_NAME}] "
                f"Page {page_number} "
                f"failed: {exc}"
            )

            if (
                attempt
                < PAGE_RETRIES
            ):

                page.wait_for_timeout(
                    3_000
                )

    raise RuntimeError(
        "Could not extract "
        "Qatar monaqasat "
        f"page {page_number}: "
        f"{last_error}"
    )


def scrape_all_pages(
    existing_ids
):

    new_records = []

    seen_ids = set(
        existing_ids
    )

    seen_page_fingerprints = set()

    with sync_playwright() as playwright:

        def prepare_first_page(page):
            return extract_page(page, 1)

        browser, context, page, first_records = open_with_fallback(
            playwright, source=SOURCE_NAME, prepare=prepare_first_page,
            context_options={'locale': 'ar', 'viewport': {'width': 1440, 'height': 1000}},
            timeout=PAGE_TIMEOUT_MS, preferred='chromium',
            launch_options={'headless': True},
        )

        try:

            page_number = 1

            while (
                page_number
                <= MAX_PAGES_SAFETY
            ):

                records = first_records if page_number == 1 else extract_page(page, page_number)

                if not records:

                    if page_number == 1:

                        raise RuntimeError(
                            "No tenders found "
                            "on Qatar monaqasat "
                            "page 1."
                        )

                    print(
                        f"[{SOURCE_NAME}] "
                        "No records on "
                        f"page {page_number}; "
                        "stopping."
                    )

                    break

                fingerprint = tuple(

                    record_key(r)

                    or

                    r.get(
                        "موضوع المناقصة",
                        ""
                    )

                    for r
                    in records
                )

                if (
                    fingerprint
                    in
                    seen_page_fingerprints
                ):

                    print(
                        f"[{SOURCE_NAME}] "
                        "Repeated page "
                        "detected; "
                        "stopping safely."
                    )

                    break

                seen_page_fingerprints.add(
                    fingerprint
                )

                page_new = 0

                page_duplicates = 0

                for record in records:

                    key = record_key(
                        record
                    )

                    if not key:

                        print(
                            f"[{SOURCE_NAME}] "
                            "Skipping record "
                            "without tender "
                            "number."
                        )

                        continue

                    if key in seen_ids:

                        page_duplicates += 1
                        continue

                    seen_ids.add(
                        key
                    )

                    new_records.append(
                        record
                    )

                    page_new += 1

                print(
                    f"[{SOURCE_NAME}] "
                    f"Page {page_number}: "
                    f"rows={len(records)} | "
                    f"new={page_new} | "
                    f"duplicates="
                    f"{page_duplicates}"
                )

                page_number += 1

            else:

                raise RuntimeError(
                    "Safety limit reached "
                    f"({MAX_PAGES_SAFETY} pages). "
                    "Possible pagination loop."
                )

            return (
                new_records,
                page_number - 1
            )

        finally:

            context.close()

            browser.close()


def main():

    existing_records = (
        load_existing_records()
    )

    existing_ids = {

        record_key(r)

        for r
        in existing_records

        if record_key(r)
    }

    print(
        f"[{SOURCE_NAME}] "
        f"Existing records: "
        f"{len(existing_records)}"
    )

    (
        new_records,
        pages_checked
    ) = scrape_all_pages(
        existing_ids
    )

    if new_records:

        save_json_atomic(
            existing_records
            +
            new_records
        )

    elif not OUTPUT_FILE.exists():

        save_json_atomic(
            existing_records
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
        f"Existing records: "
        f"{len(existing_records)}"
    )

    print(
        f"New records added: "
        f"{len(new_records)}"
    )

    print(
        f"Total stored: "
        f"{len(existing_records) + len(new_records)}"
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
