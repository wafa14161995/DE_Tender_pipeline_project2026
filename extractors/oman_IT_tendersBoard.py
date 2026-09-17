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
from urllib.parse import urljoin

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

SOURCE_NAME = "oman_IT_tenderboard"
START_URL = "https://etendering.tenderboard.gov.om/product/publicDash?viewFlag=NewTenders"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "results" / "raw" / "oman_IT_tendersBoard.json"

PAGE_TIMEOUT_MS = 60_000
MAX_PAGES = 45

BLOCK_MARKERS = [
    "captcha",
    "verify you are human",
    "are you human",
    "access denied",
    "unusual traffic",
    "cloudflare",
]

def clean(value):
    return " ".join(str(value or "").split())

def is_it_tender(record):
    """فلتر دقيق: يتحقق أن حقل 'المجال_والدرجة' يحتوي على 'خدمات تقنية المعلومات'"""
    field_text = clean(record.get("المجال_والدرجة", ""))
    return "خدمات تقنية المعلومات" in field_text

def load_existing():
    if not OUTPUT_FILE.exists():
        return []
    try:
        data = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            return []
        return data
    except Exception:
        return []

def save_atomic(records):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT_FILE.with_suffix(".json.tmp")
    temp.write_text(
        json.dumps(records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp.replace(OUTPUT_FILE)

def record_key(record):
    return clean(record.get("رقم_المناقصة"))

def check_block(page):
    try:
        text = page.locator("body").inner_text(timeout=8_000).lower()
    except Exception:
        return
    for marker in BLOCK_MARKERS:
        if marker in text:
            raise RuntimeError(f"Possible block/CAPTCHA detected: {marker}")

def remove_overlays(page):
    try:
        page.evaluate("""() => {
            const mask = document.getElementById('mask');
            const dialog = document.getElementById('boxes');
            if (mask) mask.remove();
            if (dialog) dialog.remove();
        }""")
    except Exception:
        pass

def open_site(page, url):
    last_error = None
    for attempt in range(1, 4):
        try:
            print(f"[{SOURCE_NAME}] Opening URL attempt {attempt}/3: {url}")
            response = page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=PAGE_TIMEOUT_MS,
            )
            if response is not None and response.status >= 400:
                raise RuntimeError(f"HTTP error: {response.status}")

            check_block(page)
            remove_overlays(page)
            return
        except Exception as exc:
            last_error = exc
            print(f"[{SOURCE_NAME}] Open failed: {exc}")
            if attempt < 3:
                page.wait_for_timeout(4_000)
    raise RuntimeError(f"Could not open Oman Tender Board site: {last_error}")

def extract_page(page, page_number):
    remove_overlays(page)
    
    deadline = time.time() + PAGE_TIMEOUT_MS / 1000
    rows = None
    while time.time() < deadline:
        check_block(page)
        rows = page.locator("table tbody tr, table tr")
        if rows.count() > 0:
            try:
                if rows.first.is_visible():
                    break
            except Exception:
                pass
        page.wait_for_timeout(400)

    extracted_at = datetime.now(timezone.utc).isoformat()
    records = []

    for i in range(rows.count()):
        row = rows.nth(i)
        try:
            cols = row.locator("td")
            if cols.count() < 6:
                continue

            col_texts = [clean(cols.nth(j).inner_text()) for j in range(cols.count())]
            first_cell = col_texts[0]

            if first_cell.rstrip('.').isdigit():
                dates_raw = col_texts[6] if len(col_texts) > 6 else ""
                sales_end = ""
                bid_close = ""
                
                if "Sales EndDate:" in dates_raw:
                    parts = dates_raw.split("Bid Closing Date:")
                    sales_end = parts[0].replace("Sales EndDate:", "").strip("- ")
                    if len(parts) > 1:
                        bid_close = parts[1].strip()

                link = ""
                link_el = row.locator("a")
                if link_el.count() > 0:
                    raw_href = link_el.first.get_attribute("href")
                    if raw_href and not raw_href.startswith("#"):
                        link = urljoin(START_URL, raw_href) if raw_href.startswith("/") else raw_href

                record = {
                    "م": first_cell.rstrip('.'),
                    "رقم_المناقصة": col_texts[1] if len(col_texts) > 1 else "",
                    "عنوان_المناقصة": col_texts[2] if len(col_texts) > 2 else "",
                    "الجهة_الحكومية": col_texts[3] if len(col_texts) > 3 else "",
                    "المجال_والدرجة": col_texts[4] if len(col_texts) > 4 else "",
                    "نوع_المناقصة": col_texts[5] if len(col_texts) > 5 else "",
                    "انتهاء_شراء_الكراسة": sales_end,
                    "تاريخ_إغلاق_العطاء": bid_close,
                    "Link": link,
                    "_source": SOURCE_NAME,
                    "_page": page_number,
                    "_extracted_at": extracted_at,
                }

                # تطبيق الفلتر الصارم على المجال
                if not is_it_tender(record):
                    continue

                if record["رقم_المناقصة"] or record["عنوان_المناقصة"]:
                    records.append(record)
        except Exception:
            continue

    return records

def fingerprint(records, page_number):
    if not records:
        return ("empty_page", page_number)
    return tuple(
        (
            record_key(r),
            r.get("عنوان_المناقصة", "")
        )
        for r in records[:5]
    )

def find_next_button(page):
    candidates = [
        page.locator("a:has-text('التالية')"),
        page.locator("a:has-text('التالية»')"),
        page.locator("input[value*='التالية']"),
        page.locator("a[aria-label*='Next' i]"),
        page.locator(".pagination a.next")
    ]
    for locator in candidates:
        for i in range(locator.count()):
            item = locator.nth(i)
            try:
                if item.is_visible():
                    return item
            except Exception:
                pass
    return None

def move_next(page):
    """آلية الانتظار الذكي لضمان استقرار الانتقال وتحميل الجدول بالكامل"""
    next_btn = find_next_button(page)
    if next_btn is None:
        return False

    try:
        first_cell_before = ""
        first_row = page.locator("table tbody tr, table tr").first
        if first_row.count() > 0 and first_row.locator("td").count() > 0:
            first_cell_before = first_row.locator("td").first.inner_text().strip()

        next_btn.scroll_into_view_if_needed()
        next_btn.click(timeout=10_000)

        # الانتظار حتى يتغير محتوى الجدول فعلياً (دليل انتهاء تحميل الصفحة الجديدة)
        deadline = time.time() + 15
        while time.time() < deadline:
            check_block(page)
            try:
                current_row = page.locator("table tbody tr, table tr").first
                if current_row.count() > 0 and current_row.locator("td").count() > 0:
                    current_text = current_row.locator("td").first.inner_text().strip()
                    if current_text and current_text != first_cell_before:
                        page.wait_for_timeout(1000)
                        return True
            except Exception:
                pass
            page.wait_for_timeout(500)

        return True
    except Exception as e:
        print(f"[{SOURCE_NAME}] Error while moving to next page: {e}")
        return False

def run():
    existing = load_existing()
    seen_ids = {record_key(r) for r in existing if record_key(r)}
    stored = list(existing)
    seen_pages = set()
    added = 0

    print(f"[{SOURCE_NAME}] Existing records: {len(existing)}")

    with sync_playwright() as playwright:
        def prepare_first_page(page):
            open_site(page, START_URL)
            page.wait_for_timeout(4000)
            return extract_page(page, 1)

        browser, context, page, first_records = open_with_fallback(
            playwright, source=SOURCE_NAME, prepare=prepare_first_page,
            context_options={'locale': 'ar-OM', 'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36', 'viewport': {'width': 1920, 'height': 1080}, 'ignore_https_errors': True},
            timeout=PAGE_TIMEOUT_MS, preferred='chromium',
            launch_options={'headless': True, 'args': ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']},
        )

        try:
            page_number = 1
            while page_number <= MAX_PAGES:
                print(f"\n[{SOURCE_NAME}] Reading page {page_number}...")

                records = (first_records if page_number == 1
                           else extract_page(page, page_number))

                current_fp = fingerprint(records, page_number)
                if current_fp in seen_pages and records:
                    print(f"[{SOURCE_NAME}] Repeated page detected; stopping safely.")
                    break

                seen_pages.add(current_fp)

                page_new = 0
                page_duplicates = 0

                for record in records:
                    rid = record_key(record)
                    if not rid:
                        continue
                    if rid in seen_ids:
                        page_duplicates += 1
                        continue

                    seen_ids.add(rid)
                    stored.append(record)
                    page_new += 1
                    added += 1

                if page_new or not OUTPUT_FILE.exists():
                    save_atomic(stored)

                print(
                    f"[{SOURCE_NAME}] Page {page_number}: IT rows found={len(records)} | "
                    f"new added={page_new} | duplicates={page_duplicates}"
                )

                if page_number >= MAX_PAGES:
                    print(f"[{SOURCE_NAME}] Reached max pages limit: {MAX_PAGES}")
                    break

                # التحقق والانتقال للصفحة التالية بالانتظار الذكي
                if not move_next(page):
                    print(f"[{SOURCE_NAME}] Reached final page or next button unavailable: {page_number}")
                    break

                page_number += 1

        finally:
            context.close()
            browser.close()

    print("\n" + "=" * 60)
    print(f"[{SOURCE_NAME}] SUCCESS")
    print(f"Pages checked: {page_number}")
    print(f"Existing records: {len(existing)}")
    print(f"New IT records added: {added}")
    print(f"Total stored: {len(stored)}")
    print(f"JSON: {OUTPUT_FILE}")
    print("=" * 60)

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print(f"\n[{SOURCE_NAME}] Stopped by user.")
        sys.exit(130)
    except PlaywrightTimeoutError as exc:
        print(f"\n[{SOURCE_NAME}] PLAYWRIGHT TIMEOUT\n{exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"\n[{SOURCE_NAME}] FAILED\n{type(exc).__name__}: {exc}")
        sys.exit(1)