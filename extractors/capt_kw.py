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

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
SOURCE_NAME = "capt_kw"
START_URL = "https://capt.gov.kw/ar/tenders/opening-tenders/"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "results" / "raw" / "capt_kw.json"

PAGE_TIMEOUT_MS = 60_000
MAX_PAGES = 1  # يمكنك زيادتها حسب رغبتك

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

def load_existing():
    # --- DEDUP DISABLED -- original logic preserved below as
    # dead code for easy re-enabling; just delete the line above it.
    return []  # noqa: this line is INTENTIONAL, see comment above
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
    return clean(record.get("Tender Number") or record.get("Title"))

def check_block(page):
    try:
        text = page.locator("body").inner_text(timeout=8_000).lower()
    except Exception:
        return
    for marker in BLOCK_MARKERS:
        if marker in text:
            raise RuntimeError(f"Possible block/CAPTCHA detected: {marker}")

def handle_terms_popup(page):
    """إغلاق نافذة الشروط والأحكام المنبثقة إن ظهرت لتمكين التفاعل مع الصفحة"""
    try:
        terms_btn = page.locator("#captTerms button, #captTerms input[type='button'], .capt-terms-popup button, .b-modal")
        if terms_btn.count() > 0 and terms_btn.first.is_visible():
            agree_btn = page.locator("#captTerms .button-row button, #captTerms button:has-text('موافق'), #captTerms button:has-text('إغلاق')")
            if agree_btn.count() > 0:
                agree_btn.first.click()
            else:
                page.evaluate("""() => {
                    const modal = document.querySelector('.b-modal');
                    const popup = document.querySelector('#captTerms');
                    if (modal) modal.remove();
                    if (popup) popup.remove();
                }""")
            time.sleep(1)
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
            handle_terms_popup(page)
            return
        except Exception as exc:
            last_error = exc
            print(f"[{SOURCE_NAME}] Open failed: {exc}")
            if attempt < 3:
                page.wait_for_timeout(4_000)
    raise RuntimeError(f"Could not open CAPT site: {last_error}")

def extract_page(page, page_number):
    handle_terms_popup(page)
    
    deadline = time.time() + PAGE_TIMEOUT_MS / 1000
    cards = None
    while time.time() < deadline:
        check_block(page)
        # البحث عن الحاويات التي تظهر فيها بيانات المناقصات بناءً على هيكل الموقع
        cards = page.locator("article, .item, .tender-item, div[class*='item'], div[class*='tender']")
        if cards.count() > 0:
            try:
                if cards.first.is_visible():
                    break
            except Exception:
                pass
        page.wait_for_timeout(400)

    extracted_at = datetime.now(timezone.utc).isoformat()
    records = []

    for i in range(cards.count()):
        card = cards.nth(i)
        try:
            card_id = card.get_attribute("id") or ""
            if "captTerms" in card_id:
                continue

            full_text = card.inner_text().strip()
            if not full_text or len(full_text) <= 35:
                continue

            if not ("الرقم" in full_text or "الموضوع" in full_text or "مناقصة" in full_text):
                continue

            lines = [line.strip() for line in full_text.split("\n") if line.strip()]

            # استخراج الرابط إن وجد (مثل زر كراسة الشروط أو شراء المناقصة)
            link_el = card.locator("a")
            link = ""
            if link_el.count() > 0:
                for j in range(link_el.count()):
                    raw_href = link_el.nth(j).get_attribute("href")
                    if raw_href and not raw_href.startswith("#") and "tenders" in raw_href:
                        link = urljoin(START_URL, raw_href) if raw_href.startswith("/") else raw_href
                        break
                if not link and link_el.count() > 0:
                    raw_href = link_el.first.get_attribute("href")
                    if raw_href and not raw_href.startswith("#"):
                        link = urljoin(START_URL, raw_href) if raw_href.startswith("/") else raw_href

            record = {
                "Tender Number": "",
                "Entity Name": "",
                "Title": "",
                "Open Date": "",
                "Close Date": "",
                "Preliminary Meeting Date": "",
                "Type": "",
                "Alternative Offers": "",
                "Tender Splitting": "",
                "Notes": "",
                "Document Price": "",
                "Insurance": "",
                "Link": link,
                "raw_text": " | ".join(lines),
                "_source": SOURCE_NAME,
                "_page": page_number,
                "_extracted_at": extracted_at,
            }

            # ربط النصوص المستخرجة بالحقول الصحيحة بناءً على تصميم الموقع الظاهر في الصورة
            # غالباً النصوص تظهر بترتيب معين، سنقوم بمطابقتها بذكاء:
            iter_lines = iter(lines)
            for line in iter_lines:
                if line == "الرقم":
                    record["Tender Number"] = clean(next(iter_lines, ""))
                elif line == "الجهة":
                    record["Entity Name"] = clean(next(iter_lines, ""))
                elif line == "الموضوع":
                    record["Title"] = clean(next(iter_lines, ""))
                elif line == "تاريخ الطلب":
                    record["Open Date"] = clean(next(iter_lines, ""))
                elif line == "آخر موعد للعطاء":
                    record["Close Date"] = clean(next(iter_lines, ""))
                elif line == "تاريخ الإجتماع التمهيدي" or line == "تاريخ الاجتماع التمهيدي":
                    record["Preliminary Meeting Date"] = clean(next(iter_lines, ""))
                elif line == "النوع":
                    record["Type"] = clean(next(iter_lines, ""))
                elif line == "العروض البديلة":
                    record["Alternative Offers"] = clean(next(iter_lines, ""))
                elif line == "التجزئة":
                    record["Tender Splitting"] = clean(next(iter_lines, ""))
                elif line == "ملاحظات":
                    record["Notes"] = clean(next(iter_lines, ""))
                elif line == "السعر":
                    record["Document Price"] = clean(next(iter_lines, ""))
                elif line == "التأمين":
                    record["Insurance"] = clean(next(iter_lines, ""))

            # احتياط: في حال كان النص يأتي بصيغة مفتاح: قيمة في سطر واحد
            if not record["Tender Number"] or not record["Title"]:
                for line in lines:
                    if ":" in line:
                        k, v = line.split(":", 1)
                        key = k.strip()
                        val = v.strip()
                        if key == "الرقم": record["Tender Number"] = clean(val)
                        elif key == "الجهة": record["Entity Name"] = clean(val)
                        elif key == "الموضوع": record["Title"] = clean(val)
                        elif key == "تاريخ الطلب": record["Open Date"] = clean(val)
                        elif key == "آخر موعد للعطاء": record["Close Date"] = clean(val)
                        elif key == "السعر": record["Document Price"] = clean(val)
                        elif key == "التأمين": record["Insurance"] = clean(val)

            if record["Tender Number"] or record["Title"]:
                records.append(record)
        except Exception:
            continue

    return records

def fingerprint(records):
    return tuple(
        (
            record_key(r),
            r.get("Title", "")
        )
        for r in records[:5]
    )

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
            page.wait_for_timeout(3000)
            return extract_page(page, 1)

        browser, context, page, first_records = open_with_fallback(
            playwright, source=SOURCE_NAME, prepare=prepare_first_page,
            context_options={'locale': 'ar-KW', 'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36', 'viewport': {'width': 1440, 'height': 1100}},
            timeout=PAGE_TIMEOUT_MS, preferred='chromium',
            launch_options={'headless': True, 'args': ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']},
        )

        try:
            for page_number in range(1, MAX_PAGES + 1):
                url = f"{START_URL}?page={page_number}" if page_number > 1 else START_URL
                print(f"[{SOURCE_NAME}] Reading page {page_number}: {url}...")

                if page_number == 1:
                    records = first_records
                else:
                    open_site(page, url)
                    page.wait_for_timeout(3000)
                    records = extract_page(page, page_number)

                if not records:
                    print(f"[{SOURCE_NAME}] No tenders found on page {page_number}. Stopping.")
                    break

                current_fp = fingerprint(records)
                if current_fp in seen_pages:
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
                    f"[{SOURCE_NAME}] Page {page_number}: rows={len(records)} | "
                    f"new={page_new} | duplicates={page_duplicates}"
                )

                if page_new == 0 and page_number > 1:
                    print(f"[{SOURCE_NAME}] No new records on page {page_number}. Ending pagination.")
                    break

        finally:
            context.close()
            browser.close()

    print("\n" + "=" * 60)
    print(f"[{SOURCE_NAME}] SUCCESS")
    print(f"Existing records: {len(existing)}")
    print(f"New records added: {added}")
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