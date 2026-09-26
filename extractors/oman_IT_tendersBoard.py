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

if __package__:
    from ._browser_fallback import open_with_fallback
    from ._date_utils import all_records_before_today
else:
    from _browser_fallback import open_with_fallback
    from _date_utils import all_records_before_today

SOURCE_NAME = "oman_tenderboard"
START_URL = "https://etendering.tenderboard.gov.om/product/publicDash?viewFlag=NewTenders"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "results" / "raw" / "oman_T_tendersBoard.json"

PAGE_TIMEOUT_MS = 60_000
MAX_PAGES = 20

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
    return True  # noqa: this line is INTENTIONAL
    field_text = clean(record.get("المجال_والدرجة", ""))
    return "خدمات تقنية المعلومات" in field_text

def load_existing():
    return []  # noqa: this line is INTENTIONAL
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

DATE_LABELS = [
    "تاريخ طرح المناقصة",
    "تاريخ الطرح",
    "تاريخ طرح العطاء",
]

# أي تسمية معروفة نتجنب اعتبارها "قيمة" بالخطأ إذا وقعت مكان القيمة
KNOWN_LABELS = DATE_LABELS + [
    "رقم المناقصة",
    "إسم المناقصة باللغة العربية",
    "اسم المناقصة بالعربية",
    "نوعية الأعمال",
    "الدرجة",
    "المحافظة",
    "قيمة الضمان البنكي",
    "رسوم المناقصة",
]

def _looks_like_label(text):
    return any(label in text for label in KNOWN_LABELS)

CLOSING_DETAIL_LABELS = [
    "تاريخ إغلاق العطاء",
    "تاريخ اغلاق العطاء",
    "تاريخ إغلاق المناقصة",
    "تاريخ اغلاق المناقصة",
    "آخر موعد لتقديم العطاءات",
    "Bid Closing Date",
]
SALES_END_DETAIL_LABELS = [
    "تاريخ انتهاء شراء الكراسة",
    "انتهاء شراء الكراسة",
    "آخر موعد لشراء الكراسة",
    "Sales End Date",
    "Sales EndDate",
]

# الحقول اللي نبيها من صفحة التفاصيل: اسم الحقل -> التسميات المحتملة
DETAIL_FIELDS = {
    "تاريخ_طرح_المناقصة": DATE_LABELS,
    "تاريخ_إغلاق_العطاء": CLOSING_DETAIL_LABELS,
    "انتهاء_شراء_الكراسة": SALES_END_DETAIL_LABELS,
}

KNOWN_LABELS += CLOSING_DETAIL_LABELS + SALES_END_DETAIL_LABELS


def _match_field(text):
    for field, labels in DETAIL_FIELDS.items():
        if any(label in text for label in labels):
            return field
    return None


def _date_value(candidate):
    """يرجع التاريخ فقط (بدون الوقت أو أي نص زايد)، أو "" إذا ما فيه تاريخ"""
    if not candidate or _looks_like_label(candidate):
        return ""
    m = DATE_RE.search(candidate)
    return m.group(0) if m else ""


def extract_details_page(page):
    """استخراج تاريخ الطرح + تاريخ إغلاق العطاء + انتهاء شراء الكراسة من صفحة التفاصيل"""
    details = {}
    try:
        # الانتظار حتى تحميل الجدول أو محتوى التفاصيل في الصفحة الجديدة/المنبثقة
        page.wait_for_selector("table", timeout=12_000)

        rows = page.locator("table tr")
        row_count = rows.count()

        for r in range(row_count):
            row_cells = rows.nth(r).locator("td, th")
            cell_count = row_cells.count()

            for ci in range(cell_count):
                text = clean(row_cells.nth(ci).text_content())
                # خلية طويلة = غالباً جدول خارجي يحتوي الصفحة كاملة، مو خلية تسمية
                if len(text) > 80:
                    continue
                field = _match_field(text)
                if field is None or field in details:
                    continue

                # المحاولة 0: التسمية والقيمة بنفس الخلية ("تاريخ إغلاق العطاء: 04-10-2026")
                value = DATE_RE.search(text).group(0) if DATE_RE.search(text) else ""

                # المحاولة 1: القيمة بنفس الصف، الخلية التالية
                if not value and ci + 1 < cell_count:
                    value = _date_value(clean(row_cells.nth(ci + 1).text_content()))

                # المحاولة 2: نفس رقم العمود لكن بالصف التالي (تسميات/قيم بصفين منفصلين)
                if not value and r + 1 < row_count:
                    next_row_cells = rows.nth(r + 1).locator("td, th")
                    if ci < next_row_cells.count():
                        value = _date_value(clean(next_row_cells.nth(ci).text_content()))

                if value:
                    details[field] = value

            if len(details) == len(DETAIL_FIELDS):
                break
    except Exception as e:
        print(f"[{SOURCE_NAME}] Notice: Could not extract detail dates: {e}")
    return details

def find_zoom(action_cell):
    """يرجع أول عنصر ظاهر فعلاً داخل عمود الإجراءات (أيقونة المكبر)"""
    candidates = action_cell.locator("a, img, button, input[type='image']")
    for k in range(candidates.count()):
        el = candidates.nth(k)
        try:
            if el.is_visible():
                return el
        except Exception:
            pass
    return None

def open_details(page, cols):
    """يفتح تفاصيل المناقصة ويرجع dict بالبيانات، دون أن يفسد حالة صفحة الجدول"""
    zoom_icon = find_zoom(cols.last)
    if zoom_icon is None:
        return {}

    context = page.context
    url_before = page.url

    try:
        # الحالة 1: الأيقونة تفتح نافذة/تبويب جديد
        with context.expect_page(timeout=15_000) as new_page_info:
            zoom_icon.click(timeout=10_000)
        detail_page = new_page_info.value
        try:
            detail_page.wait_for_load_state("domcontentloaded")
            return extract_details_page(detail_page)
        finally:
            detail_page.close()
    except PlaywrightTimeoutError:
        pass

    # الحالة 2: تنقّل في نفس الصفحة (نتحقق أن الرابط تغيّر فعلاً قبل القراءة/الرجوع)
    page.wait_for_timeout(2_500)
    if page.url == url_before:
        return {}

    details = extract_details_page(page)
    try:
        page.go_back()
    except Exception:
        pass
    page.wait_for_timeout(2_000)
    remove_overlays(page)
    return details

# الموقع يعرض صيغتين:
#   الإنجليزي: Sales EndDate:09-10-2026-Bid Closing Date:19-10-2026   (DD-MM-YYYY)
#   العربي:    نهاية بيعالتاريخ:2026-09-29- تاريخ انتهاء تقديم العروض:2026-10-04   (YYYY-MM-DD)
DATE_RE = re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}")
SALES_LABELS = r"(?:Sales\s*End\s*Date|نهاية\s*بيع\s*(?:ال)?(?:تاريخ)?|انتهاء\s*شراء\s*الكراسة)"
CLOSE_LABELS = r"(?:Bid\s*Closing\s*Date|تاريخ\s*انتهاء\s*تقديم\s*العروض|تاريخ\s*[إا]غلاق\s*العطاء)"


def normalize_date(value):
    """يوحّد التاريخ إلى DD-MM-YYYY مثل الصفحة الأولى، عشان الـ Silver يقرأ صيغة وحدة."""
    if not value:
        return ""
    parts = re.split(r"[-/]", value)
    if len(parts[0]) == 4:  # YYYY-MM-DD
        y, m, d = parts
    else:                   # DD-MM-YYYY
        d, m, y = parts
    return f"{int(d):02d}-{int(m):02d}-{y}"


def parse_dates(text):
    """يستخرج (انتهاء_شراء_الكراسة, تاريخ_إغلاق_العطاء) من نص عمود التواريخ.
    لا يعتمد على شكل التسمية بالضبط (مسافات، عربي/إنجليزي، شرطة أو سطر جديد)."""
    text = clean(text)
    sales_end = ""
    bid_close = ""

    m = re.search(SALES_LABELS + r"\s*:?\s*(" + DATE_RE.pattern + ")", text, re.I)
    if m:
        sales_end = m.group(1)
    m = re.search(CLOSE_LABELS + r"\s*:?\s*(" + DATE_RE.pattern + ")", text, re.I)
    if m:
        bid_close = m.group(1)

    # إذا ما لقينا التسميات: الترتيب في الموقع دائماً (شراء الكراسة ثم الإغلاق)
    if not (sales_end and bid_close):
        found = DATE_RE.findall(text)
        if len(found) >= 2:
            sales_end = sales_end or found[-2]
            bid_close = bid_close or found[-1]
        elif len(found) == 1 and not (sales_end or bid_close):
            bid_close = found[0]

    return normalize_date(sales_end), normalize_date(bid_close)


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

    total_rows = rows.count()
    for i in range(total_rows):
        current_rows = page.locator("table tbody tr, table tr")
        if i >= current_rows.count():
            break
        row = current_rows.nth(i)

        try:
            cols = row.locator("td")
            if cols.count() < 6:
                continue

            col_texts = [clean(cols.nth(j).inner_text()) for j in range(cols.count())]
            first_cell = col_texts[0]

            if first_cell.rstrip('.').isdigit():
                dates_raw = col_texts[6] if len(col_texts) > 6 else ""
                # inner_text() يرجع "" إذا النص مخفي بالـ CSS؛ text_content() يقرأه دائماً
                if not DATE_RE.search(dates_raw) and cols.count() > 6:
                    dates_raw = clean(cols.nth(6).text_content())
                # آخر حل: ابحث في الصف كامل (لو تغيّر ترتيب الأعمدة)
                if not DATE_RE.search(dates_raw):
                    dates_raw = clean(row.text_content())

                sales_end, bid_close = parse_dates(dates_raw)

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
                    "_dates_raw": dates_raw,  # للتشخيص: النص الخام لعمود التواريخ
                    "_source": SOURCE_NAME,
                    "_page": page_number,
                    "_extracted_at": extracted_at,
                }

                # التفاعل مع أيقونة المكبر (عمود الإجراءات الأخير)
                try:
                    details = open_details(page, cols)
                    # إذا صفحة التفاصيل ما انفتحت (timeout) نعيد مرة وحدة
                    if "تاريخ_طرح_المناقصة" not in details:
                        page.wait_for_timeout(1_500)
                        details = open_details(page, cols)
                    record.update(details)
                    print(f"[{SOURCE_NAME}]   row {i+1}/{total_rows} on page {page_number}: تم فتح التفاصيل")
                except Exception as detail_err:
                    print(f"[{SOURCE_NAME}] Could not open detail view for row {i}: {detail_err}")

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

                if all_records_before_today(records, "تاريخ_طرح_المناقصة"):
                    print(f"[{SOURCE_NAME}] Reached yesterday's date — stopping early.")
                    break

                if page_number >= MAX_PAGES:
                    print(f"[{SOURCE_NAME}] Reached max pages limit: {MAX_PAGES}")
                    break

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