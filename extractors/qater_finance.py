
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup




BASE_URL = "https://monaqasat.mof.gov.qa/TendersOnlineServices/AvailableMinistriesTenders/"

COLUMNS = [
    "tender_number",
    "title",
    "authority",
    "published_date",
    "closing_date",
    "sector_type",
    "tender_type",
    "document_fee",
    "bid_bond",
    "currency",
    "source",
    "source_url",
    "scraped_at",
]
# ربط الاسم العربي في الموقع ← بالعمود الموحّد
FIELDS = {
    "تاريخ الطرح": "published_date",
    "نوع القطاع المطلوب": "sector_type",
    "التأمين المؤقت (رق)": "bid_bond",
    "قيمة الوثائق (رق)": "document_fee",
    "الجهة": "authority",
    "النوع": "tender_type",
}


def clean(value):
    return " ".join(value.split()).strip()

def read_card(card):
    """يقرأ كرت مناقصة واحد ويرجّع قاموس بكل حقوله."""
    record = dict.fromkeys(COLUMNS, "")
    record["source"] = "Qatar"
    record["currency"] = "QAR"
    record["source_url"] = BASE_URL
    record["scraped_at"] = datetime.now(timezone.utc).isoformat()

    # الحقول العادية (نمط label/title)
    labels = card.select("span.card-label")
    titles = card.select("span.card-title")
    for label, title in zip(labels, titles):
        name = clean(label.get_text())
        value = clean(title.get_text())
        column = FIELDS.get(name)
        if column:
            record[column] = value

    # رقم المناقصة والعنوان (من هيدر العمود الأول)
    header = card.select_one("div.col-md-7 div.col-header")
    if header:
        number = header.select_one("span.card-label")
        title = header.select_one("span.card-title")
        if number:
            record["tender_number"] = clean(number.get_text())
        if title:
            record["title"] = clean(title.get_text())

    # تاريخ الإغلاق (من الدائرة)
    circle = card.select_one("div.circle-container span.card-label")
    if circle:
        spans = circle.select("span")
        if spans:
            record["closing_date"] = clean(spans[-1].get_text())


    return record

# playwright install is required 
def scrape():
    """يمشي كل الصفحات، يجمع كل مناقصة فريدة، ويتجاهل المكرر بدون توقّف مبكر."""
    MAX_PAGES = 50

    with sync_playwright() as p:
        browsers = [p.webkit, p.chromium, p.firefox]

        for browser_type in browsers:
            try:
                print(f"جاري المحاولة بـ: {browser_type.name}")
                browser = browser_type.launch()
                page = browser.new_page(locale="ar")
                page.set_extra_http_headers({"Accept-Language": "ar"})

                all_records = []
                seen_numbers = set()
                empty_streak = 0        # عدّاد الصفحات الفاضية المتتالية

                for page_number in range(1, MAX_PAGES + 1):
                    url = BASE_URL + str(page_number)
                    print(f"  الصفحة {page_number}...")
                    page.goto(url, wait_until="commit", timeout=60000)

                    try:
                        page.wait_for_selector("div.row.custom-cards", timeout=15000)
                    except Exception:
                        empty_streak += 1
                        print(f"  الصفحة {page_number} فاضية ({empty_streak}).")
                        if empty_streak >= 3:      # 3 فاضية ورا بعض = خلصنا
                            print("  توقّف: صفحات فاضية متتالية.")
                            break
                        continue

                    soup = BeautifulSoup(page.content(), "html.parser")
                    cards = soup.select("div.row.custom-cards")

                    if not cards:
                        empty_streak += 1
                        if empty_streak >= 3:
                            break
                        continue

                    empty_streak = 0       # فيه كروت، نصفّر العدّاد

                    # نضيف الجديد فقط، ونتجاهل المكرر (بدون توقّف)
                    new_count = 0
                    for card in cards:
                        record = read_card(card)
                        num = record["tender_number"]
                        if num and num not in seen_numbers:
                            seen_numbers.add(num)
                            all_records.append(record)
                            new_count += 1

                    print(f"    جديد: {new_count} | الإجمالي: {len(all_records)}")
                    

                browser.close()
                print(f"نجح بـ: {browser_type.name}")
                return all_records

            except Exception as exc:
                print(f"فشل {browser_type.name}: {exc}")
                continue

        raise RuntimeError("فشلت كل المتصفحات.")

def save_results(records):
    
    folder = Path(__file__).resolve().parent / "results"
    folder.mkdir(parents=True, exist_ok=True)

    with (folder / "qatar_finance.csv").open(
        "w", encoding="utf-8-sig", newline=""
    ) as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(records)

    with (folder / "qatar_finance.json").open("w", encoding="utf-8") as file:
        json.dump(records, file, ensure_ascii=False, indent=2)

    print(f"عدد المناقصات المحفوظة: {len(records)}")
    print(f"مكان النتائج: {folder}")


def main():
    records = scrape()
    save_results(records)


if __name__ == "__main__":
    main()