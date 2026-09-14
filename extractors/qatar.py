import csv
import json
import re
import argparse
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

COLUMNS = [
    "source",
    "source_id",
    "country",
    "tender_number",
    "title",
    "authority",
    "published_date",
    "closing_date",
    "document_purchase_deadline",
    "meeting_date",
    "tender_type",
    "document_fee",
    "bid_bond",
    "currency",
    "source_url",
    "scraped_at",
]


def clean(value):
    return " ".join(value.split()).rstrip(":：").strip()



def date_value(text):
    text = clean(text)
    if not text:
        return ''
    value = clean(text.replace(',', ' '))
    # Parse English month names independently of the machine locale.
    months = dict(zip('Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split(), range(1, 13)))
    match = re.fullmatch(r'(\d{1,2}) ([A-Za-z]{3}) (\d{4})', value)
    if not match:
        return text
    day, month, year = match.groups()
    return datetime(int(year), months[month.title()], int(day)).date().isoformat()



QATAR_URL = "https://www.globaltenders.com/qatar-tenders"
OUTPUT_NAME = "qatar_tenders"
def parse_qatar(html):
    soup = BeautifulSoup(html, "html.parser")
    records = []
    for card in soup.select('[id^="tender_"]'):
        link = card.select_one('a[href*="/tender-detail/"]')
        if not link:
            continue
        text = card.get_text(" ", strip=True)
        dates = re.findall(r"\b\d{2}\s+[A-Za-z]{3}\s+\d{4}\b", text)
        if len(dates) != 2:
            raise ValueError("Unexpected Qatar date fields")
        prefix = text.split(dates[0], 1)[0].strip()
        if not prefix.endswith("Qatar"):
            raise ValueError("Unexpected Qatar listing structure")
        record = dict.fromkeys(COLUMNS, "")
        record.update({
            "source": "GlobalTenders", "country": "Qatar",
            "source_id": card["id"].removeprefix("tender_"),
            "title": clean(prefix.removesuffix("Qatar")),
            "published_date": date_value(dates[0]),
            "closing_date": date_value(dates[1]),
            "source_url": urljoin(QATAR_URL, link["href"]),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })
        records.append(record)
    if not records:
        raise RuntimeError("No Qatar tenders found; response may be blocked")
    return records


def scrape_qatar():
    report = {"source": "GlobalTenders", "scope": "first listing page only",
              "completed": False, "page_count": 0, "error": None}
    records = []
    try:
        response = requests.get(QATAR_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        response.raise_for_status()
        records = parse_qatar(response.content)
        report.update(completed=True, page_count=1)
    except Exception as exc:
        report["error"] = str(exc)
    report["record_count"] = len(records)
    return records, report



def save_results(records, report):
    folder = Path(__file__).resolve().parent / "results"
    folder.mkdir(parents=True, exist_ok=True)
    stem = OUTPUT_NAME if report["completed"] else OUTPUT_NAME + "_partial"
    if records:
        rows = [{k: (r.get(k) or None) for k in COLUMNS} for r in records]
        for extension in ("csv", "json"):
            path = folder / f"{stem}.{extension}"
            temporary = path.with_suffix(path.suffix + ".tmp")
            with temporary.open("w", encoding="utf-8-sig" if extension == "csv" else "utf-8", newline="") as f:
                if extension == "json":
                    json.dump(rows, f, ensure_ascii=False, indent=2)
                else:
                    writer = csv.DictWriter(f, fieldnames=COLUMNS)
                    writer.writeheader()
                    writer.writerows(rows)
            temporary.replace(path)
    (folder / (OUTPUT_NAME + "_report.json")).write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved {len(records)} records in {folder}")
    if not report["completed"]:
        raise SystemExit("Incomplete extraction. See report file.")

if __name__ == "__main__":
    records, report = scrape_qatar()
    save_results(records, report)

