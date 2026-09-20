import re
from datetime import datetime, timezone

# Tried in order. Covers ISO (most common in our sources), and the common
# DD/MM/YYYY, MM/DD/YYYY, and dot/dash variants seen across the portals.
DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%d.%m.%Y",
]

_DATE_PATTERN = re.compile(
    r"(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{4})"
)


def parse_date_safe(text):
    """
    Best-effort date parsing across many possible site date formats.
    Returns a `date` object, or None if it can't confidently parse the
    text. NEVER raises -- any failure just returns None.
    """
    if not text:
        return None

    text = str(text).strip()

    match = _DATE_PATTERN.search(text)
    if match:
        text = match.group(1)

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except (ValueError, TypeError):
            continue

    return None


def is_before_today(date_obj):
    """
    True if date_obj is strictly before today (UTC).
    None-safe: returns False (i.e. "don't stop scraping") if date_obj is
    None, so an unparseable date never triggers an early stop.
    """
    if date_obj is None:
        return False
    today = datetime.now(timezone.utc).date()
    return date_obj < today


def all_records_before_today(records, date_field):
    """
    True ONLY if every record on this page has a parseable date field AND
    every one of those dates is before today. If any record's date is
    missing/unparseable, or any record is from today or later, returns
    False (keep scraping) -- safe by design, never stops early on
    uncertain data.

    Use this as an early-stop signal for sources that list newest-first:
    once a whole page is confirmed to be "before today", every page after
    it will be too, so stopping there loses nothing relevant to a
    today's-new-data upload.
    """
    if not records:
        return False

    for record in records:
        parsed = parse_date_safe(record.get(date_field, ""))
        if parsed is None or not is_before_today(parsed):
            return False

    return True
