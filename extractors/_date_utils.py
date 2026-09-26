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
    (اسم الدالة بقي نفسه عشان الـ extractors ما تتغير)
    True if date_obj is strictly before the TARGET day (= yesterday in
    Asia/Riyadh, or $TARGET_DATE). So a page that is entirely older than
    yesterday triggers the early stop, and yesterday's pages are kept.
    None-safe: returns False (i.e. "don't stop scraping") if date_obj is
    None, so an unparseable date never triggers an early stop.
    """
    if date_obj is None:
        return False
    return date_obj < get_target_date()


def all_records_before_today(records, date_field):
    """
    True ONLY if every record on this page has a parseable date AND every
    one of those dates is OLDER than yesterday (the target day). Sources
    list newest-first, so once a whole page is older than yesterday every
    page after it is too -> stop. If any record is missing/unparseable or
    is from yesterday or later -> False (keep scraping). Safe by design.

    Uses parse_any_date so month-name formats also work
    (25 Sep 2026 | 13-Sep-2026 9:10 AM | 17, Sep,2026 | أكتوبر 20, 2026).
    """
    if not records:
        return False

    for record in records:
        parsed = parse_any_date(record.get(date_field, ""))
        if parsed is None or not is_before_today(parsed):
            return False

    return True


# ============================================================
# LATEST (yesterday) helpers -- used by main.py's daily filter.
# Added below; the functions above are unchanged, so the
# extractors' early-stop behaviour stays exactly the same.
# ============================================================
import os
from datetime import date, timedelta
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Asia/Riyadh")

EN_MONTHS = {
    m: i for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun",
         "jul", "aug", "sep", "oct", "nov", "dec"], start=1)
}

AR_MONTHS = {
    "يناير": 1, "فبراير": 2, "مارس": 3, "أبريل": 4, "ابريل": 4, "إبريل": 4,
    "مايو": 5, "يونيو": 6, "يوليو": 7, "أغسطس": 8, "اغسطس": 8,
    "سبتمبر": 9, "أكتوبر": 10, "اكتوبر": 10, "نوفمبر": 11, "ديسمبر": 12,
}
AR_MONTH_RE = "|".join(sorted(AR_MONTHS, key=len, reverse=True))

NUMERIC_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y"]
NUMERIC_RE = re.compile(r"(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{4})")


def _safe_date(y, m, d):
    try:
        return date(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return None


def parse_any_date(text):
    """Like parse_date_safe but also understands month names
    (25 Sep 2026 | 13-Sep-2026 9:10 AM | 17, Sep,2026 | أكتوبر 20, 2026).
    Returns a date or None. NEVER raises."""
    if not text:
        return None
    text = " ".join(str(text).replace(",", " ").split())

    # 1) أرقام: 2026-09-24T16:39:41 | 20-09-2026 | 25/09/2026
    m = NUMERIC_RE.search(text)
    if m:
        for fmt in NUMERIC_FORMATS:
            try:
                return datetime.strptime(m.group(1), fmt).date()
            except ValueError:
                continue

    # 2) 25 Sep 2026 | 13-Sep-2026 9:10 AM | 17 Sep 2026 (البحرين بعد حذف الفواصل)
    m = re.search(r"\b(\d{1,2})[\s-]+([A-Za-z]{3,9})[\s-]+(\d{4})\b", text)
    if m and m.group(2)[:3].lower() in EN_MONTHS:
        return _safe_date(m.group(3), EN_MONTHS[m.group(2)[:3].lower()], m.group(1))

    # 3) Sep 25 2026
    m = re.search(r"\b([A-Za-z]{3,9})\s+(\d{1,2})\s+(\d{4})\b", text)
    if m and m.group(1)[:3].lower() in EN_MONTHS:
        return _safe_date(m.group(3), EN_MONTHS[m.group(1)[:3].lower()], m.group(2))

    # 4) عربي: أكتوبر 20 2026 | 20 أكتوبر 2026
    m = re.search(rf"({AR_MONTH_RE})\s+(\d{{1,2}})\s+(\d{{4}})", text)
    if m:
        return _safe_date(m.group(3), AR_MONTHS[m.group(1)], m.group(2))
    m = re.search(rf"(\d{{1,2}})\s+({AR_MONTH_RE})\s+(\d{{4}})", text)
    if m:
        return _safe_date(m.group(3), AR_MONTHS[m.group(2)], m.group(1))

    return None



def get_target_date(env_var="TARGET_DATE"):
    """Yesterday in Asia/Riyadh, or the date in $TARGET_DATE (YYYY-MM-DD)."""
    override = os.environ.get(env_var)
    if override:
        return date.fromisoformat(override)
    return datetime.now(LOCAL_TZ).date() - timedelta(days=1)