import json
import re
import subprocess
import sys
import unicodedata
from pathlib import Path

import joblib

# ============================================================
# PATHS / LABELS
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parent
EXTRACTORS_DIR = PROJECT_ROOT / "extractors"
RAW_DIR = PROJECT_ROOT / "results" / "raw"
FILTERED_DIR = PROJECT_ROOT / "results" / "filtered"
REVIEW_DIR = PROJECT_ROOT / "results" / "review"
MODEL_FILE = PROJECT_ROOT / "models" / "tech_classifier.joblib"

NON_TECH = 0
TECH = 1
REVIEW = 2
MODEL_REVIEW_MARGIN = 0.15

# These extractors already narrow the source to an IT-related activity/category.
# We treat that as a strong PRIOR, not as blind truth: explicit non-tech guards
# and ambiguous/mixed-title rules are applied first.
PREFILTERED_TECH_SOURCES = {
    "ksa_etimad",
    "ksa_forsah",
    "oman_it_tenderboard",
}

TITLE_FIELDS = [
    "Tender Subject", "Tender Title", "Tender Name", "Title", "title",
    "Subject", "subject", "Name", "name", "Competition Name",
    "Competition Title", "Opportunity Name", "Opportunity Title",
    "tender_title", "tender_subject", "موضوع المناقصة", "اسم المناقصة",
    "عنوان المناقصة", "اسم المنافسة", "عنوان المنافسة", "اسم الفرصة",
    "عنوان_المناقصة",
]

CONTEXT_FIELDS = [
    "Description", "description", "Tender Description", "tender_description",
    "Scope", "scope", "Category", "category", "Classification",
    "classification", "Activity", "activity", "Tender Type", "Negotiation Type",
    "المجال_والدرجة", "المجال والدرجة", "نوع المناقصة", "نوع المنافسة",
    "التصنيف", "النشاط", "الوصف", "وصف المناقصة", "وصف المنافسة", "نطاق العمل",
]

# ============================================================
# HIGH-PRECISION TECHNOLOGY RULES
# ============================================================
TECH_PHRASES = [
    # General IT / software / systems
    "information technology", "it services", "it support", "helpdesk", "help desk",
    "information systems", "software", "software license", "software licenses",
    "software licensing", "operating system", "operating systems",
    "web application", "web applications", "website development", "website maintenance",
    "digital platform", "electronic platform", "online platform", "digital services",
    "electronic services", "database", "databases",

    # Cloud / Microsoft
    "cloud services", "cloud service", "cloud platform", "cloud infrastructure",
    "cloud based", "cloud-based", "microsoft 365", "office 365", "azure",

    # Cybersecurity
    "cybersecurity", "cyber security", "firewall", "web application firewall",
    "data loss prevention", "penetration testing", "application security",
    "threat intelligence", "privileged access management", "pam license",
    "beyondtrust", "crowdstrike", "trend micro", "trendmicro", "sonicwall",

    # Hardware / compute
    "computer server", "computer servers", "server infrastructure", "server",
    "servers", "computer hardware", "it hardware", "computer", "computers",
    "laptop", "laptops", "workstation", "workstations", "mini pc", "desktop computer",
    "surface pro", "surface studio", "imac", "thinkpad", "data tape", "data cartridge",
    "nas drive", "backup storage", "memory card", "usb drive",

    # Network / telecom
    "network equipment", "network infrastructure", "network management", "network security",
    "internet connectivity", "network", "switch catalyst", "cisco switch", "aruba",
    "access point", "ethernet", "voip", "ip telephone", "ip telephony", "cisco webex",
    "webex", "avaya", "wireless active components", "lan passive components",

    # Data center / enterprise
    "data centre", "data center", "erp system", "erp solution", "crm system",
    "customer relationship management", "document management system", "cmdb",
    "asset tracking solution", "point of sale system", "attendance management system",

    # AI / data
    "artificial intelligence", "machine learning", "business intelligence", "data analytics",
    "data platform", "intelligent platform", "statistical data analysis",

    # Development / integration / recovery
    "api integration", "system integration", "systems integration", "system analysis",
    "system quality assurance", "developer outsourcing", "application developers",
    "business continuity", "disaster recovery", "it administration & support services",
    "bug fix", "health check and bug fix", "end user devices",

    # Specific technology terms seen in audited tenders
    "smart led pixel screen", "audio visual over ip", "avoip", "digital forensics",
    "3d mapping", "digital mapping", "interactive 3d", "rfid barcode reader",
    "billing & accounting solution", "billing and accounting solution", "cobit",
    "deep freeze", "autocad", "oracle license", "oracle licenses", "sql server",
    "smart net cisco", "f5 licence", "f5 license",

    # Arabic
    "تقنية المعلومات", "تكنولوجيا المعلومات", "خدمات تقنية المعلومات",
    "نظم المعلومات", "انظمة المعلومات", "برمجيات", "الامن السيبراني",
    "جدار حماية", "منصة الكترونية", "منصة رقمية", "خدمات الكترونية",
    "قواعد البيانات", "الخوادم", "خوادم", "اجهزة الحاسب", "اجهزة الحاسوب",
    "حاسب الي", "حواسيب", "لابتوب", "لابتوبات", "شبكات تقنية المعلومات",
    "بنية تحتية تقنية", "البنية التحتية التقنية", "البنية التحتية التكنولوجية",
    "الذكاء الاصطناعي", "مركز البيانات", "مراكز البيانات", "نظام نقاط البيع",
    "مكتب الدعم التقني", "لمكتب الدعم التقني", "محاكاة مراقبة الحركة الجوية",
    "صيانة وتطوير وتشغيل وضمان انظمة", "تطوير موقع الكتروني", "تصميم موقع الكتروني",
    "موقع الكتروني", "تطبيق ذكي", "تطوير نظام", "تطوير الانظمة",
    "رخص مايكروسوفت", "رخص اوراكل", "جدار حماية تطبيقات الويب",
    "تشفير قواعد البيانات", "الحوسبة السحابية", "نظام تخطيط موارد المؤسسات",
    "سويتشات", "محول شبكات", "شبكة", "سيرفر", "نظام الوصول المتميز",
]

# ============================================================
# HIGH-PRECISION NON-TECH RULES
# ============================================================
NON_TECH_PHRASES = [
    # Cleaning / facilities / transport
    "cleaning services", "cleaning and hospitality", "laundry services", "landscaping",
    "waste management", "security guards", "guarding services", "vehicle leasing",
    "purchase of vehicle", "supply of vehicles", "transportation services", "cargo van",

    # Physical security (project scope choice: not IT)
    "cctv", "surveillance camera", "surveillance cameras", "security camera",
    "security cameras", "ip camera", "security screening equipment", "security gate",
    "security gates", "access control system", "remote control security lock",

    # Fire / safety
    "fire alarm", "fire suppression", "firefighting", "fire fighting", "fire equipment",
    "fire cable", "radiation dose reader", "x-ray", "x ray",

    # Consumables / office / non-IT accessories
    "printer toner", "printer toners", "toner cartridge", "toner cartridges", "toner", "toners", "ink cartridge", "ink cartridges",
    "color ribbon", "printer ribbon", "card cleaner", "printable cd", "stationery", "office stationery", "paper cup",
    "forklift battery", "counterfeit currency detector",

    # Marketing / PR / events
    "public relations", "social media content", "marketing services", "marketing project",
    "weekly brochure", "brochure", "digital roll up", "design and montage", "communications agency services",
    "event management", "ceremony organization", "exhibition design and build",

    # Construction / MEP
    "road construction", "civil works", "building construction", "geotechnical",
    "materials testing", "air conditioning", "chiller maintenance", "underfloor heating",
    "electrical equipment", "generator maintenance", "substation maintenance",
    "water pumps", "pump spares", "pipes and fittings",

    # Medical / general supplies
    "medical consumables", "laboratory reagents", "laboratory chemicals", "ultrasound",
    "blood sugar meter", "support medical services", "furniture and fittings", "uniforms",
    "archival preservation", "archival storage materials", "tax consulting",
    "financial audit", "commemorative coins", "recruitment services",

    # Arabic
    "خدمات النظافة", "خدمات الحراسة", "الحراسة الامنية", "كاميرات مراقبة", "كاميرات مراقبه",
    "كاميرا مراقبة", "كاميرا مراقبه",
    "توريد كاميرات", "كميرات مراقبة", "كميرات مراقبه", "كميرا مراقبة", "كميرا مراقبه", "الكاميرات الامنية",
    "بوابات امنية", "بوابة امنية", "اجهزة الحريق", "معدات الحريق",
    "انذار الحريق", "كيابل نظام الحريق", "احبار طابعات", "احبار", "قرطاسية",
    "ادوات مكتبية", "مستلزمات مكتبية", "بطارية رافعة", "كاشف تزوير عملات",
    "البروشور الاسبوعي", "خدمات التسويق", "مشروع التسويق", "تصميم و مونتاج",
    "تصميم ومونتاج", "خدمات العلاقات العامة", "كاميرا كانون", "كاميره احترافيه",
    "كاميرا احترافية", "كاميرات حرارية", "توثيق الرصيد الارشيفي",
    "قطع غيار اجهزة مكتبية", "بحث واستقطاب الكفاءات", "ملحقات الاجهزه الاعلاميه",
    "ملحقات الأجهزة الإعلامية", "اعمال الطرق", "اعمال مدنية", "اجهزة التكييف",
    "مستهلكات طبية", "اجهزة المسح بالاشعة", "اجهزة اشعة", "معدات كهربائية",
    "عملات تذكارية", "برامج القيادات", "ادارة الموارد المالية",
    "مكتب دعم دافعي الضرائب", "تدفئة الارضية", "تدفئة الارضيه",
    "مخاطر الحريق والسلامة", "مخاطر الصحة المهنية", "استراتيجيات النقل",
    "الاستشارية للاعارة", "الاستشاريه للاعاره", "الموجات فوق الصوتية",
]

# ============================================================
# AMBIGUOUS / MIXED TITLES FOR PRE-FILTERED SOURCES
# ============================================================
SOURCE_REVIEW_EXACT = [
    "Extension",
    "الرياض",
    "General Maintenance Services Contract",
    "اجهزة",
    "أجهزة",
    "RFQ",
    "BATTERY PACK",
    "License",
    "Jaw_1.2.2",
    "Low Current for New Building",
    "Purchase Request | 21-April-2026 | IT Dep",
    "تجهيز غرفة اجتماعات",
    "Supply and Installation Service for Passive Materials",
    "الالتزام بالتشريعات الرقمية في الاستدامة المالية",
]

SOURCE_REVIEW_PHRASES = [
    # mixed technology + non-technology scope
    "كيابل نظام الحريق كيابل الصوتيات",
    "طابعة وسكانر وهواتف واحبار",
    "احبار و ذاكرة سيرفر",
    "احبار وذاكرة سيرفر",
    "كميرات وشبكات",
    "كاميرات وشبكات",
    "جهاز بصمه حضور",
    "جهاز بصمة حضور",
    "جهاز بصمه وجه",
    "جهاز بصمة وجه",
    "biometric attendance machine",
    "attendance management system",
    "fire equipment inventory labeller",
    "ادوات واجهزة واحتياجات قسم الابداع والتواصل الرقمي",

    # inherently unclear without detail
    "equipment and devices",
    "equipment and systems",
    "meeting room",
    "meeting rooms",
    "administration and support services",
]

# Minimal global review rules. Keep this intentionally small.
GLOBAL_REVIEW_PHRASES = [
    "supply installation commissioning validation training and maintenance of a",
    "administration and support services",
    "meeting room",
    "meeting rooms",
    "غرف الاجتماعات",
    "تجهيز غرف الاجتماعات",
]

# ============================================================
# TEXT HELPERS
# ============================================================
def normalize_text(value):
    text = str(value or "")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    for old, new in {
        "أ": "ا", "إ": "ا", "آ": "ا", "ى": "ي", "ؤ": "و", "ئ": "ي", "ـ": ""
    }.items():
        text = text.replace(old, new)
    text = text.casefold()
    text = re.sub(r"[_\-/\\|(),:;]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def get_title(record):
    for field in TITLE_FIELDS:
        value = record.get(field)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def flatten_value(value):
    if value is None:
        return []
    if isinstance(value, dict):
        out = []
        for item in value.values():
            out.extend(flatten_value(item))
        return out
    if isinstance(value, list):
        out = []
        for item in value:
            out.extend(flatten_value(item))
        return out
    text = str(value).strip()
    return [text] if text else []


def get_context_text(record):
    pieces = []
    title = get_title(record)
    if title:
        pieces.append(title)
    for field in CONTEXT_FIELDS:
        if field in record:
            pieces.extend(flatten_value(record.get(field)))
    raw_fields = record.get("Raw Fields")
    if isinstance(raw_fields, dict):
        pieces.extend(flatten_value(raw_fields))
    return " | ".join(pieces)


def find_phrase(text, phrases):
    normalized = normalize_text(text)
    padded = f" {normalized} "
    for phrase in phrases:
        p = normalize_text(phrase)
        if not p:
            continue
        if f" {p} " in padded:
            return phrase
    return None


def is_exact_review_title(title):
    normalized = normalize_text(title)
    return any(normalized == normalize_text(item) for item in SOURCE_REVIEW_EXACT)

# ============================================================
# MODEL
# ============================================================
def model_decision(model, text):
    try:
        score = float(model.decision_function([text])[0])
        if abs(score) < MODEL_REVIEW_MARGIN:
            return REVIEW, score
        return (TECH if score > 0 else NON_TECH), score
    except Exception:
        return int(model.predict([text])[0]), None

# ============================================================
# SOURCE-AWARE CLASSIFICATION
# ============================================================
def classify_prefiltered_source(record, model):
    """
    Etimad / Forsah / Oman already arrive from an IT-related source filter.
    We therefore use the source filter as a prior, but only AFTER checking:
      1) ambiguous/mixed cases -> Review
      2) explicit non-tech guards -> Non-Tech
      3) explicit tech evidence -> Tech
      4) residual case -> source prior (Tech)

    This avoids both previous failures:
      - blindly marking every source-filtered row as Technology
      - ignoring the source filter and letting ML reject most Forsah rows
    """
    title = get_title(record)

    # Ambiguous/mixed titles are reviewed before any hard decision.
    if is_exact_review_title(title):
        return REVIEW, "source_review", "ambiguous_exact_title", None

    mixed = find_phrase(title, SOURCE_REVIEW_PHRASES)
    if mixed:
        return REVIEW, "source_review", mixed, None

    # Explicit non-tech title evidence overrides the source prior.
    nontech = find_phrase(title, NON_TECH_PHRASES)
    if nontech:
        return NON_TECH, "source_guard", nontech, None

    # Explicit technology evidence confirms the source prior.
    tech = find_phrase(title, TECH_PHRASES)
    if tech:
        return TECH, "source_tech_rule", tech, None

    source = str(record.get("_source", "")).strip().casefold()

    # Extra structural checks for Etimad/Oman before trusting source prior.
    if source == "ksa_etimad":
        activity = normalize_text(record.get("Activity", ""))
        # If the page returned a clearly different activity, do not blindly accept it.
        if activity and normalize_text("تقنية المعلومات") not in activity:
            return REVIEW, "source_review", f"etimad_activity={record.get('Activity', '')}", None
        return TECH, "source_rule", "etimad_it_activity", None

    if source == "oman_it_tenderboard":
        category = normalize_text(record.get("المجال_والدرجة", ""))
        if normalize_text("خدمات تقنية المعلومات") in category:
            return TECH, "source_rule", "oman_it_category", None
        return REVIEW, "source_review", "oman_category_not_confirmed", None

    if source == "ksa_forsah":
        # Forsah category filter is useful but broad. The explicit guards above
        # remove known physical-security/consumable/non-tech cases. Residuals
        # keep the source prior instead of being rejected by a model trained on
        # a different source distribution.
        return TECH, "source_rule", "forsah_it_category_after_guards", None

    # Defensive fallback; normally unreachable.
    pred, score = model_decision(model, title)
    return pred, "model", "", score


def classify_record(record, model):
    title = get_title(record)
    context = get_context_text(record)
    source = str(record.get("_source", "")).strip().casefold()

    if not title and not context:
        return REVIEW, "missing_text", "", None

    if source in PREFILTERED_TECH_SOURCES:
        return classify_prefiltered_source(record, model)

    # For normal sources, title has priority. This avoids source metadata
    # contaminating the decision.
    tech_title = find_phrase(title, TECH_PHRASES)
    nontech_title = find_phrase(title, NON_TECH_PHRASES)

    if tech_title and nontech_title:
        return REVIEW, "rule_conflict", f"TECH={tech_title} | NONTECH={nontech_title}", None
    if tech_title:
        return TECH, "tech_rule", tech_title, None
    if nontech_title:
        return NON_TECH, "nontech_rule", nontech_title, None

    review_match = find_phrase(title, GLOBAL_REVIEW_PHRASES)
    if review_match:
        return REVIEW, "review_rule", review_match, None

    # Context is secondary and only used when title had no strong signal.
    tech_context = find_phrase(context, TECH_PHRASES)
    nontech_context = find_phrase(context, NON_TECH_PHRASES)

    if tech_context and nontech_context:
        return REVIEW, "rule_conflict", f"TECH={tech_context} | NONTECH={nontech_context}", None
    if tech_context:
        return TECH, "tech_rule", tech_context, None
    if nontech_context:
        return NON_TECH, "nontech_rule", nontech_context, None

    # ML sees the title first; context only fills a missing title.
    model_text = title or context
    prediction, score = model_decision(model, model_text)
    if prediction == REVIEW:
        return REVIEW, "model_review", "", score
    return prediction, "model", "", score

# ============================================================
# I/O / PIPELINE
# ============================================================
def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path.name} does not contain a JSON list")
    return data


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def discover_extractors():
    if not EXTRACTORS_DIR.exists():
        return []
    return [
        p for p in sorted(EXTRACTORS_DIR.glob("*.py"))
        if p.name != "__init__.py" and not p.name.startswith("_")
    ]


def run_extractor(extractor):
    print("\n" + "=" * 60)
    print("EXTRACTING:", extractor.name)
    print("=" * 60)
    result = subprocess.run([sys.executable, str(extractor)], cwd=PROJECT_ROOT)
    return result.returncode == 0


def filter_source(extractor, model):
    source_name = extractor.stem
    raw_file = RAW_DIR / f"{source_name}.json"
    filtered_file = FILTERED_DIR / f"{source_name}.json"
    review_file = REVIEW_DIR / f"{source_name}.json"

    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_file}")

    records = load_json(raw_file)
    technology_records = []
    review_records = []

    methods = {
        "source_rule": 0,
        "source_guard": 0,
        "source_tech_rule": 0,
        "source_review": 0,
        "tech_rule": 0,
        "nontech_rule": 0,
        "rule_conflict": 0,
        "review_rule": 0,
        "model": 0,
        "model_review": 0,
        "missing_text": 0,
    }

    counts = {"technology": 0, "review": 0, "nontechnology": 0, "missing_title": 0}

    for record in records:
        if not get_title(record):
            counts["missing_title"] += 1

        prediction, method, reason, score = classify_record(record, model)
        if method in methods:
            methods[method] += 1

        if prediction == TECH:
            technology_records.append(record)
            counts["technology"] += 1
        elif prediction == REVIEW:
            item = dict(record)
            item["_review_method"] = method
            item["_review_reason"] = reason
            if score is not None:
                item["_model_score"] = round(float(score), 6)
            review_records.append(item)
            counts["review"] += 1
        else:
            counts["nontechnology"] += 1

    save_json(filtered_file, technology_records)
    save_json(review_file, review_records)

    return {
        "source": source_name,
        "total": len(records),
        **counts,
        "methods": methods,
    }


def main():
    print("=" * 60)
    print("TENDER PIPELINE - HYBRID TECHNOLOGY FILTER V4 FINAL")
    print("=" * 60)

    if not MODEL_FILE.exists():
        raise FileNotFoundError(f"Model not found: {MODEL_FILE}")

    model = joblib.load(MODEL_FILE)
    extractors = discover_extractors()

    print(f"Model: {MODEL_FILE}")
    print(f"Review margin: {MODEL_REVIEW_MARGIN}")
    print(f"Extractors found: {len(extractors)}")
    for extractor in extractors:
        print(" -", extractor.name)

    summaries = []
    extraction_failed = []
    filtering_failed = []

    filter_only = "--filter-only" in sys.argv
    if filter_only:
        print("\nFILTER-ONLY mode: using existing raw JSON files; extractors will not run.")

    for extractor in extractors:
        if not filter_only:
            if not run_extractor(extractor):
                extraction_failed.append(extractor.name)
                print(f"WARNING: {extractor.name} extraction failed; trying existing raw JSON.")

        try:
            summaries.append(filter_source(extractor, model))
        except Exception as exc:
            filtering_failed.append(extractor.name)
            print(f"Filtering failed for {extractor.name}: {exc}")

    print("\n" + "=" * 60)
    print("HYBRID TECHNOLOGY FILTER V4 FINAL SUMMARY")
    print("=" * 60)

    total = {"total": 0, "technology": 0, "review": 0, "nontechnology": 0, "missing_title": 0}
    method_totals = collections_zero = {
        "source_rule": 0, "source_guard": 0, "source_tech_rule": 0, "source_review": 0,
        "tech_rule": 0, "nontech_rule": 0, "rule_conflict": 0, "review_rule": 0,
        "model": 0, "model_review": 0, "missing_text": 0,
    }

    for summary in summaries:
        print(
            f"{summary['source']}: {summary['technology']} Technology | "
            f"{summary['review']} Review | {summary['nontechnology']} Non-Tech | "
            f"{summary['total']} total"
        )
        for key in total:
            total[key] += summary[key]
        for key, value in summary["methods"].items():
            method_totals[key] += value

    print("\n" + "-" * 60)
    print(f"Total records:       {total['total']}")
    print(f"Technology:          {total['technology']}")
    print(f"Review:              {total['review']}")
    print(f"Non-Technology:      {total['nontechnology']}")
    print(f"Missing title:       {total['missing_title']}")

    print("\nDecision methods:")
    for key, label in [
        ("source_rule", "Source prior"),
        ("source_guard", "Source Non-Tech guards"),
        ("source_tech_rule", "Source Tech confirmations"),
        ("source_review", "Source ambiguous/review"),
        ("tech_rule", "Technology rules"),
        ("nontech_rule", "Non-Tech rules"),
        ("rule_conflict", "Rule conflicts"),
        ("review_rule", "Review rules"),
        ("model", "ML decisions"),
        ("model_review", "ML uncertain"),
    ]:
        print(f"{label + ':':28} {method_totals[key]}")

    if extraction_failed:
        print("\nExtraction failed:", ", ".join(extraction_failed))
    if filtering_failed:
        print("Filtering failed:", ", ".join(filtering_failed))

    print("\nTechnology files:", FILTERED_DIR)
    print("Review files:", REVIEW_DIR)
    print("=" * 60)
    print("Pipeline finished" + (" with warnings." if extraction_failed or filtering_failed else " successfully."))
    print("=" * 60)


if __name__ == "__main__":
    main()
