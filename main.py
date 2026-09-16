import json
import re
import subprocess
import sys
import unicodedata

from pathlib import Path

import joblib


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent

EXTRACTORS_DIR = PROJECT_ROOT / "extractors"

RAW_DIR = PROJECT_ROOT / "results" / "raw"

FILTERED_DIR = PROJECT_ROOT / "results" / "filtered"

REVIEW_DIR = PROJECT_ROOT / "results" / "review"

MODEL_FILE = (
    PROJECT_ROOT
    / "models"
    / "tech_classifier.joblib"
)


# ============================================================
# MODEL SETTINGS
# ============================================================

# LinearSVC يعطينا score:
#
# موجب = يميل Technology
# سالب = يميل Non-Technology
#
# إذا كان قريب من الصفر:
# الموديل غير واثق.
#
# بدل ما نخليه يخمن:
# نرسله Review.
MODEL_REVIEW_MARGIN = 0.15


# ============================================================
# LABELS
# ============================================================

NON_TECH = 0
TECH = 1
REVIEW = 2


# ============================================================
# TITLE FIELDS
# ============================================================

TITLE_FIELDS = [
    "Tender Subject",
    "Tender Title",
    "Title",
    "title",
    "Subject",
    "subject",
    "tender_title",
    "tender_subject",

    "موضوع المناقصة",
    "اسم المناقصة",

    # Oman
    "عنوان_المناقصة",
]


# ============================================================
# CONTEXT FIELDS
#
# القواعد تستطيع استخدام هذه الحقول.
#
# الموديل نفسه يظل يستخدم Title
# لأنه تدرب على العناوين.
# ============================================================

CONTEXT_FIELDS = [

    # English
    "Description",
    "description",
    "Tender Description",
    "tender_description",

    "Scope",
    "scope",

    "Category",
    "category",

    "Classification",
    "classification",

    "Tender Type",
    "Negotiation Type",

    # Arabic
    "المجال_والدرجة",
    "المجال والدرجة",

    "نوع المناقصة",

    "التصنيف",

    "الوصف",
    "وصف المناقصة",

    "نطاق العمل",
]


# ============================================================
# STRONG TECHNOLOGY PHRASES
# ============================================================

TECH_PHRASES = [

    # ========================================================
    # GENERAL IT
    # ========================================================

    "information technology",
    "it services",
    "it support",

    "helpdesk",
    "help desk",

    "information systems",
    "information system",

    "software",
    "software license",
    "software licenses",
    "software licensing",

    "web application",
    "web applications",

    "website development",
    "website maintenance",

    "digital platform",
    "electronic platform",
    "online platform",

    "digital services",
    "electronic services",

    "database",
    "databases",


    # ========================================================
    # CLOUD
    # ========================================================

    "cloud services",
    "cloud service",

    "cloud platform",

    "cloud infrastructure",

    "cloud based platform",
    "cloud-based platform",

    "cloud solution",
    "cloud solutions",

    "azure",


    # ========================================================
    # MICROSOFT
    # ========================================================

    "microsoft 365",
    "office 365",

    "microsoft license",
    "microsoft licenses",

    "microsoft licensing",


    # ========================================================
    # CYBER SECURITY
    # ========================================================

    "cybersecurity",
    "cyber security",

    "firewall",
    "firewalls",

    "security analytics platform",

    "application security",

    "application security testing",

    "penetration testing",

    "threat intelligence",

    "security orchestration",

    "privileged access",

    "privileged access management",

    "pam license",
    "pam licenses",

    "identity services engine",

    "crowdstrike",

    "trendmicro",
    "trend micro",

    "beyondtrust",


    # ========================================================
    # SERVERS / HARDWARE
    # ========================================================

    "computer server",
    "computer servers",

    "server infrastructure",

    "servers",

    "desktop computer",
    "desktop computers",

    "computer hardware",

    "computers",

    "laptop",
    "laptops",

    "workstation",
    "workstations",

    "small form factor pc",
    "small form factor pcs",

    "sff pc",
    "sff pcs",

    "it hardware",


    # ========================================================
    # NETWORK
    # ========================================================

    "network equipment",

    "network infrastructure",

    "network management",

    "network access",

    "internet connectivity",

    "network security",


    # ========================================================
    # DATA CENTER
    # ========================================================

    "data centre infrastructure",
    "data center infrastructure",

    "data centre maintenance",
    "data center maintenance",

    "data centre support",
    "data center support",


    # ========================================================
    # COMMUNICATION / VOIP
    # ========================================================

    "voip",

    "ip telephony",

    "cloud ip telephony",

    "cisco call manager",

    "cisco webex",

    "webex subscription",


    # ========================================================
    # AUDIO VISUAL OVER IP
    # ========================================================

    "audio visual over ip",

    "avoip",

    "audio visual ip",


    # ========================================================
    # ENTERPRISE SYSTEMS
    # ========================================================

    "erp system",
    "erp solution",

    "crm system",

    "customer relationship management system",

    "configuration management database",

    "cmdb",

    "asset management system",

    "document management system",


    # ========================================================
    # AI / DATA
    # ========================================================

    "artificial intelligence",

    "agentic ai",

    "machine learning",

    "business intelligence",

    "data analytics",

    "data platform",

    "intelligent platform",


    # ========================================================
    # DIGITAL FORENSICS
    # ========================================================

    "digital forensics",

    "digital forensic",


    # ========================================================
    # DIGITAL / 3D
    # ========================================================

    "3d mapping",

    "digital mapping",

    "interactive 3d",

    "digital game",

    "audio game",


    # ========================================================
    # SYSTEM INTEGRATION
    # ========================================================

    "api integration",

    "system integration",

    "systems integration",


    # ========================================================
    # KNOWN PRODUCTS / SERVICES
    # ========================================================

    "appdynamics",

    "aruba amc",

    "telecontrol hardware",

    "talent network solution",


    # ========================================================
    # ARABIC IT
    # ========================================================

    "تقنية المعلومات",

    "تكنولوجيا المعلومات",

    "خدمات تقنية المعلومات",

    "نظم المعلومات",

    "انظمة المعلومات",

    "الأمن السيبراني",

    "الامن السيبراني",

    "جدار الحماية",

    "جدار ناري",

    "منصة إلكترونية",

    "منصة الكترونية",

    "منصة رقمية",

    "خدمات إلكترونية",

    "خدمات الكترونية",

    "تطبيقات إلكترونية",

    "تطبيقات الكترونية",

    "برمجيات",

    "الخوادم",

    "خوادم",

    "أجهزة الحاسب",

    "اجهزة الحاسب",

    "أجهزة الحاسوب",

    "اجهزة الحاسوب",

    "الحواسيب المحمولة",

    "شبكات تقنية المعلومات",

    "قواعد البيانات",

    "الذكاء الاصطناعي",

    "اختبار أمن التطبيقات",

    "اختبار امن التطبيقات",

    "إدارة الوصول المتميز",

    "ادارة الوصول المتميز",

    "الاتصالات الهاتفية السحابية",

    "مركز البيانات",

    "مراكز البيانات",
]


# ============================================================
# STRONG NON-TECH PHRASES
#
# هنا نستخدم عبارات واضحة فقط.
#
# لا نستخدم:
# maintenance
# system
# support
# technical
#
# لأنها ممكن تكون تقنية.
# ============================================================

NON_TECH_PHRASES = [

    # ========================================================
    # CLEANING / FACILITY
    # ========================================================

    "cleaning services",

    "cleaning and hospitality",

    "hospitality services",

    "laundry services",

    "landscaping services",

    "landscaping",

    "waste management services",

    "waste collection",

    "waste disposal",


    # ========================================================
    # VEHICLES / TRANSPORT
    # ========================================================

    "executive vehicle",

    "purchase of vehicle",

    "supply of vehicles",

    "vehicle leasing",

    "transportation services",

    "public transportation",

    "pickup car",

    "cargo van",


    # ========================================================
    # INSURANCE
    # ========================================================

    "health insurance",

    "medical insurance",

    "travel insurance",

    "life insurance",


    # ========================================================
    # GUARDS
    # ========================================================

    "security guards",

    "security guard",

    "guarding services",


    # ========================================================
    # PHYSICAL SECURITY
    #
    # حسب Scope المشروع الحالي
    # ========================================================

    "cctv cameras",

    "cctv system",

    "access control systems",

    "access control system",

    "security screening equipment",


    # ========================================================
    # EVENTS / PR
    # ========================================================

    "event management services",

    "ceremony organization",

    "award ceremony",

    "national day celebration",

    "pyrotechnics",

    "social media content creation",

    "communications agency services",

    "public relations services",

    "campaign key visuals",

    "exhibition design and build",


    # ========================================================
    # CONSTRUCTION
    # ========================================================

    "road construction",

    "road infrastructure",

    "civil works",

    "civil discipline",

    "waterproofing works",

    "land restoration",

    "building construction",

    "geotechnical investigation",

    "materials testing services",


    # ========================================================
    # HVAC / MECHANICAL
    # ========================================================

    "air conditioning",

    "air-conditioning",

    "split ac",

    "refrigerators",

    "chiller maintenance",

    "water chiller",

    "underfloor heating",


    # ========================================================
    # ELECTRICAL
    # ========================================================

    "switchgear panel",

    "electrical and lighting works",

    "diesel engine generators",

    "generator maintenance",

    "substation maintenance",

    "substation spare",

    "substation spare-parts",

    "low voltage electrical",

    "electrical equipment",

    "ups system",

    "ups systems",

    "uninterruptible power supply",


    # ========================================================
    # PUMPS / VALVES / PIPES
    # ========================================================

    "water pumps",

    "oil pumps",

    "pump spares",

    "valve plug",

    "valve relief",

    "pipes and fittings",

    "piping gaskets",

    "water fittings",


    # ========================================================
    # MEDICAL
    # ========================================================

    "medical consumables",

    "laboratory reagents",

    "laboratory chemicals",

    "medical gas system",

    "ultrasound scanner",

    "ivf consumables",

    "eye wash stations",

    "blood sugar meter",

    "support medical services",


    # ========================================================
    # FIRE PHYSICAL SYSTEMS
    # ========================================================

    "fire suppression system",

    "firefighting systems",

    "fire alarm equipment",


    # ========================================================
    # ARCHIVAL
    # ========================================================

    "archival preservation",

    "archival storage materials",


    # ========================================================
    # GENERAL SUPPLIES
    # ========================================================

    "uniforms",

    "uniforms and formal shoes",

    "furniture and fittings",

    "savoury snacks",

    "paper cup",


    # ========================================================
    # FINANCE / CONSULTING
    # ========================================================

    "tax consulting",

    "tax advisory",

    "financial audit services",

    "external audit services",


    # ========================================================
    # COINS
    # ========================================================

    "commemorative coins",


    # ========================================================
    # TRAINING MATERIALS
    # ========================================================

    "training bags",

    "training materials",


    # ========================================================
    # ARABIC NON-TECH
    # ========================================================

    "خدمات النظافة",

    "النظافة والضيافة",

    "خدمات الحراسة",

    "الحراسة الأمنية",

    "الحراسة الامنية",

    "شراء مركبة",

    "توريد مركبات",

    "خدمات المواصلات",

    "خدمات النقل",

    "التأمين الصحي",

    "التامين الصحي",

    "أعمال الطرق",

    "اعمال الطرق",

    "أعمال مدنية",

    "اعمال مدنية",

    "أجهزة التكييف",

    "اجهزة التكييف",

    "التكييف والتبريد",

    "مستهلكات طبية",

    "كواشف مخبرية",

    "خدمات التدقيق الخارجي",

    "الاستشارات الضريبية",

    "احتفالية عيد الاتحاد",

    "العشب الصناعي",

    "المصاعد والسلالم الكهربائية",

    "عملات تذكارية",

    "معدات كهربائية",

    "اجهزة ومعدات كهربائية",

    "أجهزة ومعدات كهربائية",

    "خدمات الحراسة الامنية",

    "خدمات الحراسة الأمنية",

    "تجديد البلاط",

    "خدمات وتجهيزات وإدارة فعاليات",

    "خدمات وتجهيزات وادارة فعاليات",
]


# ============================================================
# REVIEW PHRASES
#
# هذه ليست Tech ولا Non-Tech.
#
# هذه فقط تقول:
# العنوان غامض ويحتاج معلومات أكثر.
# ============================================================

REVIEW_PHRASES = [
    "equipment and devices",
    "equipment and systems",
    "meeting rooms",
    "meeting room",
    "administration and support services",
    "administration & support services",
    "supply installation commissioning validation training and maintenance",

    "غرف الاجتماعات",
    "تجهيز غرف الاجتماعات",
]


# ============================================================
# NORMALIZE TEXT
# ============================================================

def normalize_text(value):

    text = str(
        value or ""
    )


    # --------------------------------------------------------
    # Unicode normalization
    # --------------------------------------------------------

    text = unicodedata.normalize(
        "NFKD",
        text
    )


    text = "".join(
        char
        for char in text
        if not unicodedata.combining(
            char
        )
    )


    # --------------------------------------------------------
    # Arabic normalization
    # --------------------------------------------------------

    replacements = {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
        "ؤ": "و",
        "ئ": "ي",
        "ـ": "",
    }


    for old, new in replacements.items():

        text = text.replace(
            old,
            new
        )


    # --------------------------------------------------------
    # Lowercase
    # --------------------------------------------------------

    text = text.casefold()


    # --------------------------------------------------------
    # Replace punctuation
    # --------------------------------------------------------

    text = re.sub(
        r"[_\-/\\|(),:;]+",
        " ",
        text
    )


    # --------------------------------------------------------
    # Remove extra spaces
    # --------------------------------------------------------

    text = re.sub(
        r"\s+",
        " ",
        text
    )


    return text.strip()


# ============================================================
# GET TITLE
# ============================================================

def get_title(record):

    for field in TITLE_FIELDS:

        value = record.get(
            field
        )


        if value:

            text = str(
                value
            ).strip()


            if text:

                return text


    return ""


# ============================================================
# FLATTEN VALUES
# ============================================================

def flatten_value(value):

    if value is None:

        return []


    if isinstance(
        value,
        dict
    ):

        output = []


        for item in value.values():

            output.extend(
                flatten_value(
                    item
                )
            )


        return output


    if isinstance(
        value,
        list
    ):

        output = []


        for item in value:

            output.extend(
                flatten_value(
                    item
                )
            )


        return output


    text = str(
        value
    ).strip()


    if text:

        return [text]


    return []


# ============================================================
# GET CONTEXT
# ============================================================

def get_context_text(record):

    pieces = []


    # --------------------------------------------------------
    # Title
    # --------------------------------------------------------

    title = get_title(
        record
    )


    if title:

        pieces.append(
            title
        )


    # --------------------------------------------------------
    # Known fields
    # --------------------------------------------------------

    for field in CONTEXT_FIELDS:

        if field in record:

            pieces.extend(
                flatten_value(
                    record.get(
                        field
                    )
                )
            )


    # --------------------------------------------------------
    # Qatar Foundation / generic Raw Fields
    # --------------------------------------------------------

    raw_fields = record.get(
        "Raw Fields"
    )


    if isinstance(
        raw_fields,
        dict
    ):

        pieces.extend(
            flatten_value(
                raw_fields
            )
        )


    return " | ".join(
        pieces
    )


# ============================================================
# PHRASE MATCH
# ============================================================

def find_phrase(
    text,
    phrases
):

    normalized_text = normalize_text(
        text
    )


    padded_text = (
        " "
        +
        normalized_text
        +
        " "
    )


    for phrase in phrases:

        normalized_phrase = normalize_text(
            phrase
        )


        if not normalized_phrase:

            continue


        padded_phrase = (
            " "
            +
            normalized_phrase
            +
            " "
        )


        if padded_phrase in padded_text:

            return phrase


    return None


# ============================================================
# SOURCE-SPECIFIC RULES
# ============================================================

 
def source_rule(record):

    # ========================================================
    # OMAN
    #
    # Oman extractor نفسه أصلاً يفلتر فقط:
    # المجال_والدرجة = خدمات تقنية المعلومات
    #
    # لذلك إذا السجل جاي من هذا المصدر
    # ومعه التصنيف الصحيح:
    # نعتبره Technology مباشرة.
    # ========================================================

    source = str(
        record.get(
            "_source",
            ""
        )
    ).strip().casefold()


    oman_category = normalize_text(
        record.get(
            "المجال_والدرجة",
            ""
        )
    )


    if (
        source == "oman_it_tenderboard"
        and
        "خدمات تقنية المعلومات"
        in oman_category
    ):

        return (
            TECH,
            "oman_it_category"
        )


    return (
        None,
        None
    )
    


# ============================================================
# MODEL DECISION
# ============================================================

def model_decision(
    model,
    text
):

    # --------------------------------------------------------
    # LinearSVC Decision Score
    # --------------------------------------------------------

    try:

        score = float(
            model.decision_function(
                [text]
            )[0]
        )


        # ----------------------------------------------------
        # Uncertain
        # ----------------------------------------------------

        if abs(
            score
        ) < MODEL_REVIEW_MARGIN:

            return (
                REVIEW,
                score
            )


        # ----------------------------------------------------
        # Technology
        # ----------------------------------------------------

        if score > 0:

            return (
                TECH,
                score
            )


        # ----------------------------------------------------
        # Non-Tech
        # ----------------------------------------------------

        return (
            NON_TECH,
            score
        )


    except Exception:

        # Fallback
        prediction = int(
            model.predict(
                [text]
            )[0]
        )


        return (
            prediction,
            None
        )


# ============================================================
# HYBRID CLASSIFIER
# ============================================================

def classify_record(
    record,
    model
):

    title = get_title(
        record
    )


    context = get_context_text(
        record
    )


    # ========================================================
    # STEP 1
    # SOURCE RULE
    # ========================================================

    (
        source_prediction,
        source_reason
    ) = source_rule(
        record
    )


    if (
        source_prediction
        is not None
    ):

        return (
            source_prediction,
            "source_rule",
            source_reason,
            None
        )


    # ========================================================
    # STEP 2
    # STRONG TECH RULE
    # ========================================================

    tech_match = find_phrase(
        context,
        TECH_PHRASES
    )


    # ========================================================
    # STEP 3
    # STRONG NON-TECH RULE
    # ========================================================

    nontech_match = find_phrase(
        context,
        NON_TECH_PHRASES
    )


    # ========================================================
    # TECH ONLY
    # ========================================================

    if (
        tech_match
        and
        not nontech_match
    ):

        return (
            TECH,
            "tech_rule",
            tech_match,
            None
        )


    # ========================================================
    # NON-TECH ONLY
    # ========================================================

    if (
        nontech_match
        and
        not tech_match
    ):

        return (
            NON_TECH,
            "nontech_rule",
            nontech_match,
            None
        )


    # ========================================================
    # CONFLICT
    #
    # Tech + Non-Tech في نفس السجل
    #
    # لا نخمن.
    # ========================================================

    if (
        tech_match
        and
        nontech_match
    ):

        return (
            REVIEW,
            "rule_conflict",
            (
                f"TECH={tech_match} | "
                f"NONTECH={nontech_match}"
            ),
            None
        )


    # ========================================================
    # STEP 4
    # REVIEW PHRASE
    # ========================================================

    review_match = find_phrase(
        context,
        REVIEW_PHRASES
    )


    if review_match:

        return (
            REVIEW,
            "review_rule",
            review_match,
            None
        )


    # ========================================================
    # STEP 5
    # ML MODEL
    #
    # نخلي الموديل يقرأ Title فقط
    # لأنه تدرب بهذه الطريقة.
    # ========================================================

    model_text = title


    if not model_text:

        model_text = context


    # ========================================================
    # NO TEXT
    # ========================================================

    if not model_text:

        return (
            REVIEW,
            "missing_text",
            "",
            None
        )


    (
        prediction,
        score
    ) = model_decision(
        model,
        model_text
    )


    # ========================================================
    # MODEL UNCERTAIN
    # ========================================================

    if prediction == REVIEW:

        return (
            REVIEW,
            "model_review",
            "",
            score
        )


    # ========================================================
    # MODEL DECISION
    # ========================================================

    return (
        prediction,
        "model",
        "",
        score
    )


# ============================================================
# LOAD JSON
# ============================================================

def load_json(path):

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as file:

        data = json.load(
            file
        )


    if not isinstance(
        data,
        list
    ):

        raise ValueError(
            f"{path.name} "
            "does not contain a JSON list."
        )


    return data


# ============================================================
# SAVE JSON
# ============================================================

def save_json(
    path,
    data
):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )


    with open(
        path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2
        )


# ============================================================
# DISCOVER EXTRACTORS
# ============================================================

def discover_extractors():

    if not EXTRACTORS_DIR.exists():

        return []


    extractors = []


    for path in sorted(
        EXTRACTORS_DIR.glob(
            "*.py"
        )
    ):

        if (
            path.name
            ==
            "__init__.py"
        ):

            continue


        if path.name.startswith(
            "_"
        ):

            continue


        extractors.append(
            path
        )


    return extractors


# ============================================================
# RUN EXTRACTOR
# ============================================================

def run_extractor(
    extractor
):

    print(
        "\n"
        +
        "=" * 60
    )


    print(
        "EXTRACTING:",
        extractor.name
    )


    print(
        "=" * 60
    )


    result = subprocess.run(

        [
            sys.executable,
            str(
                extractor
            )
        ],

        cwd=PROJECT_ROOT
    )


    return (
        result.returncode
        ==
        0
    )


# ============================================================
# FILTER ONE SOURCE
# ============================================================

def filter_source(
    extractor,
    model
):

    source_name = (
        extractor.stem
    )


    raw_file = (
        RAW_DIR
        /
        f"{source_name}.json"
    )


    filtered_file = (
        FILTERED_DIR
        /
        f"{source_name}.json"
    )


    review_file = (
        REVIEW_DIR
        /
        f"{source_name}.json"
    )


    # ========================================================
    # RAW FILE CHECK
    # ========================================================

    if not raw_file.exists():

        raise FileNotFoundError(
            f"Raw file not found: "
            f"{raw_file}"
        )


    records = load_json(
        raw_file
    )


    # ========================================================
    # RESULTS
    # ========================================================

    technology_records = []

    review_records = []


    technology_count = 0

    nontechnology_count = 0

    review_count = 0

    missing_title = 0


    # ========================================================
    # METHOD COUNTS
    # ========================================================

    method_counts = {

        "source_rule": 0,

        "tech_rule": 0,

        "nontech_rule": 0,

        "rule_conflict": 0,

        "review_rule": 0,

        "model": 0,

        "model_review": 0,

        "missing_text": 0,
    }


    # ========================================================
    # CLASSIFY RECORDS
    # ========================================================

    for record in records:

        title = get_title(
            record
        )


        if not title:

            missing_title += 1


        (
            prediction,
            method,
            reason,
            score
        ) = classify_record(
            record,
            model
        )


        if method in method_counts:

            method_counts[
                method
            ] += 1


        # ====================================================
        # TECHNOLOGY
        # ====================================================

        if prediction == TECH:

            technology_records.append(
                record
            )

            technology_count += 1


        # ====================================================
        # REVIEW
        # ====================================================

        elif prediction == REVIEW:

            review_item = dict(
                record
            )


            review_item[
                "_review_reason"
            ] = reason


            review_item[
                "_review_method"
            ] = method


            if score is not None:

                review_item[
                    "_model_score"
                ] = round(
                    float(score),
                    6
                )


            review_records.append(
                review_item
            )


            review_count += 1


        # ====================================================
        # NON-TECH
        # ====================================================

        else:

            nontechnology_count += 1


    # ========================================================
    # SAVE TECHNOLOGY ONLY
    #
    # هذا اللي بيروح Azure لاحقًا.
    # ========================================================

    save_json(
        filtered_file,
        technology_records
    )


    # ========================================================
    # SAVE REVIEW
    #
    # لا يروح Azure.
    # نراجعه يدويًا.
    # ========================================================

    save_json(
        review_file,
        review_records
    )


    # ========================================================
    # SUMMARY
    # ========================================================

    return {

        "source":
            source_name,

        "total":
            len(
                records
            ),

        "technology":
            technology_count,

        "nontechnology":
            nontechnology_count,

        "review":
            review_count,

        "missing_title":
            missing_title,

        "methods":
            method_counts,
    }


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "=" * 60
    )


    print(
        "TENDER PIPELINE"
    )


    print(
        "HYBRID TECHNOLOGY FILTER V2"
    )


    print(
        "=" * 60
    )


    # ========================================================
    # LOAD MODEL
    # ========================================================

    if not MODEL_FILE.exists():

        print(
            "\nERROR:"
        )


        print(
            "Model not found:"
        )


        print(
            MODEL_FILE
        )


        sys.exit(
            1
        )


    model = joblib.load(
        MODEL_FILE
    )


    print(
        "\nModel loaded:"
    )


    print(
        MODEL_FILE
    )


    print(
        "\nModel review margin:",
        MODEL_REVIEW_MARGIN
    )


    # ========================================================
    # FIND EXTRACTORS
    # ========================================================

    extractors = (
        discover_extractors()
    )


    if not extractors:

        print(
            "\nNo extractors found."
        )

        return


    print(
        "\nExtractors found:",
        len(
            extractors
        )
    )


    for extractor in extractors:

        print(
            " -",
            extractor.name
        )


    # ========================================================
    # RESULTS
    # ========================================================

    summaries = []

    extraction_failed = []

    filtering_failed = []


    # ========================================================
    # RUN SOURCES
    # ========================================================

    for extractor in extractors:

        extraction_ok = (
            run_extractor(
                extractor
            )
        )


        if not extraction_ok:

            extraction_failed.append(
                extractor.name
            )


            print(
                f"\nWARNING: "
                f"{extractor.name} "
                "extraction failed."
            )


            print(
                "If an old raw JSON exists, "
                "the pipeline will still try "
                "to filter it."
            )


        try:

            summary = filter_source(
                extractor,
                model
            )


            summaries.append(
                summary
            )


        except Exception as error:

            filtering_failed.append(
                extractor.name
            )


            print(
                f"\nFiltering failed for "
                f"{extractor.name}:"
            )


            print(
                error
            )


    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    print(
        "\n"
        +
        "=" * 60
    )


    print(
        "HYBRID TECHNOLOGY FILTER V2 SUMMARY"
    )


    print(
        "=" * 60
    )


    total_records = 0

    total_technology = 0

    total_nontechnology = 0

    total_review = 0

    total_missing = 0


    total_methods = {

        "source_rule": 0,

        "tech_rule": 0,

        "nontech_rule": 0,

        "rule_conflict": 0,

        "review_rule": 0,

        "model": 0,

        "model_review": 0,

        "missing_text": 0,
    }


    for summary in summaries:

        print(
            f"{summary['source']}: "
            f"{summary['technology']} Technology "
            f"| {summary['review']} Review "
            f"| {summary['nontechnology']} Non-Tech "
            f"| {summary['total']} total"
        )


        total_records += (
            summary[
                "total"
            ]
        )


        total_technology += (
            summary[
                "technology"
            ]
        )


        total_nontechnology += (
            summary[
                "nontechnology"
            ]
        )


        total_review += (
            summary[
                "review"
            ]
        )


        total_missing += (
            summary[
                "missing_title"
            ]
        )


        for method, count in (
            summary[
                "methods"
            ].items()
        ):

            total_methods[
                method
            ] += count


    # ========================================================
    # TOTALS
    # ========================================================

    print(
        "\n"
        +
        "-" * 60
    )


    print(
        f"Total records:       "
        f"{total_records}"
    )


    print(
        f"Technology:          "
        f"{total_technology}"
    )


    print(
        f"Review:              "
        f"{total_review}"
    )


    print(
        f"Non-Technology:      "
        f"{total_nontechnology}"
    )


    print(
        f"Missing title:       "
        f"{total_missing}"
    )


    # ========================================================
    # METHODS
    # ========================================================

    print(
        "\nDecision methods:"
    )


    print(
        f"Source rules:        "
        f"{total_methods['source_rule']}"
    )


    print(
        f"Technology rules:    "
        f"{total_methods['tech_rule']}"
    )


    print(
        f"Non-Tech rules:      "
        f"{total_methods['nontech_rule']}"
    )


    print(
        f"Rule conflicts:      "
        f"{total_methods['rule_conflict']}"
    )


    print(
        f"Review rules:        "
        f"{total_methods['review_rule']}"
    )


    print(
        f"ML decisions:        "
        f"{total_methods['model']}"
    )


    print(
        f"ML uncertain:        "
        f"{total_methods['model_review']}"
    )


    # ========================================================
    # OUTPUT LOCATIONS
    # ========================================================

    print(
        "\nTechnology files:"
    )


    print(
        FILTERED_DIR
    )


    print(
        "\nReview files:"
    )


    print(
        REVIEW_DIR
    )


    # ========================================================
    # FAILURES
    # ========================================================

    if extraction_failed:

        print(
            "\nExtraction failed:"
        )


        for name in extraction_failed:

            print(
                " -",
                name
            )


    if filtering_failed:

        print(
            "\nFiltering failed:"
        )


        for name in filtering_failed:

            print(
                " -",
                name
            )


    # ========================================================
    # FINISH
    # ========================================================

    print(
        "\n"
        +
        "=" * 60
    )


    if (
        extraction_failed
        or
        filtering_failed
    ):

        print(
            "Pipeline finished, "
            "but some steps failed."
        )


    else:

        print(
            "Pipeline finished successfully."
        )


    print(
        "=" * 60
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()