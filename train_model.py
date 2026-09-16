import csv
import json
import random
import re
import unicodedata

from collections import Counter
from pathlib import Path

import joblib

from sklearn.feature_extraction.text import (
    TfidfVectorizer,
)

from sklearn.pipeline import (
    FeatureUnion,
    Pipeline,
)

from sklearn.svm import LinearSVC

from sklearn.model_selection import (
    train_test_split,
)

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent

TRAINING_FILE = (
    PROJECT_ROOT
    / "training_data.csv"
)

MODEL_DIR = (
    PROJECT_ROOT
    / "models"
)

MODEL_FILE = (
    MODEL_DIR
    / "tech_classifier.joblib"
)

RAW_DIR = (
    PROJECT_ROOT
    / "results"
    / "raw"
)

SOURCE_FILES = [
    RAW_DIR / "etimad.json",
    RAW_DIR / "forsah.json",
]


# ============================================================
# SETTINGS
# ============================================================

RANDOM_STATE = 42

SOURCE_TECH_CONFIDENCE = 0.85

MAX_SOURCE_PER_CLASS = 60


# ============================================================
# TITLE FIELDS
# ============================================================

TITLE_FIELDS = [
    "text",
    "Text",
    "title",
    "Title",

    "Tender Title",
    "Tender Subject",
    "Tender Name",

    "Subject",
    "Name",

    "Competition Name",
    "Competition Title",

    "Opportunity Name",
    "Opportunity Title",

    "اسم المناقصة",
    "عنوان المناقصة",
    "موضوع المناقصة",

    "اسم المنافسة",
    "عنوان المنافسة",

    "اسم الفرصة",

    "عنوان_المناقصة",
]


# ============================================================
# VERY STRONG TECH PHRASES
#
# هذه لا نستخدمها لتصنيف كل شيء.
# فقط لاختيار أمثلة تدريب جديدة عالية الثقة.
# ============================================================

SOURCE_TECH_PHRASES = [

    "information technology",
    "it services",
    "it support",
    "it infrastructure",

    "software",
    "software license",
    "software licenses",

    "cybersecurity",
    "cyber security",

    "firewall",

    "cloud platform",
    "cloud services",
    "cloud infrastructure",

    "microsoft 365",
    "office 365",

    "computer servers",
    "server infrastructure",

    "network infrastructure",
    "network equipment",
    "network security",

    "data centre",
    "data center",

    "web application",
    "website development",

    "database",

    "erp system",
    "crm system",

    "cisco webex",

    "point of sale system",

    "system analysis",
    "system quality assurance",

    "application developers",
    "developer outsourcing",

    "asset tracking solution",

    "disaster recovery",

    "business continuity",

    "digital platform",

    "electronic platform",

    "artificial intelligence",

    "machine learning",

    "data analytics",

    "تقنية المعلومات",
    "تكنولوجيا المعلومات",

    "خدمات تقنية المعلومات",

    "نظم المعلومات",
    "أنظمة المعلومات",
    "انظمة المعلومات",

    "برمجيات",

    "الأمن السيبراني",
    "الامن السيبراني",

    "منصة رقمية",

    "منصة إلكترونية",
    "منصة الكترونية",

    "قواعد البيانات",

    "الخوادم",

    "شبكات تقنية المعلومات",

    "الذكاء الاصطناعي",

    "تطوير نظام",
    "تطوير الأنظمة",
    "تطوير الانظمة",
]


# ============================================================
# VERY STRONG NON-TECH PHRASES
# ============================================================

SOURCE_NONTECH_PHRASES = [

    "cleaning services",
    "cleaning",

    "landscaping",

    "waste management",

    "security guards",

    "cctv",
    "cctv system",
    "cctv cameras",

    "ip camera",
    "security camera",

    "printer toner",
    "toner cartridge",
    "toner",

    "ink cartridge",
    "ink cartridges",

    "forklift battery",

    "radiation dose reader",

    "x-ray",

    "x ray",

    "blood sugar meter",

    "medical consumables",

    "furniture",

    "uniforms",

    "stationery",

    "public relations",

    "social media content",

    "marketing services",

    "transportation services",

    "vehicle",

    "fire alarm",

    "firefighting",

    "fire fighting",

    "air conditioning",

    "generator maintenance",

    "water pump",

    "electrical equipment",

    "leadership program",
    "leadership programmes",

    "كاميرات مراقبة",

    "كاميرا مراقبة",

    "بوابات أمنية",
    "بوابات امنية",

    "أحبار طابعات",
    "احبار طابعات",

    "أحبار",
    "احبار",

    "قرطاسية",

    "بطارية رافعة",

    "جهاز كاشف تزوير عملات",

    "خدمات التسويق",

    "العلاقات العامة",

    "مستهلكات طبية",

    "أجهزة أشعة",
    "اجهزة اشعة",

    "أجهزة الحريق",
    "اجهزة الحريق",

    "إنذار الحريق",
    "انذار الحريق",

    "خدمات النظافة",
]


# ============================================================
# HARD EXAMPLES
#
# هذه أمثلة قليلة جدًا لحماية الموديل من الأخطاء
# التي ظهرت فعليًا عندنا.
# ============================================================

HARD_TECH = [

    "Microsoft Cloud Services",

    "Cybersecurity Platform",

    "Development of Government Website",

    "Artificial Intelligence Platform",

    "Supply of Computers and Servers",

    "Cisco Network Infrastructure",

    "Oracle Software License Renewal",

    "ERP System Support",

    "Application Developer Outsourcing",

    "IT Business Continuity and Disaster Recovery Consultancy",

    "System Analysis and System Quality Assurance Services",

    "Asset Tracking Solution with Tagging Services",

    "Enterprise Schools Operating System",

    "Point of Sale System Installation and Support",

    "تطوير وتشغيل وصيانة الأنظمة الإلكترونية",

    "تحديث البنية التحتية التقنية",

    "دعم أنظمة تقنية المعلومات",
]


HARD_NONTECH = [

    "Cleaning Services",

    "Water Pumps",

    "Road Construction",

    "Air Conditioning Maintenance",

    "Medical Consumables",

    "CCTV Cameras",

    "Printer Toner",

    "Ink Cartridges",

    "Forklift Battery",

    "Furniture Supply",

    "Security Guards",

    "Fire Alarm Maintenance",

    "X-Ray Scanning Equipment",

    "Leadership Development Program",

    "Public Relations Services",

    "Social Media Content Creation",

    "أحبار طابعات",

    "كاميرات مراقبة",

    "صيانة أجهزة الحريق",

    "قرطاسية مكتبية",
]


# ============================================================
# TEXT NORMALIZATION
# ============================================================

def normalize_text(value):

    text = str(
        value or ""
    )

    text = unicodedata.normalize(
        "NFKD",
        text
    )

    text = "".join(
        char
        for char in text
        if not unicodedata.combining(char)
    )

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

    text = text.casefold()

    text = re.sub(
        r"[_\-/\\|(),:;]+",
        " ",
        text
    )

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


# ============================================================
# PHRASE MATCH
# ============================================================

def has_phrase(
    text,
    phrases
):

    normalized = normalize_text(
        text
    )

    padded = (
        " "
        +
        normalized
        +
        " "
    )

    for phrase in phrases:

        p = normalize_text(
            phrase
        )

        if not p:
            continue

        if (
            " " + p + " "
            in padded
        ):
            return True

    return False


# ============================================================
# PARSE LABEL
# ============================================================

def parse_label(value):

    text = str(
        value
    ).strip().casefold()

    if text in {
        "1",
        "tech",
        "technology",
        "it",
    }:
        return 1

    if text in {
        "0",
        "nontech",
        "non-tech",
        "non technology",
        "non-technology",
    }:
        return 0

    try:

        number = int(
            float(text)
        )

        if number in {
            0,
            1,
        }:
            return number

    except Exception:
        pass

    return None


# ============================================================
# READ BASE TRAINING CSV
# ============================================================

def load_base_data():

    if not TRAINING_FILE.exists():

        raise FileNotFoundError(
            f"Missing: {TRAINING_FILE}"
        )

    with open(
        TRAINING_FILE,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(
            file
        )

        if not reader.fieldnames:

            raise RuntimeError(
                "training_data.csv has no header."
            )

        fields = [
            field.strip()
            for field in reader.fieldnames
        ]

        label_field = None

        for candidate in [
            "label",
            "Label",
            "class",
            "Class",
        ]:

            if candidate in fields:

                label_field = candidate
                break

        if label_field is None:

            raise RuntimeError(
                "Could not find label column."
            )

        text_field = None

        for candidate in TITLE_FIELDS:

            if candidate in fields:

                text_field = candidate
                break

        if text_field is None:

            for field in fields:

                if field != label_field:

                    text_field = field
                    break

        if text_field is None:

            raise RuntimeError(
                "Could not find text/title column."
            )

        rows = []

        for row in reader:

            text = str(
                row.get(
                    text_field,
                    ""
                )
            ).strip()

            label = parse_label(
                row.get(
                    label_field,
                    ""
                )
            )

            if not text:
                continue

            if label not in {
                0,
                1,
            }:
                continue

            rows.append(
                (
                    text,
                    label
                )
            )

    return dedupe_labeled(
        rows
    )


# ============================================================
# DEDUPE LABELED DATA
#
# إذا نفس العنوان له label متناقض:
# نستبعده بدل ما نعلم الموديل شيئين مختلفين.
# ============================================================

def dedupe_labeled(rows):

    data = {}

    conflicts = set()

    for text, label in rows:

        key = normalize_text(
            text
        )

        if not key:
            continue

        if key in data:

            if (
                data[key][1]
                !=
                label
            ):

                conflicts.add(
                    key
                )

            continue

        data[key] = (
            text,
            label
        )

    output = []

    for key, value in data.items():

        if key in conflicts:
            continue

        output.append(
            value
        )

    return output


# ============================================================
# GET TITLE FROM JSON RECORD
# ============================================================

def get_record_title(record):

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
# LOAD SOURCE TITLES
# ============================================================

def load_source_titles(
    base_keys
):

    titles = []

    seen = set()

    duplicates = 0

    for path in SOURCE_FILES:

        if not path.exists():

            print(
                f"WARNING: missing source file: "
                f"{path.name}"
            )

            continue

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
            continue

        for record in data:

            title = get_record_title(
                record
            )

            if not title:
                continue

            key = normalize_text(
                title
            )

            if not key:
                continue

            if key in base_keys:

                continue

            if key in seen:

                duplicates += 1
                continue

            seen.add(
                key
            )

            titles.append(
                title
            )

    return (
        titles,
        duplicates
    )


# ============================================================
# MODEL
# ============================================================

def build_model():

    features = FeatureUnion(
        [
            (
                "word",
                TfidfVectorizer(
                    lowercase=True,
                    ngram_range=(1, 2),
                    sublinear_tf=True,
                    min_df=1,
                )
            ),

            (
                "char",
                TfidfVectorizer(
                    analyzer="char_wb",
                    lowercase=True,
                    ngram_range=(3, 5),
                    sublinear_tf=True,
                    min_df=1,
                )
            ),
        ]
    )

    classifier = LinearSVC(
        class_weight="balanced",
        random_state=RANDOM_STATE,
    )

    return Pipeline(
        [
            (
                "features",
                features
            ),

            (
                "classifier",
                classifier
            ),
        ]
    )


# ============================================================
# METRICS
# ============================================================

def evaluate(
    model,
    X,
    y
):

    predictions = model.predict(
        X
    )

    return {
        "accuracy":
            accuracy_score(
                y,
                predictions
            ),

        "tech_precision":
            precision_score(
                y,
                predictions,
                pos_label=1,
                zero_division=0,
            ),

        "tech_recall":
            recall_score(
                y,
                predictions,
                pos_label=1,
                zero_division=0,
            ),

        "tech_f1":
            f1_score(
                y,
                predictions,
                pos_label=1,
                zero_division=0,
            ),

        "confusion":
            confusion_matrix(
                y,
                predictions,
                labels=[
                    0,
                    1,
                ]
            ),
    }


# ============================================================
# PRINT METRICS
# ============================================================

def print_metrics(
    name,
    metrics
):

    print(
        "\n"
        +
        "=" * 60
    )

    print(
        name
    )

    print(
        "=" * 60
    )

    print(
        f"Accuracy:       "
        f"{metrics['accuracy'] * 100:.2f}%"
    )

    print(
        f"Tech precision: "
        f"{metrics['tech_precision'] * 100:.2f}%"
    )

    print(
        f"Tech recall:    "
        f"{metrics['tech_recall'] * 100:.2f}%"
    )

    print(
        f"Tech F1:        "
        f"{metrics['tech_f1'] * 100:.2f}%"
    )

    print(
        "\nConfusion matrix:"
    )

    print(
        metrics[
            "confusion"
        ]
    )


# ============================================================
# ADD HARD EXAMPLES
# ============================================================

def add_hard_examples(
    X,
    y
):

    output_X = list(
        X
    )

    output_y = list(
        y
    )

    existing = {
        normalize_text(text)
        for text in output_X
    }

    for text in HARD_TECH:

        key = normalize_text(
            text
        )

        if key not in existing:

            output_X.append(
                text
            )

            output_y.append(
                1
            )

            existing.add(
                key
            )

    for text in HARD_NONTECH:

        key = normalize_text(
            text
        )

        if key not in existing:

            output_X.append(
                text
            )

            output_y.append(
                0
            )

            existing.add(
                key
            )

    return (
        output_X,
        output_y
    )


# ============================================================
# CURATE SOURCE EXAMPLES
#
# Technology:
# - strong tech phrase
# OR
# - baseline is strongly confident Tech
#
# Non-Tech:
# - only strong Non-Tech evidence
#
# Ambiguous:
# - ignored
# ============================================================

def curate_source_examples(
    titles,
    baseline_model
):

    tech = []

    nontech = []

    review = []

    for title in titles:

        tech_rule = has_phrase(
            title,
            SOURCE_TECH_PHRASES
        )

        nontech_rule = has_phrase(
            title,
            SOURCE_NONTECH_PHRASES
        )


        # conflicting evidence
        if (
            tech_rule
            and
            nontech_rule
        ):

            review.append(
                title
            )

            continue


        if nontech_rule:

            nontech.append(
                title
            )

            continue


        if tech_rule:

            tech.append(
                title
            )

            continue


        try:

            score = float(
                baseline_model
                .decision_function(
                    [title]
                )[0]
            )

        except Exception:

            review.append(
                title
            )

            continue


        if (
            score
            >=
            SOURCE_TECH_CONFIDENCE
        ):

            tech.append(
                title
            )

        else:

            review.append(
                title
            )

    return (
        tech,
        nontech,
        review
    )


# ============================================================
# CRITICAL REGRESSION TEST
# ============================================================

CRITICAL_TESTS = [

    (
        "Microsoft Cloud Services",
        1
    ),

    (
        "Cybersecurity Platform",
        1
    ),

    (
        "Supply of Computers and Servers",
        1
    ),

    (
        "Cisco Network Infrastructure",
        1
    ),

    (
        "ERP System Support",
        1
    ),

    (
        "Cleaning Services",
        0
    ),

    (
        "Printer Toner",
        0
    ),

    (
        "CCTV Cameras",
        0
    ),

    (
        "Water Pumps",
        0
    ),

    (
        "Road Construction",
        0
    ),

    (
        "Air Conditioning Maintenance",
        0
    ),

    (
        "Medical Consumables",
        0
    ),

    (
        "Forklift Battery",
        0
    ),
]


def critical_failures(
    model
):

    failures = []

    for text, expected in CRITICAL_TESTS:

        predicted = int(
            model.predict(
                [text]
            )[0]
        )

        if (
            predicted
            !=
            expected
        ):

            failures.append(
                (
                    text,
                    expected,
                    predicted
                )
            )

    return failures


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "=" * 60
    )

    print(
        "TECHNOLOGY TENDER MODEL TRAINING V4"
    )

    print(
        "=" * 60
    )


    # ========================================================
    # BASE DATA
    # ========================================================

    base = load_base_data()

    X = [
        text
        for text, label in base
    ]

    y = [
        label
        for text, label in base
    ]

    print(
        f"\nBase usable examples: "
        f"{len(X)}"
    )

    print(
        "Base labels:",
        dict(
            Counter(
                y
            )
        )
    )


    # ========================================================
    # UNTOUCHED TEST
    # ========================================================

    (
        X_train_val,
        X_test,
        y_train_val,
        y_test
    ) = train_test_split(

        X,
        y,

        test_size=0.20,

        random_state=
            RANDOM_STATE,

        stratify=y,
    )


    # ========================================================
    # TRAIN / VALIDATION
    # ========================================================

    (
        X_train,
        X_val,
        y_train,
        y_val
    ) = train_test_split(

        X_train_val,
        y_train_val,

        test_size=0.25,

        random_state=
            RANDOM_STATE,

        stratify=
            y_train_val,
    )


    print(
        f"Train:      "
        f"{len(X_train)}"
    )

    print(
        f"Validation: "
        f"{len(X_val)}"
    )

    print(
        f"Test:       "
        f"{len(X_test)}"
    )


    # ========================================================
    # BASELINE USED TO CURATE SOURCES
    # ========================================================

    (
        baseline_X,
        baseline_y
    ) = add_hard_examples(
        X_train,
        y_train
    )


    curator_model = build_model()

    curator_model.fit(
        baseline_X,
        baseline_y
    )


    # ========================================================
    # LOAD SOURCE DATA
    # ========================================================

    base_keys = {
        normalize_text(
            text
        )
        for text in X
    }


    (
        source_titles,
        source_duplicates
    ) = load_source_titles(
        base_keys
    )


    print(
        f"\nUnique new source titles: "
        f"{len(source_titles)}"
    )

    print(
        f"Source duplicates skipped: "
        f"{source_duplicates}"
    )


    (
        source_tech,
        source_nontech,
        source_review
    ) = curate_source_examples(
        source_titles,
        curator_model
    )


    print(
        "\nCurated source examples:"
    )

    print(
        f"Technology high-confidence: "
        f"{len(source_tech)}"
    )

    print(
        f"Non-Tech high-confidence:   "
        f"{len(source_nontech)}"
    )

    print(
        f"Ambiguous skipped:          "
        f"{len(source_review)}"
    )


    # ========================================================
    # BALANCE SOURCE EXAMPLES
    #
    # ما نسمح لـ196 Tech يطغون على 31 Non-Tech.
    # ========================================================

    max_balanced = min(

        len(
            source_tech
        ),

        len(
            source_nontech
        ),

        MAX_SOURCE_PER_CLASS,
    )


    rng = random.Random(
        RANDOM_STATE
    )


    rng.shuffle(
        source_tech
    )

    rng.shuffle(
        source_nontech
    )


    print(
        f"\nMaximum balanced source "
        f"examples per class: "
        f"{max_balanced}"
    )


    # ========================================================
    # TRY MULTIPLE SOURCE AMOUNTS
    # ========================================================

    candidate_sizes = {
        0
    }

    for size in [
        10,
        20,
        30,
        40,
        50,
        60,
        max_balanced,
    ]:

        if (
            size > 0
            and
            size <= max_balanced
        ):

            candidate_sizes.add(
                size
            )


    candidate_sizes = sorted(
        candidate_sizes
    )


    best_model = None

    best_size = 0

    best_metrics = None

    best_selection_score = None


    print(
        "\n"
        +
        "=" * 60
    )

    print(
        "VALIDATION MODEL SEARCH"
    )

    print(
        "=" * 60
    )


    for size in candidate_sizes:

        train_X = list(
            X_train
        )

        train_y = list(
            y_train
        )


        (
            train_X,
            train_y
        ) = add_hard_examples(
            train_X,
            train_y
        )


        if size > 0:

            train_X.extend(
                source_tech[
                    :size
                ]
            )

            train_y.extend(
                [
                    1
                ]
                *
                size
            )

            train_X.extend(
                source_nontech[
                    :size
                ]
            )

            train_y.extend(
                [
                    0
                ]
                *
                size
            )


        model = build_model()

        model.fit(
            train_X,
            train_y
        )


        metrics = evaluate(
            model,
            X_val,
            y_val
        )


        # Balanced selection:
        #
        # Accuracy أهم شيء
        # ثم Precision
        # ثم Recall
        selection_score = (

            metrics[
                "accuracy"
            ]
            *
            0.50

            +

            metrics[
                "tech_precision"
            ]
            *
            0.30

            +

            metrics[
                "tech_recall"
            ]
            *
            0.20
        )


        print(
            f"\nSource/class = {size}"
        )

        print(
            f"Accuracy="
            f"{metrics['accuracy']:.3f} | "
            f"Tech Precision="
            f"{metrics['tech_precision']:.3f} | "
            f"Tech Recall="
            f"{metrics['tech_recall']:.3f} | "
            f"Score="
            f"{selection_score:.3f}"
        )


        if (
            best_selection_score
            is None
            or
            selection_score
            >
            best_selection_score
        ):

            best_selection_score = (
                selection_score
            )

            best_size = size

            best_metrics = metrics

            best_model = model


    print(
        "\nBest validation source "
        f"size/class: {best_size}"
    )


    # ========================================================
    # FINAL BASELINE
    # ========================================================

    (
        final_base_X,
        final_base_y
    ) = add_hard_examples(
        X_train_val,
        y_train_val
    )


    final_baseline = build_model()

    final_baseline.fit(
        final_base_X,
        final_base_y
    )


    baseline_test_metrics = evaluate(
        final_baseline,
        X_test,
        y_test
    )


    # ========================================================
    # FINAL CANDIDATE
    # ========================================================

    final_candidate_X = list(
        X_train_val
    )

    final_candidate_y = list(
        y_train_val
    )


    (
        final_candidate_X,
        final_candidate_y
    ) = add_hard_examples(
        final_candidate_X,
        final_candidate_y
    )


    if best_size > 0:

        final_candidate_X.extend(
            source_tech[
                :best_size
            ]
        )

        final_candidate_y.extend(
            [
                1
            ]
            *
            best_size
        )

        final_candidate_X.extend(
            source_nontech[
                :best_size
            ]
        )

        final_candidate_y.extend(
            [
                0
            ]
            *
            best_size
        )


    final_candidate = build_model()

    final_candidate.fit(
        final_candidate_X,
        final_candidate_y
    )


    candidate_test_metrics = evaluate(
        final_candidate,
        X_test,
        y_test
    )


    # ========================================================
    # COMPARE
    # ========================================================

    print_metrics(
        "FINAL BASELINE TEST",
        baseline_test_metrics
    )


    print_metrics(
        "FINAL CANDIDATE TEST",
        candidate_test_metrics
    )


    baseline_failures = (
        critical_failures(
            final_baseline
        )
    )

    candidate_failures = (
        critical_failures(
            final_candidate
        )
    )


    print(
        "\nCritical regression failures:"
    )

    print(
        "Baseline:",
        len(
            baseline_failures
        )
    )

    print(
        "Candidate:",
        len(
            candidate_failures
        )
    )


    # ========================================================
    # SAFETY GATE
    #
    # Candidate ما ينحفظ إذا خرّب الأداء.
    # ========================================================

    candidate_allowed = (

        best_size > 0

        and

        candidate_test_metrics[
            "accuracy"
        ]
        >=
        baseline_test_metrics[
            "accuracy"
        ]
        -
        0.005

        and

        candidate_test_metrics[
            "tech_precision"
        ]
        >=
        baseline_test_metrics[
            "tech_precision"
        ]
        -
        0.01

        and

        candidate_test_metrics[
            "tech_recall"
        ]
        >=
        baseline_test_metrics[
            "tech_recall"
        ]
        -
        0.03

        and

        len(
            candidate_failures
        )
        <=
        len(
            baseline_failures
        )
    )


    if candidate_allowed:

        final_model = (
            final_candidate
        )

        selected_name = (
            "CANDIDATE WITH SOURCE DATA"
        )

        selected_source_size = (
            best_size
        )

    else:

        final_model = (
            final_baseline
        )

        selected_name = (
            "BASELINE - SOURCE DATA REJECTED"
        )

        selected_source_size = 0


    # ========================================================
    # SAVE
    # ========================================================

    MODEL_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    joblib.dump(
        final_model,
        MODEL_FILE
    )


    print(
        "\n"
        +
        "=" * 60
    )

    print(
        "MODEL SELECTED"
    )

    print(
        "=" * 60
    )

    print(
        selected_name
    )

    print(
        "Balanced source examples "
        f"used per class: "
        f"{selected_source_size}"
    )

    print(
        "\nModel saved:"
    )

    print(
        MODEL_FILE
    )


    # ========================================================
    # QUICK TEST
    # ========================================================

    print(
        "\n"
        +
        "=" * 60
    )

    print(
        "QUICK TEST"
    )

    print(
        "=" * 60
    )


    for text, expected in CRITICAL_TESTS:

        prediction = int(
            final_model.predict(
                [text]
            )[0]
        )

        predicted_name = (
            "Technology"
            if prediction == 1
            else "Non-Technology"
        )

        expected_name = (
            "Technology"
            if expected == 1
            else "Non-Technology"
        )

        status = (
            "OK"
            if prediction == expected
            else "WRONG"
        )

        print(
            f"{status:5} | "
            f"{predicted_name:14} | "
            f"expected="
            f"{expected_name:14} | "
            f"{text}"
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()