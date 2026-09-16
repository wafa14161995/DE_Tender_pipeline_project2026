import json
import subprocess
import sys
from pathlib import Path

import joblib


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent

EXTRACTORS_DIR = (
    PROJECT_ROOT
    / "extractors"
)

RAW_DIR = (
    PROJECT_ROOT
    / "results"
    / "raw"
)

FILTERED_DIR = (
    PROJECT_ROOT
    / "results"
    / "filtered"
)

MODEL_FILE = (
    PROJECT_ROOT
    / "models"
    / "tech_classifier.joblib"
)


# ============================================================
# POSSIBLE TITLE FIELDS
#
# كل مصدر يسمي عنوان المناقصة بطريقة مختلفة
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
]


# ============================================================
# CLEAN TEXT
# ============================================================

def clean_text(value):

    if value is None:
        return ""

    return " ".join(
        str(value).split()
    )


# ============================================================
# GET TENDER TITLE
# ============================================================

def get_tender_title(record):

    # أولاً نجرب الحقول المعروفة
    for field in TITLE_FIELDS:

        value = clean_text(
            record.get(field)
        )

        if value:
            return value


    # لو مصدر جديد عنده اسم مختلف
    # نحاول نكتشف الحقل تلقائياً
    for key, value in record.items():

        key_text = str(key).lower()

        if (
            "title" in key_text
            or "subject" in key_text
            or "موضوع" in str(key)
        ):

            value = clean_text(
                value
            )

            if value:
                return value


    return ""


# ============================================================
# GET RECORDS FROM JSON
# ============================================================

def get_records(data):

    # الشكل الطبيعي عندنا:
    #
    # [
    #   {...},
    #   {...}
    # ]

    if isinstance(
        data,
        list
    ):
        return data


    # حماية لو أضفنا مصدر مستقبلاً
    # يرجع:
    #
    # {"data": [...]}
    #
    # أو results / items / tenders
    if isinstance(
        data,
        dict
    ):

        possible_keys = [
            "data",
            "results",
            "items",
            "tenders",
            "records",
        ]


        for key in possible_keys:

            value = data.get(
                key
            )


            if isinstance(
                value,
                list
            ):
                return value


    return []


# ============================================================
# LOAD ML MODEL
# ============================================================

def load_model():

    if not MODEL_FILE.exists():

        print(
            "❌ ML model was not found:"
        )

        print(
            f"   {MODEL_FILE}"
        )

        sys.exit(1)


    try:

        model = joblib.load(
            MODEL_FILE
        )


        print(
            "✅ Technology classifier loaded."
        )


        return model


    except Exception as error:

        print(
            "❌ Could not load technology classifier:"
        )

        print(
            f"   {error}"
        )

        sys.exit(1)


# ============================================================
# FIND EXTRACTORS
# ============================================================

def get_sources():

    """
    Find all Python extractor files.
    """

    sources = []


    for file in EXTRACTORS_DIR.glob(
        "*.py"
    ):

        if file.name == "__init__.py":
            continue


        if file.name.startswith("_"):
            continue


        sources.append(
            file
        )


    return sorted(
        sources
    )


# ============================================================
# RUN ONE EXTRACTOR
# ============================================================

def run_source(source):

    """
    Run one source.

    إذا فشل مصدر واحد،
    ما نخرب بقية المصادر.
    """

    print(
        "\n" + "=" * 60
    )

    print(
        f"Running source: {source.name}"
    )

    print(
        "=" * 60
    )


    try:

        result = subprocess.run(

            [
                sys.executable,
                "-u",
                str(source),
            ],

            cwd=PROJECT_ROOT,

            check=False,
        )


        if result.returncode == 0:

            print(
                f"✅ {source.name} completed successfully."
            )

            return True


        print(
            f"❌ {source.name} failed "
            f"with exit code "
            f"{result.returncode}."
        )


        return False


    except KeyboardInterrupt:

        print(
            "\n⛔ Execution stopped by user."
        )

        raise


    except Exception as error:

        print(
            f"❌ Unexpected error in "
            f"{source.name}: {error}"
        )


        return False


# ============================================================
# FILTER ONE SOURCE
# ============================================================

def filter_source(
    source,
    model
):

    """
    نقرأ الـRaw JSON الذي سحبه المصدر.

    ثم:
        Technology     = نحتفظ بها
        Non-Technology = نستبعدها

    مهم:
    لا نغير بيانات الـRecord نفسها.
    نحفظ السجل الأصلي كما جاء من المصدر.
    """

    # --------------------------------------------------------
    # مثلاً:
    #
    # bahrain.py
    # ↓
    # results/raw/bahrain.json
    # --------------------------------------------------------

    raw_file = (
        RAW_DIR
        / f"{source.stem}.json"
    )


    # --------------------------------------------------------
    # الناتج المفلتر:
    #
    # results/filtered/bahrain.json
    # --------------------------------------------------------

    output_file = (
        FILTERED_DIR
        / f"{source.stem}.json"
    )


    print(
        "\n" + "-" * 60
    )

    print(
        f"Filtering source: {source.stem}"
    )

    print(
        "-" * 60
    )


    # ========================================================
    # CHECK RAW FILE
    # ========================================================

    if not raw_file.exists():

        print(
            f"❌ Raw file not found: "
            f"{raw_file}"
        )

        return None


    # ========================================================
    # READ JSON
    # ========================================================

    try:

        data = json.loads(

            raw_file.read_text(
                encoding="utf-8"
            )
        )


    except Exception as error:

        print(
            f"❌ Could not read "
            f"{raw_file.name}: "
            f"{error}"
        )

        return None


    records = get_records(
        data
    )


    # ========================================================
    # EMPTY SOURCE
    # ========================================================

    if not records:

        print(
            "⚠️ No records found to classify."
        )


        FILTERED_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )


        output_file.write_text(
            "[]",
            encoding="utf-8",
        )


        return {
            "source":
                source.stem,

            "total":
                0,

            "technology":
                0,

            "non_technology":
                0,

            "missing_title":
                0,
        }


    # ========================================================
    # PREPARE DATA FOR MODEL
    # ========================================================

    texts = []

    valid_records = []

    missing_title = 0


    for record in records:

        if not isinstance(
            record,
            dict
        ):
            continue


        title = get_tender_title(
            record
        )


        # لو ما قدرنا نلقى عنوان
        if not title:

            missing_title += 1

            continue


        texts.append(
            title
        )


        valid_records.append(
            record
        )


    # ========================================================
    # PREDICT ALL TENDERS AT ONCE
    #
    # هذا أسرع من:
    # predict() لكل Tender لحالها
    # ========================================================

    technology_records = []


    if texts:

        predictions = model.predict(
            texts
        )


        for (
            record,
            prediction
        ) in zip(
            valid_records,
            predictions
        ):

            # 1 = Technology
            if int(
                prediction
            ) == 1:

                technology_records.append(
                    record
                )


    # ========================================================
    # COUNTS
    # ========================================================

    total_valid = len(
        valid_records
    )


    non_technology_count = (
        total_valid
        - len(
            technology_records
        )
    )


    # ========================================================
    # SAVE FILTERED JSON
    # ========================================================

    FILTERED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


    output_file.write_text(

        json.dumps(
            technology_records,
            ensure_ascii=False,
            indent=2,
        ),

        encoding="utf-8",
    )


    # ========================================================
    # SOURCE SUMMARY
    # ========================================================

    print(
        f"Total records:       "
        f"{len(records)}"
    )


    print(
        f"Technology:          "
        f"{len(technology_records)}"
    )


    print(
        f"Non-Technology:      "
        f"{non_technology_count}"
    )


    print(
        f"Missing title:       "
        f"{missing_title}"
    )


    print(
        f"✅ Saved: "
        f"{output_file}"
    )


    return {
        "source":
            source.stem,

        "total":
            len(records),

        "technology":
            len(
                technology_records
            ),

        "non_technology":
            non_technology_count,

        "missing_title":
            missing_title,
    }


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    print(
        "\nTechnology Tender Extraction Pipeline"
    )

    print(
        "=" * 60
    )


    # ========================================================
    # STEP 1
    #
    # LOAD LOCAL ML MODEL
    #
    # ما نستخدم:
    # Azure AI
    # API
    # LLM
    #
    # التصنيف كله Local
    # ========================================================

    model = load_model()


    # ========================================================
    # STEP 2
    #
    # FIND SOURCES
    # ========================================================

    sources = get_sources()


    if not sources:

        print(
            "❌ No extractor files found "
            "inside extractors/"
        )

        sys.exit(1)


    print(
        f"Found {len(sources)} source(s):"
    )


    for source in sources:

        print(
            f" - {source.name}"
        )


    successful_sources = []

    failed_sources = []

    filtering_results = []

    filtering_failed = []


    # ========================================================
    # STEP 3
    #
    # EXTRACT
    #
    # ثم مباشرة FILTER
    # ========================================================

    for source in sources:


        # ---------------------------------------------
        # Run extractor
        # ---------------------------------------------

        if run_source(
            source
        ):

            successful_sources.append(
                source.name
            )


            # -----------------------------------------
            # Run local ML filter
            # -----------------------------------------

            filter_result = filter_source(
                source,
                model,
            )


            if filter_result is None:

                filtering_failed.append(
                    source.name
                )


            else:

                filtering_results.append(
                    filter_result
                )


        else:

            failed_sources.append(
                source.name
            )


    # ========================================================
    # EXTRACTION SUMMARY
    # ========================================================

    print(
        "\n" + "=" * 60
    )

    print(
        "EXTRACTION SUMMARY"
    )

    print(
        "=" * 60
    )


    print(
        f"Total sources: "
        f"{len(sources)}"
    )


    print(
        f"Successful:    "
        f"{len(successful_sources)}"
    )


    print(
        f"Failed:        "
        f"{len(failed_sources)}"
    )


    if successful_sources:

        print(
            "\n✅ Successful sources:"
        )


        for source in successful_sources:

            print(
                f" - {source}"
            )


    if failed_sources:

        print(
            "\n❌ Failed sources:"
        )


        for source in failed_sources:

            print(
                f" - {source}"
            )


    # ========================================================
    # FILTER SUMMARY
    # ========================================================

    print(
        "\n" + "=" * 60
    )

    print(
        "TECHNOLOGY FILTER SUMMARY"
    )

    print(
        "=" * 60
    )


    total_records = sum(

        result["total"]

        for result
        in filtering_results
    )


    total_technology = sum(

        result["technology"]

        for result
        in filtering_results
    )


    total_non_technology = sum(

        result["non_technology"]

        for result
        in filtering_results
    )


    total_missing_title = sum(

        result["missing_title"]

        for result
        in filtering_results
    )


    for result in filtering_results:

        print(

            f"{result['source']}: "

            f"{result['technology']} "
            f"Technology "

            f"/ "

            f"{result['total']} total"
        )


    print(
        "\n" + "-" * 60
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
        f"Non-Technology:      "
        f"{total_non_technology}"
    )


    print(
        f"Missing title:       "
        f"{total_missing_title}"
    )


    if filtering_failed:

        print(
            "\n❌ Filtering failed:"
        )


        for source in filtering_failed:

            print(
                f" - {source}"
            )


    print(
        "\n" + "=" * 60
    )


    # ========================================================
    # FINAL STATUS
    # ========================================================

    if (
        failed_sources
        or filtering_failed
    ):

        print(
            "⚠️ Pipeline finished, "
            "but some steps failed."
        )

        sys.exit(1)


    print(
        "✅ Extraction and technology "
        "filtering completed successfully."
    )


if __name__ == "__main__":
    main()