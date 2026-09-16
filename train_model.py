import csv
from pathlib import Path

import joblib

from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC

from sklearn.model_selection import train_test_split

from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
)


# ============================================================
# FILES
# ============================================================

TRAINING_FILE = Path("training_data.csv")

MODEL_FOLDER = Path("models")

MODEL_FILE = MODEL_FOLDER / "tech_classifier.joblib"


# ============================================================
# LOAD TRAINING DATA
# ============================================================

def load_training_data():

    texts = []
    labels = []

    with open(
        TRAINING_FILE,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:

            text = str(
                row.get("text", "")
            ).strip()

            label = str(
                row.get("label", "")
            ).strip()

            # نتجاهل أي صف ما عنده label واضح.
            #
            # 0 = Non-Technology
            # 1 = Technology
            if label not in {"0", "1"}:
                continue

            if not text:
                continue

            texts.append(text)

            labels.append(
                int(label)
            )

    return texts, labels


# ============================================================
# CREATE MODEL
# ============================================================

def create_model():

    # --------------------------------------------------------
    # WORD TF-IDF
    #
    # يتعلم الكلمات والعبارات.
    #
    # أمثلة:
    #
    # cloud
    # cybersecurity
    # artificial intelligence
    # Microsoft licenses
    # تنظيف
    # صيانة أجهزة التكييف
    # --------------------------------------------------------

    word_tfidf = TfidfVectorizer(

        lowercase=True,

        ngram_range=(1, 2),

        min_df=1,

        max_df=0.95,

        sublinear_tf=True
    )


    # --------------------------------------------------------
    # CHARACTER TF-IDF
    #
    # يتعلم أجزاء الكلمات.
    #
    # مفيد لأن بياناتنا:
    #
    # عربي + إنجليزي
    # اختصارات
    # أخطاء كتابة
    # أسماء منتجات وأنظمة
    # --------------------------------------------------------

    char_tfidf = TfidfVectorizer(

        analyzer="char_wb",

        ngram_range=(3, 5),

        min_df=1,

        sublinear_tf=True
    )


    # --------------------------------------------------------
    # COMBINE FEATURES
    #
    # نجمع:
    #
    # الكلمات
    # +
    # أجزاء الكلمات
    # --------------------------------------------------------

    features = FeatureUnion(
        [
            (
                "word_features",
                word_tfidf
            ),
            (
                "char_features",
                char_tfidf
            ),
        ]
    )


    # --------------------------------------------------------
    # LINEAR SVC
    #
    # 0 = Non-Technology
    # 1 = Technology
    #
    # عندنا الآن تقريبًا:
    #
    # Technology     = 162
    # Non-Technology = 208
    #
    # بعد إضافة Hard Negatives،
    # صار الموديل أكثر تحفظًا وبدأ يفوّت
    # Technology كثيرة.
    #
    # لذلك نعطي Technology وزن أعلى.
    #
    # 0 -> وزن 1.0
    # 1 -> وزن 1.5
    #
    # الهدف:
    # الحفاظ على Precision أفضل،
    # لكن نرفع Technology Recall مرة ثانية.
    # --------------------------------------------------------

    classifier = LinearSVC(
    class_weight="balanced",
    random_state=42
)


    # --------------------------------------------------------
    # PIPELINE
    #
    # النص
    # ↓
    # Word + Character TF-IDF
    # ↓
    # LinearSVC
    # ↓
    # Technology / Non-Technology
    # --------------------------------------------------------

    model = Pipeline(
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

    return model


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("LOADING TRAINING DATA")
    print("=" * 70)


    # --------------------------------------------------------
    # READ DATA
    # --------------------------------------------------------

    texts, labels = load_training_data()


    technology_count = sum(
        label == 1
        for label in labels
    )

    non_technology_count = sum(
        label == 0
        for label in labels
    )


    print(
        "Total usable examples:",
        len(texts)
    )

    print(
        "Technology:",
        technology_count
    )

    print(
        "Non-Technology:",
        non_technology_count
    )


    # --------------------------------------------------------
    # SAFETY CHECK
    # --------------------------------------------------------

    if len(texts) < 20:

        print(
            "\nERROR: Not enough training examples."
        )

        return


    # ========================================================
    # SPLIT DATA
    #
    # 75% Training
    # 25% Testing
    #
    # stratify=labels
    #
    # معناها نحافظ تقريبًا على نفس نسبة
    # Technology و Non-Technology
    # داخل التدريب والاختبار.
    # ========================================================

    (
        X_train,
        X_test,
        y_train,
        y_test
    ) = train_test_split(

        texts,
        labels,

        test_size=0.25,

        random_state=42,

        stratify=labels
    )


    print("\n" + "=" * 70)

    print(
        "Training examples:",
        len(X_train)
    )

    print(
        "Testing examples:",
        len(X_test)
    )


    # ========================================================
    # CREATE MODEL
    # ========================================================

    print("\n" + "=" * 70)
    print("TRAINING MODEL")
    print("=" * 70)


    model = create_model()


    # ========================================================
    # TRAIN
    # ========================================================

    model.fit(
        X_train,
        y_train
    )


    print(
        "Training finished."
    )


    # ========================================================
    # TEST MODEL
    # ========================================================

    predictions = model.predict(
        X_test
    )


    accuracy = accuracy_score(
        y_test,
        predictions
    )


    print("\n" + "=" * 70)
    print("MODEL RESULTS")
    print("=" * 70)


    print(
        "Accuracy:",
        round(
            accuracy * 100,
            2
        ),
        "%"
    )


    print("\nClassification Report:\n")


    print(
        classification_report(

            y_test,
            predictions,

            target_names=[
                "Non-Technology",
                "Technology"
            ],

            digits=3,

            zero_division=0
        )
    )


    print(
        "Confusion Matrix:"
    )


    print(
        confusion_matrix(
            y_test,
            predictions
        )
    )


    # ========================================================
    # FINAL TRAINING
    #
    # بعد ما نختبر الموديل،
    # ندرب نسخة نهائية باستخدام
    # جميع الأمثلة الموجودة في CSV.
    #
    # حاليًا تقريبًا 370 مثال.
    # ========================================================

    print("\n" + "=" * 70)
    print("TRAINING FINAL MODEL")
    print("=" * 70)


    final_model = create_model()


    final_model.fit(
        texts,
        labels
    )


    # ========================================================
    # SAVE MODEL
    # ========================================================

    MODEL_FOLDER.mkdir(
        exist_ok=True
    )


    joblib.dump(
        final_model,
        MODEL_FILE
    )


    print(
        "Model saved to:",
        MODEL_FILE
    )


    # ========================================================
    # QUICK TEST
    #
    # مجرد فحص سريع لأمثلة واضحة.
    #
    # هذا ليس بديلًا عن الاختبار الحقيقي.
    # ========================================================

    test_examples = [

        "Microsoft Cloud Services",

        "Cybersecurity Platform",

        "Development of Government Website",

        "Artificial Intelligence Platform",

        "Supply of Computers and Servers",

        "Cleaning Services for Government Buildings",

        "Supply of Water Pumps",

        "Road Construction Works",

        "Maintenance of Air Conditioning Units",

        "Supply of Medical Consumables"
    ]


    test_predictions = final_model.predict(
        test_examples
    )


    print("\n" + "=" * 70)
    print("QUICK TEST")
    print("=" * 70)


    for text, prediction in zip(
        test_examples,
        test_predictions
    ):

        if prediction == 1:
            result = "Technology"

        else:
            result = "Non-Technology"


        print(
            f"{result:16} | {text}"
        )


    print("\n" + "=" * 70)
    print("FINISHED")
    print("=" * 70)


if __name__ == "__main__":
    main()