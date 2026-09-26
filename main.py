import subprocess
import sys
import threading
import os
import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# نجلب connection string من Databricks secrets لو dbutils موجود
# (يعني نشتغل داخل Databricks). لو ما موجود (تشغيل محلي)، نتخطى
# هذا السطر و azure_upload.py نفسه يتعامل مع غيابه بدون خطأ.
try:
    dbutils
except NameError:
    pass
else:
    os.environ["AZURE_STORAGE_CONNECTION_STRING"] = dbutils.secrets.get(
        scope="azure-storage",
        key="connection-string"
    )

import azure_upload

# ============================================================
# PATHS
# ============================================================
try:
    PROJECT_ROOT = Path(__file__).resolve().parent
except NameError:
    # __file__ غير موجود لما يشتغل الكود كنوتبوك Databricks
    PROJECT_ROOT = Path(os.getcwd())

EXTRACTORS_DIR = PROJECT_ROOT / "extractors"
RAW_DIR = PROJECT_ROOT / "results" / "raw"

# نستخدم نفس _date_utils.py اللي في extractors
sys.path.insert(0, str(EXTRACTORS_DIR))
from _date_utils import parse_any_date, get_target_date  # noqa: E402


# ============================================================
# LATEST FILTER -- يخلّي مناقصات "أمس" بس (بتوقيت الرياض)
# ============================================================
# True  = يفلتر ملفات results/raw على أمس قبل الرفع (التشغيل اليومي)
# False = يرفع كل شي بدون فلترة (السلوك القديم)
FILTER_LATEST = True

# لإعادة يوم معيّن بدل أمس:  TARGET_DATE=2026-09-24 python main.py

# حقل تاريخ النشر في كل مصدر -- يجرّبها بالترتيب ويستخدم أول واحد موجود
PUBLISHED_DATE_FIELDS = [
    "Published Date",        # فرصة، اعتماد، globaltenders، البحرين
    "تاريخ_طرح_المناقصة",    # عُمان
    "تاريخ الطرح",           # مناقصات قطر
    "Posting Date",          # مؤسسة قطر
    "Open Date",             # الكويت، uae_mof
    "published_date",
    "publishDate",
]

def record_published_date(record):
    for field in PUBLISHED_DATE_FIELDS:
        if record.get(field):
            return parse_any_date(record.get(field))
    return None


def filter_raw_to_target(target, run_started=None):
    """يعدّل ملفات results/raw/*.json في مكانها: يخلّي مناقصات target بس.
    حماية: إذا ما قدر يقرأ ولا تاريخ في ملف كامل، يتركه زي ما هو
    (عشان ما يضيع مصدر كامل بسبب صيغة تاريخ جديدة) ويطبع تحذير."""
    print("\n" + "=" * 60)
    print(f"LATEST FILTER -- keeping tenders published on {target} (Asia/Riyadh)")
    print("=" * 60)

    # تاريخ السحب (بتوقيت الرياض) -> results/raw/<source>/<YYYY-MM-DD>.json
    # (مجلد داخل raw، فـ azure_upload ما يشوفه لأنه يقرأ results/raw/*.json بس)
    scrape_date = datetime.now(ZoneInfo("Asia/Riyadh")).date().isoformat()

    for raw_file in sorted(RAW_DIR.glob("*.json")):
        # ملف ما انكتب في هذا التشغيل = الـ extractor حقه فشل -> نفضّيه عشان
        # azure_upload يتخطاه (يتخطى الملفات الفاضية) وما نرفع بيانات قديمة مرة ثانية
        if run_started and raw_file.stat().st_mtime < run_started:
            raw_file.write_text("[]", encoding="utf-8")
            print(f"  {raw_file.name}: WARNING not updated this run (extractor failed) -- not uploaded")
            continue

        try:
            records = json.loads(raw_file.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  {raw_file.name}: could not read ({exc}) -- skipped")
            continue

        if not isinstance(records, list):
            print(f"  {raw_file.name}: not a list -- skipped")
            continue

        kept, unparsed, parsed = [], [], 0
        for record in records:
            published = record_published_date(record) if isinstance(record, dict) else None
            if published is None:
                unparsed.append(record)
                continue
            parsed += 1
            if published == target:
                kept.append(record)

        if records and parsed == 0:
            print(f"  {raw_file.name}: WARNING no dates parsed -- left unfiltered ({len(records)})")
            continue

        # السجلات اللي تاريخها ما انقرى نخليها (أأمن للـ bronze) ونحذّر
        kept.extend(unparsed)

        payload = json.dumps(kept, ensure_ascii=False, indent=2)

        # 1) ملف باسم تاريخ السحب: results/raw/<source>/<YYYY-MM-DD>.json
        new_file = RAW_DIR / raw_file.stem / f"{scrape_date}.json"
        new_file.parent.mkdir(parents=True, exist_ok=True)
        new_file.write_text(payload, encoding="utf-8")

        # 2) نفس المحتوى في results/raw/<source>.json عشان azure_upload يرفعه مثل قبل
        tmp = raw_file.with_suffix(".json.tmp")
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(raw_file)

        note = f" | WARNING {len(unparsed)} unparsed kept" if unparsed else ""
        print(f"  {raw_file.name}: {len(records)} -> {len(kept)}{note}  ->  {new_file.relative_to(PROJECT_ROOT)}")

    print("=" * 60)


# ============================================================
# ACTIVE -- raw extraction + upload only, no filtering
# ============================================================
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

    # نضيف مجلد extractors صراحة لـ PYTHONPATH عشان ملفات
    # مثل _date_utils.py تُلقى دائماً، ونمنع buffering
    # عشان تطلع مخرجات print() أول بأول.
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(EXTRACTORS_DIR)
        + (os.pathsep + existing_pythonpath if existing_pythonpath else "")
    )
    env["PYTHONUNBUFFERED"] = "1"

    result = subprocess.run(
        [sys.executable, "-u", str(extractor)],
        cwd=PROJECT_ROOT,
        env=env,
    )
    return result.returncode == 0


def run_batch(thread_label, extractors, failed_list, lock):
    for extractor in extractors:
        print(f"[{thread_label}] starting {extractor.name}")
        if not run_extractor(extractor):
            with lock:
                failed_list.append(extractor.name)
            print(f"WARNING: {extractor.name} extraction failed.")


def main():
    run_started = time.time()
    print("=" * 60)
    print("TENDER PIPELINE - " + ("LATEST (yesterday only)" if FILTER_LATEST else "RAW EXTRACTION ONLY (no filtering)"))
    print("=" * 60)

    extractors = discover_extractors()
    print(f"Extractors found: {len(extractors)}")
    for extractor in extractors:
        print(" -", extractor.name)

    #  NUM_THREADS to use multi threads to run the 10 portals.
    NUM_THREADS = 4
    batches = [[] for _ in range(NUM_THREADS)]
    for i, extractor in enumerate(extractors):
        batches[i % NUM_THREADS].append(extractor)

    for i, batch in enumerate(batches, start=1):
        print(f"\nThread {i} ({len(batch)}): {[e.name for e in batch]}")

    extraction_failed = []
    lock = threading.Lock()

    threads = [
        threading.Thread(
            target=run_batch,
            args=(f"thread-{i}", batch, extraction_failed, lock),
        )
        for i, batch in enumerate(batches, start=1)
        if batch  # skip empty batches (e.g. fewer extractors than threads)
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)

    if extraction_failed:
        print("Extraction failed:", ", ".join(extraction_failed))
    else:
        print("All extractors completed without error.")

    print("\nRaw files:", RAW_DIR)
    print("=" * 60)

    if FILTER_LATEST:
        filter_raw_to_target(get_target_date(), run_started)

    azure_result = azure_upload.upload_raw_results()

    print("=" * 60)
    print(
        "Pipeline finished"
        + (
            " with warnings."
            if extraction_failed or azure_result["failed"]
            else " successfully."
        )
    )
    print("=" * 60)


if __name__ == "__main__":
    main()