# DE Tender Pipeline Project 2026

Current stage: extraction only.

- `main.py` runs every Python extractor inside `extractors/`.
- Every extractor scans all available listing pages for its source.
- Existing JSON is read before each run, so previously stored tenders are skipped.
- Only new tenders are appended to `results/raw/*.json`.
- `results/` is local runtime data and is not committed to Git.
- Technology filtering will be added later inside `filters/` before Azure deployment.

## Run locally

```bash
pip install -r requirements.txt
python -m playwright install chromium
python main.py