import subprocess
import sys

sources = [
    "extractors/qatar.py",
    "extractors/qater_finance.py",
]

for src in sources:
    print(f"Running {src}...")
    subprocess.run([sys.executable, src])

print("All sources done.")
