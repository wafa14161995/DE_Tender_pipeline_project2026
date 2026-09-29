##!/bin/bash
# Cluster init script: installs Playwright + browsers on Databricks (Ubuntu 24.04)
# Init scripts run only at cluster startup, so restart the cluster after editing.

set -e

echo "=== [1/3] Updating apt ==="
apt-get update -y
echo "=== [1/3] Done ==="

echo "=== [2/3] Installing Playwright Python package ==="
PIP_BIN="/databricks/python/bin/pip"
if [ ! -f "$PIP_BIN" ]; then
  PIP_BIN="pip"
fi
"$PIP_BIN" install --upgrade playwright
echo "=== [2/3] Done ==="

echo "=== [3/3] Installing browsers + system dependencies ==="
PLAYWRIGHT_BIN="/databricks/python/bin/playwright"
if [ ! -f "$PLAYWRIGHT_BIN" ]; then
  PLAYWRIGHT_BIN="playwright"
fi
# --with-deps installs the required system libraries with the correct
# Ubuntu 24.04 package names (libasound2t64, libatk1.0-0t64, ...)
"$PLAYWRIGHT_BIN" install --with-deps chromium firefox webkit
echo "=== [3/3] Done ==="

echo "=== Playwright init script finished successfully ==="