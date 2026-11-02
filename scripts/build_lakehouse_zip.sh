#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "${ROOT}/dist"
rm -f "${ROOT}/dist/lakehouse-src.zip"
cd "${ROOT}/lakehouse_src"
zip -qr -X "${ROOT}/dist/lakehouse-src.zip" lakehouse -x '*/__pycache__/*' '*.pyc'
echo "Created ${ROOT}/dist/lakehouse-src.zip"
