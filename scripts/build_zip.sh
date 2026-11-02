#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="${ROOT}/dist"
mkdir -p "${DIST_DIR}"
rm -f "${DIST_DIR}/financial-ingestion.zip"

cd "${ROOT}/lambda_src"
zip -qr -X "${DIST_DIR}/financial-ingestion.zip" . -x "*/__pycache__/*" "*.pyc"

echo "Created ${DIST_DIR}/financial-ingestion.zip"
