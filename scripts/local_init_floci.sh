#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT}/scripts/floci_env.sh"

command -v aws >/dev/null || { echo "AWS CLI v2 is required" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }

for _ in $(seq 1 30); do
  if curl -fsS "${AWS_ENDPOINT_URL}/_floci/health" >/dev/null; then break; fi
  sleep 2
done
curl -fsS "${AWS_ENDPOINT_URL}/_floci/health" >/dev/null || {
  echo "Floci is not healthy at ${AWS_ENDPOINT_URL}. Start Floci and retry." >&2; exit 1;
}

if ! aws s3api head-bucket --bucket "${RAW_BUCKET}" >/dev/null 2>&1; then
  aws s3api create-bucket --bucket "${RAW_BUCKET}" >/dev/null
fi

export PYTHONPATH="${ROOT}/lakehouse_src:${PYTHONPATH:-}"
python "${ROOT}/scripts/bootstrap_catalog.py"
aws s3 cp --recursive "${ROOT}/config/reference/" "s3://${RAW_BUCKET}/reference/" --exclude '.DS_Store'

# Glue databases are created through the Glue API. Floci's CloudFormation
# implementation does not provision AWS::Glue::Database resources.
cat <<MSG
Floci lakehouse foundation is ready.
  Endpoint: ${AWS_ENDPOINT_URL}
  Bucket:   ${RAW_BUCKET}
  Catalog:  ${ICEBERG_CATALOG}
  Silver:   ${SILVER_DATABASE}
  Gold:     ${GOLD_DATABASE}

Next: run ingestion Lambda events, then scripts/run_local_spark.sh bronze-to-silver ...
MSG
