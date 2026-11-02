#!/usr/bin/env bash
# Source this file: source scripts/floci_env.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
loaded=0
if [[ -f "${ROOT}/.env.floci" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.env.floci"
  loaded=1
fi
# Generated stack outputs intentionally override bucket/table/secret names,
# while retaining endpoint and credentials from .env.floci.
if [[ -f "${ROOT}/.env.floci.generated" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.env.floci.generated"
  loaded=1
fi
if [[ "${loaded}" -eq 0 ]]; then
  export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
  export FLOCI_ENDPOINT_URL="${FLOCI_ENDPOINT_URL:-${AWS_ENDPOINT_URL}}"
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-flociadmin}"
  export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-flociadmin}"
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
  export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION}}"
  export RAW_BUCKET="${RAW_BUCKET:-financial-lakehouse-local}"
  export STATE_TABLE="${STATE_TABLE:-financial-lakehouse-ingestion-state}"
  export SILVER_DATABASE="${SILVER_DATABASE:-lakehouse_silver}"
  export GOLD_DATABASE="${GOLD_DATABASE:-lakehouse_gold}"
  export ICEBERG_CATALOG="${ICEBERG_CATALOG:-glue_catalog}"
  export WAREHOUSE_URI="${WAREHOUSE_URI:-s3://${RAW_BUCKET}/warehouse/}"
  export REFERENCE_DIR="${REFERENCE_DIR:-s3a://${RAW_BUCKET}/reference}"
  export LAKEHOUSE_ENV=local
fi
