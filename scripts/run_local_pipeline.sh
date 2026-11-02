#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT}/scripts/floci_env.sh"
STACK_NAME="${STACK_NAME:-financial-lakehouse-ingestion}"
FUNCTION_NAME="${FUNCTION_NAME:-$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --query "Stacks[0].Outputs[?OutputKey=='IngestionFunctionName'].OutputValue" --output text)}"
TMP_DIR="${TMP_DIR:-/tmp/financial-lakehouse-runs}"
mkdir -p "${TMP_DIR}"

invoke() {
  local event_file="$1" output="$2"
  echo "Invoking ${event_file}"
  aws lambda invoke --function-name "${FUNCTION_NAME}" --cli-binary-format raw-in-base64-out \
    --payload "fileb://${ROOT}/events/${event_file}" "${TMP_DIR}/${output}.json" >/dev/null
  python - "${TMP_DIR}/${output}.json" <<'PY'
import json,sys
with open(sys.argv[1]) as f: data=json.load(f)
if data.get("status") != "SUCCESS":
    raise SystemExit(f"Lambda invocation did not return SUCCESS: {data}")
print(f"SUCCESS run_id={data.get('run_id')} objects={data.get('object_count', len(data.get('object_keys', [])))}")
PY
}

# A first run requires valid Upstox and CoinGecko secrets in Floci Secrets Manager.
# The Upstox BOD instrument master is a public gzip JSON feed; no OAuth secret is needed.
invoke upstox_instrument_master.json upstox_master
invoke upstox_bootstrap.json upstox
invoke coingecko_daily.json coingecko
invoke frankfurter_bootstrap.json frankfurter
invoke rbi_catalogue.json rbi_catalogue

python "${ROOT}/scripts/validate_bronze.py" --bucket "${RAW_BUCKET}" --source upstox --entity instrument_master
python "${ROOT}/scripts/validate_bronze.py" --bucket "${RAW_BUCKET}" --source upstox --entity equity_ohlcv
python "${ROOT}/scripts/validate_bronze.py" --bucket "${RAW_BUCKET}" --source coingecko --entity market_snapshot
python "${ROOT}/scripts/validate_bronze.py" --bucket "${RAW_BUCKET}" --source frankfurter --entity daily_rates
python "${ROOT}/scripts/validate_bronze.py" --bucket "${RAW_BUCKET}" --source rbi --entity table_catalogue

"${ROOT}/scripts/run_local_spark.sh" bronze-to-silver --source upstox --entity instrument_master
"${ROOT}/scripts/run_local_spark.sh" bronze-to-silver --source upstox --entity equity_ohlcv
"${ROOT}/scripts/run_local_spark.sh" bronze-to-silver --source coingecko --entity market_snapshot
"${ROOT}/scripts/run_local_spark.sh" bronze-to-silver --source frankfurter --entity daily_rates
"${ROOT}/scripts/run_local_spark.sh" bronze-to-silver --source rbi --entity table_catalogue
"${ROOT}/scripts/run_local_spark.sh" silver-to-gold

echo "Local Bronze -> Silver -> Gold pipeline completed."
