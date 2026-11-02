#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT}/scripts/floci_env.sh"

if [[ -n "${UPSTOX_ACCESS_TOKEN:-}" ]]; then
  secret_payload="$(python -c 'import json,os; print(json.dumps({"access_token":os.environ["UPSTOX_ACCESS_TOKEN"], **({"expires_at":os.environ["UPSTOX_EXPIRES_AT"]} if os.environ.get("UPSTOX_EXPIRES_AT") else {})}))')"
  aws secretsmanager put-secret-value --secret-id "${UPSTOX_SECRET_ARN}" --secret-string "${secret_payload}" >/dev/null
  echo "Updated local Upstox token secret."
else
  echo "UPSTOX_ACCESS_TOKEN is unset; skipping Upstox secret."
fi
if [[ -n "${COINGECKO_DEMO_API_KEY:-}" ]]; then
  secret_payload="$(python -c 'import json,os; print(json.dumps({"api_key":os.environ["COINGECKO_DEMO_API_KEY"]}))')"
  aws secretsmanager put-secret-value --secret-id "${COINGECKO_SECRET_ARN}" --secret-string "${secret_payload}" >/dev/null
  echo "Updated local CoinGecko API secret."
else
  echo "COINGECKO_DEMO_API_KEY is unset; skipping CoinGecko secret."
fi
