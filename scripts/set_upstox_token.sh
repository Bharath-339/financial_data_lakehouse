#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Source scripts/floci_env.sh first for Floci, or configure AWS credentials for AWS.

SECRET_ID="${UPSTOX_SECRET_ARN:-}"
if [[ -z "${SECRET_ID}" ]]; then
  STACK_NAME="${INGESTION_STACK:-financial-lakehouse-ingestion}"
  SECRET_ID="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='UpstoxTokenSecretArn'].OutputValue | [0]" --output text)"
fi
if [[ -z "${SECRET_ID}" || "${SECRET_ID}" == "None" ]]; then
  echo "Could not resolve the Upstox secret ARN. Set UPSTOX_SECRET_ARN or deploy the ingestion stack." >&2
  exit 1
fi

read -r -s -p "Paste the newly generated Upstox access token: " token
printf '\n'
if [[ -z "${token}" ]]; then
  echo "Token cannot be empty." >&2
  exit 2
fi
read -r -p "Token expiry (optional ISO-8601 or epoch seconds/milliseconds): " expires_at

tmp_file="$(mktemp)"
trap 'rm -f "${tmp_file}"; unset token expires_at' EXIT
UPSTOX_TOKEN_INPUT="${token}" UPSTOX_EXPIRY_INPUT="${expires_at}" python - "${tmp_file}" <<'PY'
import json, os, sys
payload = {"access_token": os.environ["UPSTOX_TOKEN_INPUT"]}
expiry = os.environ.get("UPSTOX_EXPIRY_INPUT", "").strip()
if expiry:
    payload["expires_at"] = expiry
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(payload, handle)
PY
chmod 600 "${tmp_file}"
aws secretsmanager put-secret-value --secret-id "${SECRET_ID}" \
  --secret-string "file://${tmp_file}" >/dev/null
printf 'Updated Upstox token in Secrets Manager secret %s\n' "${SECRET_ID}"
