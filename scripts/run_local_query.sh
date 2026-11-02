#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ne 1 ]]; then echo "Usage: $0 sql/analytics/01_latest_equity_quotes.sql" >&2; exit 2; fi
# shellcheck disable=SC1091
source "${ROOT}/scripts/floci_env.sh"
SQL_FILE="$1"
[[ -f "${SQL_FILE}" ]] || { echo "SQL file not found: ${SQL_FILE}" >&2; exit 2; }
OUTPUT="s3://${RAW_BUCKET}/athena-results/"
EXECUTION_ID="$(aws athena start-query-execution \
  --query-string "$(cat "${SQL_FILE}")" \
  --query-execution-context Database=lakehouse_gold \
  --result-configuration "OutputLocation=${OUTPUT}" \
  --work-group "${ATHENA_WORKGROUP:-primary}" \
  --query 'QueryExecutionId' --output text)"
echo "QueryExecutionId=${EXECUTION_ID}"
for _ in $(seq 1 60); do
  STATE="$(aws athena get-query-execution --query-execution-id "${EXECUTION_ID}" --query 'QueryExecution.Status.State' --output text)"
  case "${STATE}" in
    SUCCEEDED) aws athena get-query-results --query-execution-id "${EXECUTION_ID}"; exit 0 ;;
    FAILED|CANCELLED) aws athena get-query-execution --query-execution-id "${EXECUTION_ID}"; exit 1 ;;
    *) sleep 2 ;;
  esac
done
echo "Query still running; use aws athena get-query-execution --query-execution-id ${EXECUTION_ID}" >&2
exit 1
