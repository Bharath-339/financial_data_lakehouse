#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${ROOT}/scripts/floci_env.sh"
ARTIFACT_BUCKET="${ARTIFACT_BUCKET:-financial-lakehouse-artifacts}"
ARTIFACT_KEY="${ARTIFACT_KEY:-financial-lakehouse-ingestion/lambda.zip}"
STACK_NAME="${STACK_NAME:-financial-lakehouse-ingestion}"

curl -fsS "${AWS_ENDPOINT_URL}/_floci/health" >/dev/null || {
  echo "Floci health check failed at ${AWS_ENDPOINT_URL}" >&2; exit 1;
}
"${ROOT}/scripts/build_zip.sh"
if ! aws s3api head-bucket --bucket "${ARTIFACT_BUCKET}" >/dev/null 2>&1; then
  aws s3api create-bucket --bucket "${ARTIFACT_BUCKET}" >/dev/null
fi
aws s3 cp "${ROOT}/dist/financial-ingestion.zip" "s3://${ARTIFACT_BUCKET}/${ARTIFACT_KEY}"
aws cloudformation validate-template --template-body "file://${ROOT}/infra/template.yaml" >/dev/null
aws cloudformation deploy \
  --template-file "${ROOT}/infra/template.yaml" \
  --stack-name "${STACK_NAME}" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ProjectName=financial-lakehouse ArtifactBucket="${ARTIFACT_BUCKET}" \
    ArtifactKey="${ARTIFACT_KEY}" AwsEndpointUrl="${LAMBDA_AWS_ENDPOINT_URL:-http://host.docker.internal:4566}" PythonRuntime=python3.13 \
    LambdaTimeout=300 LambdaMemory=1024 ReservedConcurrency=3 EnforceSecureTransportDeny=false

# Set RAW_BUCKET/STATE_TABLE from the newly created stack outputs on later shell invocations.
RAW_BUCKET="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --query "Stacks[0].Outputs[?OutputKey=='RawDataBucketName'].OutputValue | [0]" --output text)"
STATE_TABLE="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --query "Stacks[0].Outputs[?OutputKey=='IngestionStateTableName'].OutputValue | [0]" --output text)"
UPSTOX_SECRET_ARN="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --query "Stacks[0].Outputs[?OutputKey=='UpstoxTokenSecretArn'].OutputValue | [0]" --output text)"
COINGECKO_SECRET_ARN="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --query "Stacks[0].Outputs[?OutputKey=='CoinGeckoApiSecretArn'].OutputValue | [0]" --output text)"
LAMBDA_AWS_ENDPOINT_URL="${LAMBDA_AWS_ENDPOINT_URL:-http://host.docker.internal:4566}"
cat > "${ROOT}/.env.floci.generated" <<ENV
export AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL}
export FLOCI_ENDPOINT_URL=${FLOCI_ENDPOINT_URL}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}
export AWS_REGION=${AWS_REGION}
export RAW_BUCKET=${RAW_BUCKET}
export STATE_TABLE=${STATE_TABLE}
export UPSTOX_SECRET_ARN=${UPSTOX_SECRET_ARN}
export COINGECKO_SECRET_ARN=${COINGECKO_SECRET_ARN}
export SILVER_DATABASE=${SILVER_DATABASE}
export GOLD_DATABASE=${GOLD_DATABASE}
export ICEBERG_CATALOG=${ICEBERG_CATALOG}
export WAREHOUSE_URI=s3://${RAW_BUCKET}/warehouse/
export REFERENCE_DIR=s3a://${RAW_BUCKET}/reference
export LAKEHOUSE_ENV=local
ENV

echo "Generated .env.floci.generated with stack outputs. For the Lambda container endpoint, the stack used ${LAMBDA_AWS_ENDPOINT_URL}."
aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --query 'Stacks[0].Outputs' --output table
