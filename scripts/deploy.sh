#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STACK_NAME="${STACK_NAME:-financial-lakehouse-ingestion}"
AWS_REGION="${AWS_REGION:-ap-south-1}"
ARTIFACT_BUCKET="${ARTIFACT_BUCKET:?Set ARTIFACT_BUCKET to an existing deployment-artifacts bucket in ${AWS_REGION}}"
ARTIFACT_KEY="${ARTIFACT_KEY:-financial-lakehouse-ingestion/lambda.zip}"

"${ROOT}/scripts/build_zip.sh"
aws s3 cp "${ROOT}/dist/financial-ingestion.zip" "s3://${ARTIFACT_BUCKET}/${ARTIFACT_KEY}" --region "${AWS_REGION}"

aws cloudformation validate-template --template-body "file://${ROOT}/infra/template.yaml" --region "${AWS_REGION}" >/dev/null

aws cloudformation deploy \
  --template-file "${ROOT}/infra/template.yaml" \
  --stack-name "${STACK_NAME}" \
  --region "${AWS_REGION}" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    ProjectName=financial-lakehouse \
    ArtifactBucket="${ARTIFACT_BUCKET}" \
    ArtifactKey="${ARTIFACT_KEY}" \
    PythonRuntime=python3.13 \
    LambdaTimeout=300 \
    LambdaMemory=1024 \
    ReservedConcurrency=5

aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${AWS_REGION}" \
  --query 'Stacks[0].Outputs' \
  --output table
