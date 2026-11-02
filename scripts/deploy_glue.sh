#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AWS_REGION="${AWS_REGION:-ap-south-1}"
INGESTION_STACK="${INGESTION_STACK:-financial-lakehouse-ingestion}"
GLUE_STACK="${GLUE_STACK:-financial-lakehouse-processing}"
ARTIFACT_BUCKET="${ARTIFACT_BUCKET:?Set ARTIFACT_BUCKET to the existing deployment artifacts bucket}"
ARTIFACT_PREFIX="${ARTIFACT_PREFIX:-financial-lakehouse-ingestion}"
GLUE_VERSION="${GLUE_VERSION:-5.1}"

outputs="$(aws cloudformation describe-stacks --stack-name "${INGESTION_STACK}" --region "${AWS_REGION}" --query 'Stacks[0].Outputs' --output json)"
RAW_BUCKET="$(python -c 'import json,sys; d=json.load(sys.stdin); print(next(x["OutputValue"] for x in d if x["OutputKey"]=="RawDataBucketName"))' <<<"${outputs}")"
GLUE_ROLE="$(python -c 'import json,sys; d=json.load(sys.stdin); print(next(x["OutputValue"] for x in d if x["OutputKey"]=="GlueJobRoleArn"))' <<<"${outputs}")"
INGESTION_FUNCTION="$(python -c 'import json,sys; d=json.load(sys.stdin); print(next(x["OutputValue"] for x in d if x["OutputKey"]=="IngestionFunctionName"))' <<<"${outputs}")"

"${ROOT}/scripts/build_lakehouse_zip.sh"
aws s3 cp "${ROOT}/dist/lakehouse-src.zip" "s3://${ARTIFACT_BUCKET}/${ARTIFACT_PREFIX}/lakehouse_src.zip" --region "${AWS_REGION}"
aws s3 sync "${ROOT}/glue_jobs/" "s3://${ARTIFACT_BUCKET}/${ARTIFACT_PREFIX}/glue_jobs/" --region "${AWS_REGION}"
aws s3 sync "${ROOT}/config/reference/" "s3://${RAW_BUCKET}/reference/" --region "${AWS_REGION}"
aws cloudformation validate-template --template-body "file://${ROOT}/infra/template.aws-glue.yaml" --region "${AWS_REGION}" >/dev/null
aws cloudformation deploy \
  --template-file "${ROOT}/infra/template.aws-glue.yaml" \
  --stack-name "${GLUE_STACK}" \
  --region "${AWS_REGION}" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ProjectName=financial-lakehouse RawBucketName="${RAW_BUCKET}" \
    ArtifactBucket="${ARTIFACT_BUCKET}" ArtifactPrefix="${ARTIFACT_PREFIX}" \
    GlueJobRoleArn="${GLUE_ROLE}" IngestionFunctionName="${INGESTION_FUNCTION}" GlueVersion="${GLUE_VERSION}"
aws cloudformation describe-stacks --stack-name "${GLUE_STACK}" --region "${AWS_REGION}" --query 'Stacks[0].Outputs' --output table
