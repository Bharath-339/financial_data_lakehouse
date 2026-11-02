#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 bronze-to-silver --source upstox --entity equity_ohlcv [Spark job args...]" >&2
  echo "       $0 silver-to-gold [Spark job args...]" >&2
  exit 2
fi
JOB="$1"; shift
case "${JOB}" in
  bronze-to-silver) SCRIPT="${ROOT}/glue_jobs/bronze_to_silver.py" ;;
  silver-to-gold) SCRIPT="${ROOT}/glue_jobs/silver_to_gold.py" ;;
  maintenance) SCRIPT="${ROOT}/glue_jobs/table_maintenance.py" ;;
  *) echo "Unknown Spark job: ${JOB}" >&2; exit 2 ;;
esac

# shellcheck disable=SC1091
source "${ROOT}/scripts/floci_env.sh"
command -v spark-submit >/dev/null || {
  echo "spark-submit not found. Install requirements-local.txt and a matching Java runtime." >&2; exit 1;
}
export PYTHONPATH="${ROOT}/lakehouse_src:${PYTHONPATH:-}"
SPARK_VERSION="${SPARK_VERSION:-3.5.6}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.10.0}"
HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.3.4}"
PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}"

exec spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --packages "${PACKAGES}" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf "spark.sql.catalog.glue_catalog.warehouse=${WAREHOUSE_URI}" \
  --conf spark.sql.catalog.glue_catalog.glue.endpoint="${AWS_ENDPOINT_URL}" \
  --conf spark.sql.catalog.glue_catalog.client.region="${AWS_DEFAULT_REGION}" \
  --conf spark.sql.catalog.glue_catalog.s3.endpoint="${AWS_ENDPOINT_URL}" \
  --conf spark.sql.catalog.glue_catalog.s3.path-style-access=true \
  --conf spark.sql.catalog.glue_catalog.s3.access-key-id="${AWS_ACCESS_KEY_ID}" \
  --conf spark.sql.catalog.glue_catalog.s3.secret-access-key="${AWS_SECRET_ACCESS_KEY}" \
  --conf spark.hadoop.fs.s3a.endpoint="${AWS_ENDPOINT_URL}" \
  --conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID}" \
  --conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.endpoint.region="${AWS_DEFAULT_REGION}" \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.adaptive.enabled=true \
  "${SCRIPT}" --bucket "${RAW_BUCKET}" --warehouse "${WAREHOUSE_URI}" \
  --catalog "${ICEBERG_CATALOG}" --silver-database "${SILVER_DATABASE}" \
  --gold-database "${GOLD_DATABASE}" --reference-dir "${REFERENCE_DIR}" --local "$@"
