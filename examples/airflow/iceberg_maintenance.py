"""Weekly Iceberg file compaction and snapshot maintenance."""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

raw_bucket = Variable.get("FINANCIAL_RAW_BUCKET", default_var="SET_FINANCIAL_RAW_BUCKET")
region = Variable.get("FINANCIAL_AWS_REGION", default_var="ap-south-1")
with DAG(
    dag_id="financial_lakehouse_iceberg_maintenance",
    description="Weekly Iceberg compaction and conservative snapshot retention",
    schedule="0 3 * * SUN",
    start_date=pendulum.datetime(2026, 1, 4, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 0, "retry_delay": timedelta(minutes=5)},
    tags=["financial-lakehouse", "iceberg", "maintenance"],
) as dag:
    GlueJobOperator(
        task_id="compact_and_expire_snapshots",
        job_name=Variable.get("FINANCIAL_GLUE_MAINTENANCE_JOB", default_var="financial-lakehouse-iceberg-maintenance"),
        aws_conn_id="aws_default", region_name=region, wait_for_completion=True,
        script_args={
            "--bucket": raw_bucket,
            "--warehouse": f"s3://{raw_bucket}/warehouse/",
            "--reference-dir": f"s3a://{raw_bucket}/reference",
            "--catalog": "glue_catalog",
            "--silver-database": "lakehouse_silver",
            "--gold-database": "lakehouse_gold",
            "--snapshot-retention-days": "30",
        },
    )
