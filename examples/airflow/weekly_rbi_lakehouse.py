"""Weekly RBI catalogue, table metadata and macro observations pipeline.

Configure FINANCIAL_RBI_SCHEMA / FINANCIAL_RBI_TABLE only after validating the
schema/table through the catalogue and reviewing its metadata and row mapping.
"""
from datetime import timedelta
import json
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

region = Variable.get("FINANCIAL_AWS_REGION", default_var="ap-south-1")
raw_bucket = Variable.get("FINANCIAL_RAW_BUCKET", default_var="SET_FINANCIAL_RAW_BUCKET")
function_name = Variable.get("FINANCIAL_INGESTION_FUNCTION_NAME", default_var="SET_FINANCIAL_INGESTION_FUNCTION_NAME")
b2s_job = Variable.get("FINANCIAL_GLUE_B2S_JOB", default_var="financial-lakehouse-bronze-to-silver")
s2g_job = Variable.get("FINANCIAL_GLUE_S2G_JOB", default_var="financial-lakehouse-silver-to-gold")
base_args = {
    "--bucket": raw_bucket,
    "--warehouse": f"s3://{raw_bucket}/warehouse/",
    "--reference-dir": f"s3a://{raw_bucket}/reference",
    "--catalog": "glue_catalog",
    "--silver-database": "lakehouse_silver",
    "--gold-database": "lakehouse_gold",
}

with DAG(
    dag_id="financial_lakehouse_weekly_rbi_end_to_end",
    description="Discover RBI tables, retain metadata, ingest observations, and refresh macro marts",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(2026, 1, 5, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["financial-lakehouse", "rbi", "macro"],
) as dag:
    catalogue_ingest = LambdaInvokeFunctionOperator(
        task_id="discover_rbi_catalogue",
        function_name=function_name, aws_conn_id="aws_default",
        invocation_type="RequestResponse", log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
        payload=json.dumps({"source": "rbi", "entity": "table_catalogue"}),
    )
    catalogue_silver = GlueJobOperator(
        task_id="rbi_catalogue_bronze_to_silver", job_name=b2s_job,
        aws_conn_id="aws_default", region_name=region, wait_for_completion=True,
        script_args={**base_args, "--source": "rbi", "--entity": "table_catalogue",
                     "--extract-date": "{{ data_interval_end.in_timezone('Asia/Kolkata').to_date_string() }}"},
    )

    rows_ingest = LambdaInvokeFunctionOperator(
        task_id="ingest_rbi_rows_and_metadata",
        function_name=function_name, aws_conn_id="aws_default",
        invocation_type="RequestResponse", log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 960, "tcp_keepalive": True},
        payload=json.dumps({
            "source": "rbi", "entity": "table_rows",
            "schema": Variable.get("FINANCIAL_RBI_SCHEMA", default_var="SET_FINANCIAL_RBI_SCHEMA"),
            "table": Variable.get("FINANCIAL_RBI_TABLE", default_var="SET_FINANCIAL_RBI_TABLE"),
            "overlap_days": 365,
            "bootstrap_start_date": Variable.get("FINANCIAL_RBI_BOOTSTRAP_START_DATE", default_var="2010-01-01"),
            "page_size": 500, "max_pages": 20, "include_labels": True,
        }),
    )
    metadata_silver = GlueJobOperator(
        task_id="rbi_metadata_bronze_to_silver", job_name=b2s_job,
        aws_conn_id="aws_default", region_name=region, wait_for_completion=True,
        script_args={**base_args, "--source": "rbi", "--entity": "table_metadata",
                     "--extract-date": "{{ data_interval_end.in_timezone('Asia/Kolkata').to_date_string() }}"},
    )
    rows_silver = GlueJobOperator(
        task_id="rbi_rows_bronze_to_silver", job_name=b2s_job,
        aws_conn_id="aws_default", region_name=region, wait_for_completion=True,
        script_args={**base_args, "--source": "rbi", "--entity": "table_rows",
                     "--extract-date": "{{ data_interval_end.in_timezone('Asia/Kolkata').to_date_string() }}"},
    )
    gold = GlueJobOperator(
        task_id="rebuild_gold_macro_marts", job_name=s2g_job,
        aws_conn_id="aws_default", region_name=region, wait_for_completion=True,
        script_args=base_args,
    )

    catalogue_ingest >> catalogue_silver
    rows_ingest >> [metadata_silver, rows_silver]
    [catalogue_silver, metadata_silver, rows_silver] >> gold
