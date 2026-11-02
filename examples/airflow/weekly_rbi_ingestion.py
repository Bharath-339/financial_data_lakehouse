"""Example weekly RBI table ingestion DAG.

Bootstrap the selected table once with an explicit date range and
advance_watermark=true before enabling this incremental DAG.
"""

from datetime import timedelta
import json

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

with DAG(
    dag_id="financial_lakehouse_weekly_rbi_ingestion",
    description="Weekly incremental ingestion for one registered RBI DBIE table",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(2026, 1, 5, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["financial-lakehouse", "rbi", "ingestion"],
) as dag:
    LambdaInvokeFunctionOperator(
        task_id="ingest_rbi_table_rows",
        function_name=Variable.get("FINANCIAL_INGESTION_FUNCTION_NAME"),
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
        payload=json.dumps({
            "source": "rbi",
            "entity": "table_rows",
            "schema": Variable.get("FINANCIAL_RBI_SCHEMA"),
            "table": Variable.get("FINANCIAL_RBI_TABLE"),
            "overlap_days": 365,  # Re-fetch a year of history to capture revised macro observations.
            "page_size": 500,
            "max_pages": 20,
            "include_labels": True,
        }),
    )
