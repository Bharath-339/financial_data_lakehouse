"""Manual, parameterized backfill with a shared Lambda/Spark run ID.

Example dag_run.conf for an equity backfill:
{
  "source": "upstox", "entity": "equity_ohlcv",
  "instrument_key": "NSE_EQ|INE002A01018",
  "start_date": "2026-06-01", "end_date": "2026-06-30"
}

CoinGecko /coins/markets is a current snapshot endpoint, not a historical time-series endpoint;
reprocess existing CoinGecko Bronze snapshots by run ID instead of attempting an API backfill.
For RBI table_rows provide schema/table. The Lambda writes both table_metadata
and table_rows envelopes; this DAG processes both Silver tables under one run ID.
Explicit backfills do not advance watermarks unless requested.
"""
import json
import re
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.trigger_rule import TriggerRule

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


@dag(
    dag_id="financial_lakehouse_historical_backfill",
    description="Parameter-driven historical extraction and Silver/Gold replay",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 0, "retry_delay": timedelta(minutes=5)},
    tags=["financial-lakehouse", "backfill"],
)
def financial_lakehouse_historical_backfill():
    @task
    def build_run_id() -> str:
        context = get_current_context()
        raw_id = str(context["dag_run"].run_id)
        return "bf_" + re.sub(r"[^A-Za-z0-9_-]", "_", raw_id)[:76]

    @task
    def build_event(run_id: str) -> str:
        conf = get_current_context()["dag_run"].conf or {}
        source = str(conf.get("source", "")).lower()
        entity = str(conf.get("entity", "")).lower()
        allowed = {
            ("upstox", "equity_ohlcv"),
            ("frankfurter", "daily_rates"), ("rbi", "table_rows"),
            ("rbi", "table_catalogue"),
        }
        if (source, entity) not in allowed:
            raise ValueError(f"Unsupported backfill source/entity: {source}/{entity}")
        if (source, entity) != ("rbi", "table_catalogue"):
            if not conf.get("start_date") or not conf.get("end_date"):
                raise ValueError("Backfill requires explicit start_date and end_date")
            if conf["start_date"] > conf["end_date"]:
                raise ValueError("start_date must be <= end_date")
        required = {"upstox": "instrument_key", "rbi": "schema"}
        if source in required and entity != "table_catalogue" and not conf.get(required[source]):
            raise ValueError(f"Backfill requires {required[source]}")
        if source == "rbi" and entity == "table_rows" and not conf.get("table"):
            raise ValueError("RBI table_rows backfill requires table")
        event = {k: v for k, v in conf.items() if k in {
            "source", "entity", "start_date", "end_date", "instrument_key", "vs_currency", "base", "quotes",
            "schema", "table", "filters", "page_size", "max_pages", "include_labels", "per_page", "page",
            "order", "q", "allow_empty",
        }}
        event["source"], event["entity"], event["run_id"] = source, entity, run_id
        event["advance_watermark"] = bool(conf.get("advance_watermark", False))
        return json.dumps(event)

    run_id = build_run_id()
    event_payload = build_event(run_id)
    ingest = LambdaInvokeFunctionOperator(
        task_id="extract_backfill_to_bronze",
        function_name=function_name,
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 960, "tcp_keepalive": True},
        payload=event_payload,
    )
    silver = GlueJobOperator(
        task_id="reprocess_bronze_into_silver",
        job_name=b2s_job,
        aws_conn_id="aws_default", region_name=region, wait_for_completion=True,
        script_args={**base_args,
                     "--source": "{{ dag_run.conf['source'] }}",
                     "--entity": "{{ dag_run.conf['entity'] }}",
                     "--run-id": run_id},
    )

    def _needs_rbi_metadata(**context):
        conf = context["dag_run"].conf or {}
        if conf.get("source") == "rbi" and conf.get("entity") == "table_rows":
            return "reprocess_rbi_metadata_into_silver"
        return "skip_rbi_metadata"

    metadata_branch = BranchPythonOperator(
        task_id="route_optional_rbi_metadata", python_callable=_needs_rbi_metadata,
    )
    metadata_silver = GlueJobOperator(
        task_id="reprocess_rbi_metadata_into_silver",
        job_name=b2s_job,
        aws_conn_id="aws_default", region_name=region, wait_for_completion=True,
        script_args={**base_args, "--source": "rbi", "--entity": "table_metadata", "--run-id": run_id},
    )
    skip_metadata = EmptyOperator(task_id="skip_rbi_metadata")
    silver_join = EmptyOperator(
        task_id="silver_reprocessing_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    gold = GlueJobOperator(
        task_id="rebuild_gold_marts",
        job_name=s2g_job,
        aws_conn_id="aws_default", region_name=region, wait_for_completion=True,
        script_args=base_args,
    )

    ingest >> [silver, metadata_branch]
    metadata_branch >> [metadata_silver, skip_metadata]
    [silver, metadata_silver, skip_metadata] >> silver_join
    silver_join >> gold


financial_lakehouse_historical_backfill()
