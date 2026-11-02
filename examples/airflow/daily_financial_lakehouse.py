"""Production Airflow DAG: ingest APIs, normalize into Silver, publish Gold.

Requires apache-airflow-providers-amazon and these Airflow Variables:
  FINANCIAL_INGESTION_FUNCTION_NAME, FINANCIAL_GLUE_B2S_JOB,
  FINANCIAL_GLUE_S2G_JOB, FINANCIAL_UPSTOX_INSTRUMENT_KEYS.
"""
from datetime import timedelta
import json

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=7),
}
AWS_REGION = Variable.get("FINANCIAL_AWS_REGION", default_var="ap-south-1")
INGESTION_FUNCTION = Variable.get("FINANCIAL_INGESTION_FUNCTION_NAME", default_var="SET_FINANCIAL_INGESTION_FUNCTION_NAME")
B2S_JOB = Variable.get("FINANCIAL_GLUE_B2S_JOB", default_var="financial-lakehouse-bronze-to-silver")
S2G_JOB = Variable.get("FINANCIAL_GLUE_S2G_JOB", default_var="financial-lakehouse-silver-to-gold")
RAW_BUCKET = Variable.get("FINANCIAL_RAW_BUCKET", default_var="SET_FINANCIAL_RAW_BUCKET")


def _spark_args(**extra):
    return {
        "--bucket": RAW_BUCKET,
        "--warehouse": f"s3://{RAW_BUCKET}/warehouse/",
        "--reference-dir": f"s3a://{RAW_BUCKET}/reference",
        "--catalog": "glue_catalog",
        "--silver-database": "lakehouse_silver",
        "--gold-database": "lakehouse_gold",
        **extra,
    }


@dag(
    dag_id="financial_lakehouse_daily_end_to_end",
    description="API ingestion -> Bronze validation/normalization -> Silver -> Gold",
    # Run daily for crypto/FX; Upstox payload selects the latest completed weekday.
    schedule="15 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["financial-lakehouse", "bronze", "silver", "gold"],
)
def financial_lakehouse_daily_end_to_end():
    @task
    def upstox_payloads() -> list[str]:
        """Select the latest completed weekday, avoiding Saturday/Sunday candle requests.

        Exchange holidays can still have no candles. In that case the Lambda fails the
        task without advancing its checkpoint; the next run retries the overlap window.
        """
        context = get_current_context()
        run_date = context["data_interval_end"].in_timezone("Asia/Kolkata").date()
        target_date = run_date - timedelta(days=1)
        while target_date.weekday() >= 5:
            target_date -= timedelta(days=1)
        raw = Variable.get("FINANCIAL_UPSTOX_INSTRUMENT_KEYS", default_var="NSE_EQ|INE002A01018")
        return [json.dumps({
                    "source": "upstox", "entity": "equity_ohlcv",
                    "instrument_key": key.strip(), "overlap_days": 3,
                    "bootstrap_start_date": Variable.get("FINANCIAL_UPSTOX_BOOTSTRAP_START_DATE", default_var="2020-01-01"),
                    "end_date": target_date.isoformat(),
                }) for key in raw.split(",") if key.strip()]

    upstox = LambdaInvokeFunctionOperator.partial(
        task_id="ingest_upstox", function_name=INGESTION_FUNCTION, aws_conn_id="aws_default",
        invocation_type="RequestResponse", log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
    ).expand(payload=upstox_payloads())

    instrument_master = LambdaInvokeFunctionOperator(
        task_id="ingest_upstox_instrument_master", function_name=INGESTION_FUNCTION,
        aws_conn_id="aws_default", invocation_type="RequestResponse", log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
        payload=json.dumps({"source": "upstox", "entity": "instrument_master"}),
    )
    silver_instrument_master = GlueJobOperator(
        task_id="bronze_to_silver_upstox_instrument_master", job_name=B2S_JOB,
        aws_conn_id="aws_default", region_name=AWS_REGION, wait_for_completion=True,
        script_args=_spark_args(**{"--source": "upstox", "--entity": "instrument_master",
                                   "--extract-date": "{{ data_interval_end.in_timezone('Asia/Kolkata').to_date_string() }}"}),
    )

    coingecko = LambdaInvokeFunctionOperator(
        task_id="ingest_coingecko", function_name=INGESTION_FUNCTION, aws_conn_id="aws_default",
        invocation_type="RequestResponse", log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
        payload=json.dumps({"source": "coingecko", "entity": "market_snapshot", "vs_currency": "inr",
                           "order": "market_cap_desc", "per_page": 50, "page": 1}),
    )
    frankfurter = LambdaInvokeFunctionOperator(
        task_id="ingest_frankfurter", function_name=INGESTION_FUNCTION, aws_conn_id="aws_default",
        invocation_type="RequestResponse", log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
        payload=json.dumps({"source": "frankfurter", "entity": "daily_rates", "base": "USD",
                           "quotes": ["INR", "EUR", "GBP"], "overlap_days": 3,
                           "bootstrap_start_date": Variable.get("FINANCIAL_FX_BOOTSTRAP_START_DATE", default_var="2020-01-01")}),
    )

    silver_equity = GlueJobOperator(
        task_id="bronze_to_silver_equities", job_name=B2S_JOB, aws_conn_id="aws_default",
        region_name=AWS_REGION, wait_for_completion=True,
        script_args=_spark_args(**{"--source": "upstox", "--entity": "equity_ohlcv",
                                   "--extract-date": "{{ data_interval_end.in_timezone('Asia/Kolkata').to_date_string() }}"}),
    )
    silver_crypto = GlueJobOperator(
        task_id="bronze_to_silver_crypto", job_name=B2S_JOB, aws_conn_id="aws_default",
        region_name=AWS_REGION, wait_for_completion=True,
        script_args=_spark_args(**{"--source": "coingecko", "--entity": "market_snapshot",
                                   "--extract-date": "{{ data_interval_end.in_timezone('Asia/Kolkata').to_date_string() }}"}),
    )
    silver_fx = GlueJobOperator(
        task_id="bronze_to_silver_fx", job_name=B2S_JOB, aws_conn_id="aws_default",
        region_name=AWS_REGION, wait_for_completion=True,
        script_args=_spark_args(**{"--source": "frankfurter", "--entity": "daily_rates",
                                   "--extract-date": "{{ data_interval_end.in_timezone('Asia/Kolkata').to_date_string() }}"}),
    )
    gold = GlueJobOperator(
        task_id="silver_to_gold", job_name=S2G_JOB, aws_conn_id="aws_default",
        region_name=AWS_REGION, wait_for_completion=True, script_args=_spark_args(),
    )

    upstox >> silver_equity
    instrument_master >> silver_instrument_master
    coingecko >> silver_crypto
    frankfurter >> silver_fx
    [silver_equity, silver_instrument_master, silver_crypto, silver_fx] >> gold


financial_lakehouse_daily_end_to_end()
