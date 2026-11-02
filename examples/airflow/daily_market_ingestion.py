"""Example Airflow DAG: daily invocation fan-out for the ingestion Lambda.

Airflow owns scheduling and mapping. Lambda owns provider calls, Bronze writes,
checkpointing and source-specific retry behaviour.
"""

from datetime import timedelta
import json

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=7),  # Exceeds the default 300s Lambda timeout + 60s lease buffer.
}


@dag(
    dag_id="financial_lakehouse_daily_market_ingestion",
    description="Daily Upstox, CoinGecko and FX API ingestion into S3 Bronze",
    schedule="15 6 * * 1-5",  # Previous calendar day is complete by this IST run.
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["financial-lakehouse", "ingestion"],
)
def daily_market_ingestion():
    function_name = Variable.get("FINANCIAL_INGESTION_FUNCTION_NAME")
    instrument_keys_raw = Variable.get(
        "FINANCIAL_UPSTOX_INSTRUMENT_KEYS",
        default_var="NSE_EQ|INE002A01018",
    )
    instrument_keys = [key.strip() for key in instrument_keys_raw.split(",") if key.strip()]

    @task
    def build_upstox_payloads(keys: list[str]) -> list[str]:
        return [
            json.dumps({
                "source": "upstox",
                "entity": "equity_ohlcv",
                "instrument_key": key,
                "overlap_days": 3,
            })
            for key in keys
        ]

    payloads = build_upstox_payloads(instrument_keys)

    upstox = LambdaInvokeFunctionOperator.partial(
        task_id="ingest_upstox_instruments",
        function_name=function_name,
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
    ).expand(payload=payloads)

    coingecko = LambdaInvokeFunctionOperator(
        task_id="ingest_coingecko_market_snapshot",
        function_name=function_name,
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
        payload=json.dumps({
            "source": "coingecko",
            "entity": "market_snapshot",
            "vs_currency": "inr",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
        }),
    )

    frankfurter = LambdaInvokeFunctionOperator(
        task_id="ingest_frankfurter_rates",
        function_name=function_name,
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        log_type="Tail",
        botocore_config={"connect_timeout": 10, "read_timeout": 360, "tcp_keepalive": True},
        payload=json.dumps({
            "source": "frankfurter",
            "entity": "daily_rates",
            "base": "USD",
            "quotes": ["INR", "EUR", "GBP"],
            "overlap_days": 3,
        }),
    )

    # These source tasks are independent and can run in parallel. The Lambda
    # checkpoint lock and shared rate pacer protect duplicate work/API quotas.


daily_market_ingestion()
