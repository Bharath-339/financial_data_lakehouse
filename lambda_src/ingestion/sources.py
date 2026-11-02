from __future__ import annotations

import logging
import hashlib
import gzip
import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from .errors import ConfigurationError, PaginationLimitError, SourceAPIError
from .http import HttpClient

LOGGER = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


class IngestionRuntime:
    def __init__(self, http: HttpClient, writer, secrets, checkpoints, rate_limiter=None):
        self.http = http
        self.writer = writer
        self.secrets = secrets
        self.checkpoints = checkpoints
        self.rate_limiter = rate_limiter


def _throttle(runtime: IngestionRuntime, provider: str) -> None:
    # A DynamoDB-backed pacer shares request spacing across Lambda environments.
    intervals = {"upstox": 0.20, "coingecko": 1.20, "frankfurter": 0.10, "rbi": 0.20}
    if runtime.rate_limiter:
        runtime.rate_limiter.wait(provider, intervals[provider])



def _get_json(runtime: IngestionRuntime, provider: str, url: str, headers: dict[str, str] | None = None) -> Any:
    """Call the provider with distributed throttling applied to every HTTP attempt."""
    return runtime.http.get_json(
        url,
        headers=headers,
        before_attempt=lambda: _throttle(runtime, provider),
    )


def default_end_date() -> date:
    """Use the previous calendar date in the Indian market timezone."""
    return datetime.now(IST).date() - timedelta(days=1)


def parse_date(value: str, field: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{field} must be an ISO date in YYYY-MM-DD format") from exc


def resolve_date_range(event: dict[str, Any], state: dict[str, Any]) -> tuple[date, date, bool]:
    """Return start, end and whether this run should advance the incremental watermark.

    Explicit date ranges are treated as backfills by default. Incremental events omit
    start_date and derive it from the saved watermark minus an overlap window.
    """
    end = parse_date(event["end_date"], "end_date") if event.get("end_date") else default_end_date()
    overlap_days = int(event.get("overlap_days", os.environ.get("DEFAULT_OVERLAP_DAYS", "3")))
    if overlap_days < 0 or overlap_days > 3660:
        raise ConfigurationError("overlap_days must be between 0 and 3660")

    if event.get("start_date"):
        start = parse_date(event["start_date"], "start_date")
        advance = bool(event.get("advance_watermark", False))
    else:
        watermark = state.get("last_successful_date")
        if not watermark and event.get("bootstrap_start_date"):
            # A recurring DAG can carry this optional value: it is used only for the
            # first run, then the checkpoint takes over for incremental extraction.
            start = parse_date(event["bootstrap_start_date"], "bootstrap_start_date")
            advance = bool(event.get("advance_watermark", True))
        elif not watermark:
            raise ConfigurationError(
                "No checkpoint exists. First run must provide start_date/end_date or bootstrap_start_date."
            )
        else:
            start = parse_date(watermark, "last_successful_date") - timedelta(days=overlap_days)
            advance = True

    if start > end:
        raise ConfigurationError(f"start_date {start} must not be after end_date {end}")
    return start, end, advance


def split_upstox_daily_ranges(start: date, end: date):
    """Split daily-history requests into windows of at most ten calendar years."""
    current = start
    while current <= end:
        try:
            ten_year_boundary = current.replace(year=current.year + 10)
        except ValueError:  # 29 February
            ten_year_boundary = current.replace(month=2, day=28, year=current.year + 10)
        chunk_end = min(end, ten_year_boundary - timedelta(days=1))
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _records_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("rows", "data", "results", "records", "items", "candles", "tables"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
            if isinstance(value, dict):
                for nested_key in ("rows", "data", "results", "records", "items", "candles"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return len(nested)
    return 0


def _extract_row_list(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("rows", "data", "results", "records", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                for nested_key in ("rows", "data", "results", "records", "items"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return nested
    raise SourceAPIError("Could not locate a row array in the RBI API response; inspect the raw response schema")


def _write(runtime: IngestionRuntime, *, source: str, entity: str, entity_key: str,
           run_id: str, extract_date: str, page_number: int, request_metadata: dict[str, Any],
           payload: Any) -> tuple[str, int]:
    return runtime.writer.write(
        source=source,
        entity=entity,
        entity_key=entity_key,
        run_id=run_id,
        extract_date=extract_date,
        page_number=page_number,
        request_metadata=request_metadata,
        response_payload=payload,
    )


def _validate_upstox_token_expiry(secret: dict[str, Any]) -> None:
    """Validate optional Upstox expires_at (epoch seconds/ms or ISO timestamp)."""
    expiry = secret.get("expires_at")
    if expiry is None:
        return
    try:
        if isinstance(expiry, (int, float)) or str(expiry).isdigit():
            epoch = float(expiry)
            if epoch > 10**12:
                epoch /= 1000.0
        else:
            expiry_dt = datetime.fromisoformat(str(expiry).replace("Z", "+00:00"))
            if expiry_dt.tzinfo is None:
                expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)
            epoch = expiry_dt.timestamp()
    except (TypeError, ValueError, OverflowError) as exc:
        raise ConfigurationError("Upstox secret expires_at must be epoch seconds/milliseconds or ISO-8601") from exc
    if epoch <= time.time() + 60:
        raise ConfigurationError("Upstox access token is expired or expires within 60 seconds; update the Secrets Manager secret")


def _extract_upstox(event: dict[str, Any], runtime: IngestionRuntime, run_id: str, extract_date: str,
                    state: dict[str, Any]) -> dict[str, Any]:
    instrument_key = event.get("instrument_key")
    if not instrument_key:
        raise ConfigurationError("Upstox equity_ohlcv requires instrument_key")
    secret_arn = os.environ.get("UPSTOX_SECRET_ARN", "")
    secret = runtime.secrets.get_json(secret_arn, required=True)
    token = secret.get("access_token")
    if not token:
        raise ConfigurationError("Upstox secret must contain a non-empty 'access_token' field")
    _validate_upstox_token_expiry(secret)

    start, end, advance = resolve_date_range(event, state)
    total_records = 0
    objects: list[str] = []
    page_number = 1
    encoded_key = quote(str(instrument_key), safe="")
    for chunk_start, chunk_end in split_upstox_daily_ranges(start, end):
        path = (
            f"historical-candle/{encoded_key}/days/1/"
            f"{chunk_end.isoformat()}/{chunk_start.isoformat()}"
        )
        url = f"https://api.upstox.com/v3/{path}"
        payload = _get_json(runtime, "upstox", url, headers={"Authorization": f"Bearer {token}"})
        if not isinstance(payload, dict) or payload.get("status") != "success":
            raise SourceAPIError("Upstox returned an unsuccessful response for historical candles")
        data = payload.get("data") or {}
        candles = data.get("candles") if isinstance(data, dict) else None
        if not isinstance(candles, list):
            raise SourceAPIError("Upstox response did not contain data.candles as an array")
        if not candles and not bool(event.get("allow_empty", False)):
            LOGGER.warning("Upstox returned zero candles instrument_key=%s range=%s..%s", instrument_key, chunk_start, chunk_end)
        key, _ = _write(
            runtime, source="upstox", entity="equity_ohlcv", entity_key=str(instrument_key),
            run_id=run_id, extract_date=extract_date, page_number=page_number,
            request_metadata={"url": url, "instrument_key": instrument_key,
                              "unit": "days", "interval": 1,
                              "start_date": chunk_start.isoformat(), "end_date": chunk_end.isoformat()},
            payload=payload,
        )
        objects.append(key)
        total_records += len(candles)
        page_number += 1
    if total_records == 0 and not bool(event.get("allow_empty", False)):
        raise SourceAPIError("Upstox returned no candles for the requested range; watermark was not advanced")
    return {"record_count": total_records, "object_keys": objects,
            "start_date": start.isoformat(), "end_date": end.isoformat(), "advance_watermark": advance}



def _extract_upstox_instrument_master(event: dict[str, Any], runtime: IngestionRuntime,
                                     run_id: str, extract_date: str) -> dict[str, Any]:
    """Land the public daily BOD instrument master as one immutable Bronze snapshot."""
    url = str(event.get(
        "url",
        "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz",
    ))
    if not url.startswith("https://assets.upstox.com/"):
        raise ConfigurationError("Upstox instrument master URL must use the official assets.upstox.com host")
    raw = runtime.http.get_bytes(
        url,
        before_attempt=lambda: _throttle(runtime, "upstox"),
    )
    try:
        decoded = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
        payload = json.loads(decoded.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceAPIError("Upstox instrument master was not a valid JSON or gzip JSON document") from exc
    if isinstance(payload, dict):
        # Some feeds wrap the array in a data/instruments object. Keep the wrapper
        # shape intact for auditability but require a discoverable instrument array.
        candidates = [payload.get("data"), payload.get("instruments"), payload.get("records")]
        records = next((candidate for candidate in candidates if isinstance(candidate, list)), None)
    else:
        records = payload
    if not isinstance(records, list) or not records:
        raise SourceAPIError("Upstox instrument master contained no instrument records")
    valid = [row for row in records if isinstance(row, dict) and row.get("instrument_key")]
    if not valid:
        raise SourceAPIError("Upstox instrument master contained no records with instrument_key")
    key, compressed_size = _write(
        runtime, source="upstox", entity="instrument_master", entity_key="complete",
        run_id=run_id, extract_date=extract_date, page_number=1,
        request_metadata={"url": url, "feed": "BOD complete JSON",
                          "record_count": len(records), "valid_instrument_count": len(valid)},
        payload=payload,
    )
    return {"record_count": len(valid), "object_keys": [key],
            "compressed_bytes": compressed_size, "master_date": extract_date,
            "advance_watermark": False}

def _extract_coingecko(event: dict[str, Any], runtime: IngestionRuntime, run_id: str,
                       extract_date: str) -> dict[str, Any]:
    vs_currency = str(event.get("vs_currency", "inr")).lower()
    per_page = int(event.get("per_page", 50))
    page = int(event.get("page", 1))
    if not 1 <= per_page <= 250 or page < 1:
        raise ConfigurationError("CoinGecko per_page must be 1..250 and page must be >= 1")

    secret_arn = os.environ.get("COINGECKO_SECRET_ARN", "")
    secret = runtime.secrets.get_json(secret_arn, required=False)
    headers = {}
    if secret.get("api_key"):
        headers["x-cg-demo-api-key"] = secret["api_key"]

    query = {
        "vs_currency": vs_currency,
        "order": event.get("order", "market_cap_desc"),
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
    }
    url = HttpClient.build_url("https://api.coingecko.com/api/v3/coins/markets", query=query)
    payload = _get_json(runtime, "coingecko", url, headers=headers)
    if not isinstance(payload, list):
        raise SourceAPIError("CoinGecko /coins/markets response was not a JSON array")
    key, _ = _write(
        runtime, source="coingecko", entity="market_snapshot",
        entity_key=f"vs_currency={vs_currency}-page={page}", run_id=run_id,
        extract_date=extract_date, page_number=page,
        request_metadata={"url": url, "query": query}, payload=payload,
    )
    return {"record_count": len(payload), "object_keys": [key],
            "snapshot_date": extract_date, "advance_watermark": False}


def _extract_frankfurter(event: dict[str, Any], runtime: IngestionRuntime, run_id: str,
                         extract_date: str, state: dict[str, Any]) -> dict[str, Any]:
    base = str(event.get("base", "USD")).upper()
    quotes_value = event.get("quotes", ["INR", "EUR", "GBP"])
    if isinstance(quotes_value, str):
        quotes = [part.strip().upper() for part in quotes_value.split(",") if part.strip()]
    else:
        quotes = sorted({str(value).upper() for value in quotes_value})
    if not quotes:
        raise ConfigurationError("Frankfurter requires at least one quote currency")

    start, end, advance = resolve_date_range(event, state)
    query = {"base": base, "quotes": ",".join(quotes), "from": start.isoformat(), "to": end.isoformat()}
    url = HttpClient.build_url("https://api.frankfurter.dev/v2/rates", query=query)
    payload = _get_json(runtime, "frankfurter", url)
    if not isinstance(payload, list):
        raise SourceAPIError("Frankfurter v2 /rates response was not a JSON array")
    key, _ = _write(
        runtime, source="frankfurter", entity="daily_rates",
        entity_key=f"base={base}-quotes={'-'.join(quotes)}", run_id=run_id,
        extract_date=extract_date, page_number=1,
        request_metadata={"url": url, "query": query}, payload=payload,
    )
    return {"record_count": len(payload), "object_keys": [key],
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "advance_watermark": advance}


def _extract_rbi_catalogue(event: dict[str, Any], runtime: IngestionRuntime, run_id: str,
                           extract_date: str) -> dict[str, Any]:
    query = {key: event.get(key) for key in ("schema", "source", "q") if event.get(key) is not None}
    url = HttpClient.build_url("https://data-api.dbie.rbihub.in/api/tables", query=query)
    payload = _get_json(runtime, "rbi", url)
    key, _ = _write(
        runtime, source="rbi", entity="table_catalogue", entity_key="all-tables",
        run_id=run_id, extract_date=extract_date, page_number=1,
        request_metadata={"url": url, "query": query}, payload=payload,
    )
    return {"record_count": _records_count(payload), "object_keys": [key], "advance_watermark": False}


def _extract_rbi_rows(event: dict[str, Any], runtime: IngestionRuntime, run_id: str,
                      extract_date: str, state: dict[str, Any]) -> dict[str, Any]:
    schema = event.get("schema")
    table = event.get("table")
    if not schema or not table:
        raise ConfigurationError("RBI table_rows requires schema and table")
    start, end, advance = resolve_date_range(event, state)
    page_size = int(event.get("page_size", 500))
    max_pages = int(event.get("max_pages", 20))
    if not 1 <= page_size <= 5000 or not 1 <= max_pages <= 100:
        raise ConfigurationError("RBI page_size must be 1..5000 and max_pages must be 1..100")

    schema_path = quote(str(schema), safe="")
    table_path = quote(str(table), safe="")
    metadata_url = f"https://data-api.dbie.rbihub.in/api/tables/{schema_path}/{table_path}"
    metadata = _get_json(runtime, "rbi", metadata_url)
    meta_key, _ = _write(
        runtime, source="rbi", entity="table_metadata", entity_key=f"{schema}.{table}",
        run_id=run_id, extract_date=extract_date, page_number=0,
        request_metadata={"url": metadata_url}, payload=metadata,
    )

    objects = [meta_key]
    total_records = 0
    page_number = 0
    while page_number < max_pages:
        offset = page_number * page_size
        query: dict[str, Any] = {
            "from": start.isoformat(),
            "to": end.isoformat(),
            "limit": page_size,
            "offset": offset,
            "labels": 1 if event.get("include_labels", True) else 0,
        }
        filters = event.get("filters", {})
        if filters:
            if not isinstance(filters, dict):
                raise ConfigurationError("RBI filters must be a JSON object")
            reserved_filter_names = {"from", "to", "limit", "offset", "labels"}
            conflicts = reserved_filter_names.intersection(str(key).lower() for key in filters)
            if conflicts:
                raise ConfigurationError(f"RBI filters cannot override reserved pagination/date parameters: {sorted(conflicts)}")
            query.update({str(k): v for k, v in filters.items()})
        rows_url = HttpClient.build_url(
            f"https://data-api.dbie.rbihub.in/api/tables/{schema_path}/{table_path}/rows",
            query=query,
        )
        payload = _get_json(runtime, "rbi", rows_url)
        rows = _extract_row_list(payload)
        key, _ = _write(
            runtime, source="rbi", entity="table_rows", entity_key=f"{schema}.{table}",
            run_id=run_id, extract_date=extract_date, page_number=page_number + 1,
            request_metadata={"url": rows_url, "query": query}, payload=payload,
        )
        objects.append(key)
        total_records += len(rows)
        page_number += 1
        if len(rows) < page_size:
            break
    else:
        raise PaginationLimitError(
            f"RBI extraction reached max_pages={max_pages}; increase max_pages or narrow the date range. "
            "Raw pages already written are retained, but the watermark was not advanced."
        )

    return {"record_count": total_records, "object_keys": objects,
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "advance_watermark": advance, "pages": page_number}


def dispatch(event: dict[str, Any], runtime: IngestionRuntime, run_id: str, extract_date: str,
             state: dict[str, Any]) -> dict[str, Any]:
    source = str(event.get("source", "")).lower()
    entity = str(event.get("entity", "")).lower()
    if source == "upstox" and entity == "equity_ohlcv":
        return _extract_upstox(event, runtime, run_id, extract_date, state)
    if source == "upstox" and entity == "instrument_master":
        return _extract_upstox_instrument_master(event, runtime, run_id, extract_date)
    if source == "coingecko" and entity == "market_snapshot":
        return _extract_coingecko(event, runtime, run_id, extract_date)
    if source == "frankfurter" and entity == "daily_rates":
        return _extract_frankfurter(event, runtime, run_id, extract_date, state)
    if source == "rbi" and entity == "table_catalogue":
        return _extract_rbi_catalogue(event, runtime, run_id, extract_date)
    if source == "rbi" and entity == "table_rows":
        return _extract_rbi_rows(event, runtime, run_id, extract_date, state)
    raise ConfigurationError(
        "Unsupported source/entity. Supported: upstox/equity_ohlcv, "
        "upstox/instrument_master, coingecko/market_snapshot, frankfurter/daily_rates, "
        "rbi/table_catalogue, rbi/table_rows"
    )


def checkpoint_key(event: dict[str, Any]) -> str:
    source = str(event.get("source", "")).upper()
    entity = str(event.get("entity", "")).upper()
    if source == "UPSTOX" and entity == "EQUITY_OHLCV":
        identifier = event.get("instrument_key")
        if not identifier:
            raise ConfigurationError("Upstox equity_ohlcv requires instrument_key")
    elif source == "UPSTOX" and entity == "INSTRUMENT_MASTER":
        identifier = "complete"
    elif source == "COINGECKO":
        identifier = str(event.get("vs_currency", "inr")).upper()
    elif source == "FRANKFURTER":
        quotes = event.get("quotes", ["INR", "EUR", "GBP"])
        if isinstance(quotes, str):
            quotes = [x.strip().upper() for x in quotes.split(",") if x.strip()]
        identifier = str(event.get("base", "USD")).upper() + "_" + "-".join(sorted(str(x).upper() for x in quotes))
    elif source == "RBI" and entity == "TABLE_ROWS":
        if not event.get("schema") or not event.get("table"):
            raise ConfigurationError("RBI table_rows requires schema and table")
        filters = event.get("filters", {}) or {}
        filter_hash = hashlib.sha256(json.dumps(filters, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:10]
        identifier = f"{event['schema']}.{event['table']}.{filter_hash}"
    else:
        identifier = entity
    safe_identifier = "".join(ch if ch.isalnum() or ch in "-_.=" else "_" for ch in str(identifier))
    return f"{source}#{entity}#{safe_identifier}"
