"""Source-independent financial data quality rules and aliases."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError, TypeError):
        return None


def validate_ohlcv(row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    fields = ("open_price", "high_price", "low_price", "close_price")
    values = {name: to_decimal(row.get(name)) for name in fields}
    for name, value in values.items():
        if value is None:
            errors.append(f"{name}_missing_or_invalid")
        elif value <= 0:
            errors.append(f"{name}_must_be_positive")
    if all(values[name] is not None for name in fields):
        if values["high_price"] < max(values.values()):
            errors.append("high_price_below_ohlc")
        if values["low_price"] > min(values.values()):
            errors.append("low_price_above_ohlc")
    volume = to_decimal(row.get("volume"))
    if volume is None:
        errors.append("volume_missing_or_invalid")
    elif volume < 0:
        errors.append("volume_negative")
    if not row.get("instrument_key"):
        errors.append("instrument_key_missing")
    if not row.get("trading_date"):
        errors.append("trading_date_missing")
    return errors


def validate_fx(row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    rate = to_decimal(row.get("exchange_rate"))
    if not row.get("rate_date"):
        errors.append("rate_date_missing")
    if not row.get("base_currency") or not row.get("quote_currency"):
        errors.append("currency_missing")
    if rate is None or rate <= 0:
        errors.append("exchange_rate_invalid")
    return errors


def validate_crypto(row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not row.get("crypto_id"):
        errors.append("crypto_id_missing")
    if not row.get("snapshot_date"):
        errors.append("snapshot_date_missing")
    price = to_decimal(row.get("current_price"))
    if price is None or price < 0:
        errors.append("current_price_invalid")
    return errors


def rbi_field_candidates() -> dict[str, tuple[str, ...]]:
    """Aliases only; RBI-specific mappings must be reviewed per table in reference config."""
    return {
        "period": ("observation_period", "period", "date", "observation_date", "obs_date", "time", "month", "year"),
        "indicator": ("indicator_code", "series_code", "indicator", "series", "item_code", "item", "code", "id"),
        "value": ("observation_value", "obs_value", "value", "val", "amount", "rate", "figure", "data_value"),
        "unit": ("unit", "units", "unit_name", "measure"),
        "available_date": ("available_date", "published_at", "release_date", "publication_date"),
    }
