import os
import sys
import gzip
import json
import unittest
from datetime import date
from unittest.mock import patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "lambda_src"))

from ingestion.errors import ConfigurationError, PaginationLimitError
from ingestion.http import HttpClient
from ingestion.sources import (
    IngestionRuntime,
    _extract_frankfurter,
    _extract_upstox_instrument_master,
    _extract_rbi_rows,
    _extract_upstox,
    checkpoint_key,
    resolve_date_range,
    split_upstox_daily_ranges,
    _validate_upstox_token_expiry,
)


class FakeWriter:
    def __init__(self):
        self.calls = []

    def write(self, **kwargs):
        self.calls.append(kwargs)
        return (f"bronze/{kwargs['source']}/{kwargs['page_number']}.json.gz", 123)


class FakeSecrets:
    def __init__(self, value=None):
        self.value = value or {}

    def get_json(self, secret_arn, required=True):
        if required and not secret_arn:
            raise ConfigurationError("missing secret")
        return self.value


class FakeHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def get_json(self, url, headers=None, min_interval_seconds=0.0, before_attempt=None):
        if before_attempt is not None:
            before_attempt()
        self.urls.append((url, headers or {}))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get_bytes(self, url, headers=None, before_attempt=None):
        if before_attempt is not None:
            before_attempt()
        self.urls.append((url, headers or {}))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class SourceTests(unittest.TestCase):
    def test_split_upstox_ranges_at_ten_year_boundary(self):
        chunks = list(split_upstox_daily_ranges(date(2010, 1, 1), date(2026, 10, 30)))
        self.assertEqual(chunks[0], (date(2010, 1, 1), date(2019, 12, 31)))
        self.assertEqual(chunks[1], (date(2020, 1, 1), date(2026, 10, 30)))

    def test_incremental_range_uses_overlap(self):
        start, end, advance = resolve_date_range(
            {"overlap_days": 3, "end_date": "2026-10-30"},
            {"last_successful_date": "2026-10-28"},
        )
        self.assertEqual(start, date(2026, 10, 25))
        self.assertEqual(end, date(2026, 10, 30))
        self.assertTrue(advance)

    def test_first_incremental_requires_bootstrap(self):
        with self.assertRaises(ConfigurationError):
            resolve_date_range({}, {})

    def test_bootstrap_start_date_is_used_only_without_a_watermark(self):
        start, end, advance = resolve_date_range(
            {"bootstrap_start_date": "2025-01-01", "end_date": "2026-10-30"}, {}
        )
        self.assertEqual(start.isoformat(), "2025-01-01")
        self.assertEqual(end.isoformat(), "2026-10-30")
        self.assertTrue(advance)

        start, end, advance = resolve_date_range(
            {"bootstrap_start_date": "2020-01-01", "end_date": "2026-10-30", "overlap_days": 3},
            {"last_successful_date": "2026-10-27"},
        )
        self.assertEqual(start.isoformat(), "2026-10-24")
        self.assertTrue(advance)

    def test_explicit_backfill_does_not_advance_by_default(self):
        _, _, advance = resolve_date_range(
            {"start_date": "2020-01-01", "end_date": "2020-02-01"}, {}
        )
        self.assertFalse(advance)

    def test_expired_upstox_token_is_rejected(self):
        with patch("ingestion.sources.time.time", return_value=2_000_000_000):
            with self.assertRaises(ConfigurationError):
                _validate_upstox_token_expiry({"expires_at": 1_900_000_000_000})

    def test_upstox_token_with_future_expiry_is_accepted(self):
        with patch("ingestion.sources.time.time", return_value=2_000_000_000):
            _validate_upstox_token_expiry({"expires_at": 2_100_000_000_000})

    def test_http_build_url_omits_none_values(self):
        url = HttpClient.build_url("https://example.test/api", query={"a": 1, "b": None, "q": "x y"})
        self.assertEqual(url, "https://example.test/api?a=1&q=x+y")

    def test_upstox_extract_writes_raw_response(self):
        writer = FakeWriter()
        runtime = IngestionRuntime(
            http=FakeHttp([{"status": "success", "data": {"candles": [["2026-10-29T00:00:00+05:30", 1, 2, 1, 2, 5, 0]]}}]),
            writer=writer,
            secrets=FakeSecrets({"access_token": "not-a-real-token"}),
            checkpoints=None,
        )
        with patch.dict(os.environ, {"UPSTOX_SECRET_ARN": "test-upstox-secret"}):
            result = _extract_upstox(
                {"instrument_key": "NSE_EQ|INE002A01018", "start_date": "2026-10-29",
                 "end_date": "2026-10-29", "advance_watermark": True},
                runtime, "test-run", "2026-10-31", {},
            )
        self.assertEqual(result["record_count"], 1)
        self.assertTrue(result["advance_watermark"])
        self.assertEqual(len(writer.calls), 1)
        self.assertIn("historical-candle/NSE_EQ%7CINE002A01018/days/1/2026-10-29/2026-10-29", runtime.http.urls[0][0])
        self.assertEqual(runtime.http.urls[0][1]["Authorization"], "Bearer not-a-real-token")

    def test_upstox_instrument_master_extracts_public_gzip_feed(self):
        records = [{"instrument_key": "NSE_EQ|INE002A01018", "segment": "NSE_EQ",
                    "trading_symbol": "RELIANCE", "name": "Reliance Industries"}]
        response = gzip.compress(json.dumps(records).encode("utf-8"))
        writer = FakeWriter()
        limiter_calls = []
        runtime = IngestionRuntime(
            http=FakeHttp([response]), writer=writer, secrets=FakeSecrets(), checkpoints=None,
            rate_limiter=type("Limiter", (), {"wait": lambda self, provider, interval: limiter_calls.append((provider, interval))})(),
        )
        result = _extract_upstox_instrument_master({}, runtime, "master-run", "2026-11-02")
        self.assertEqual(result["record_count"], 1)
        self.assertEqual(writer.calls[0]["entity"], "instrument_master")
        self.assertEqual(writer.calls[0]["response_payload"], records)
        self.assertEqual(limiter_calls[0][0], "upstox")
        self.assertIn("complete.json.gz", runtime.http.urls[0][0])
        self.assertEqual(checkpoint_key({"source": "upstox", "entity": "instrument_master"}),
                         "UPSTOX#INSTRUMENT_MASTER#complete")

    def test_upstox_instrument_master_rejects_missing_instrument_key(self):
        runtime = IngestionRuntime(http=FakeHttp([json.dumps([{"segment": "NSE_EQ"}]).encode()]),
                                   writer=FakeWriter(), secrets=FakeSecrets(), checkpoints=None)
        from ingestion.sources import _extract_upstox_instrument_master
        with self.assertRaises(Exception):
            _extract_upstox_instrument_master({}, runtime, "master-run", "2026-11-02")

    def test_coingecko_market_snapshot_uses_demo_key_when_configured(self):
        writer = FakeWriter()
        http = FakeHttp([[{"id": "bitcoin", "current_price": 6000000}]
        ])
        runtime = IngestionRuntime(
            http=http, writer=writer, secrets=FakeSecrets({"api_key": "demo-key"}), checkpoints=None
        )
        with patch.dict(os.environ, {"COINGECKO_SECRET_ARN": "test-coingecko-secret"}):
            from ingestion.sources import _extract_coingecko
            result = _extract_coingecko(
                {"vs_currency": "inr", "per_page": 50}, runtime, "test-run", "2026-10-31"
            )
        self.assertEqual(result["record_count"], 1)
        self.assertEqual(http.urls[0][1]["x-cg-demo-api-key"], "demo-key")
        self.assertIn("coins/markets", http.urls[0][0])

    def test_frankfurter_request(self):
        writer = FakeWriter()
        http = FakeHttp([[{"date": "2026-10-29", "base": "USD", "quote": "INR", "rate": 88.0}]])
        runtime = IngestionRuntime(http=http, writer=writer, secrets=FakeSecrets(), checkpoints=None)
        result = _extract_frankfurter(
            {"base": "USD", "quotes": ["INR"], "start_date": "2026-10-29",
             "end_date": "2026-10-29", "advance_watermark": True},
            runtime, "test-run", "2026-10-31", {},
        )
        self.assertEqual(result["record_count"], 1)
        self.assertIn("https://api.frankfurter.dev/v2/rates?", http.urls[0][0])

    def test_rbi_paginates_until_short_page(self):
        writer = FakeWriter()
        metadata = {"title": "Test table", "columns": []}
        page_one = {"rows": [{"period": "2026-01-01"}, {"period": "2026-02-01"}]}
        page_two = {"rows": [{"period": "2026-03-01"}]}
        http = FakeHttp([metadata, page_one, page_two])
        runtime = IngestionRuntime(http=http, writer=writer, secrets=FakeSecrets(), checkpoints=None)
        result = _extract_rbi_rows(
            {"schema": "macro", "table": "cpi", "start_date": "2026-01-01",
             "end_date": "2026-03-31", "page_size": 2, "max_pages": 3,
             "advance_watermark": True},
            runtime, "test-run", "2026-10-31", {},
        )
        self.assertEqual(result["record_count"], 3)
        self.assertEqual(result["pages"], 2)
        self.assertEqual(len(writer.calls), 3)  # metadata + two row pages

    def test_rbi_stops_at_max_pages_without_claiming_completion(self):
        writer = FakeWriter()
        http = FakeHttp([{"title": "Test"}, {"rows": [{"id": 1}, {"id": 2}]}, {"rows": [{"id": 3}, {"id": 4}]}])
        runtime = IngestionRuntime(http=http, writer=writer, secrets=FakeSecrets(), checkpoints=None)
        with self.assertRaises(PaginationLimitError):
            _extract_rbi_rows(
                {"schema": "macro", "table": "large", "start_date": "2026-01-01",
                 "end_date": "2026-10-30", "page_size": 2, "max_pages": 2},
                runtime, "test-run", "2026-10-31", {},
            )

    def test_rbi_checkpoint_key_includes_filter_hash(self):
        key_a = checkpoint_key({"source": "rbi", "entity": "table_rows", "schema": "macro", "table": "cpi", "filters": {"region": "IN"}})
        key_b = checkpoint_key({"source": "rbi", "entity": "table_rows", "schema": "macro", "table": "cpi", "filters": {"region": "US"}})
        self.assertNotEqual(key_a, key_b)

    def test_checkpoint_keys_are_source_scoped(self):
        self.assertEqual(
            checkpoint_key({"source": "upstox", "entity": "equity_ohlcv", "instrument_key": "NSE_EQ|ABC"}),
            "UPSTOX#EQUITY_OHLCV#NSE_EQ_ABC",
        )


if __name__ == "__main__":
    unittest.main()
