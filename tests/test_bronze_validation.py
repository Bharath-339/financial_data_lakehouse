import gzip
import json
import unittest
import os
import sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "lakehouse_src"))
from lakehouse.bronze_validation import validate_envelope, validate_prefix


class BronzeEnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.envelope = {
            "metadata": {
                "source": "upstox", "entity": "equity_ohlcv", "entity_key": "NSE_EQ|INE002A01018",
                "run_id": "run-001", "extract_date": "2026-11-01",
                "ingested_at": "2026-11-01T12:00:00Z", "request": {"path": "/v3/historical-candle"},
            },
            "response": {"status": "success", "data": {"candles": []}},
        }

    def test_valid_gzip_envelope(self):
        raw = gzip.compress(json.dumps(self.envelope).encode())
        decoded, errors = validate_envelope(raw, expected_source="upstox", expected_entity="equity_ohlcv")
        self.assertEqual(errors, [])
        self.assertEqual(decoded["metadata"]["run_id"], "run-001")

    def test_path_mismatch_is_rejected(self):
        raw = json.dumps(self.envelope).encode()
        _, errors = validate_envelope(raw, expected_source="coingecko", expected_entity="market_snapshot")
        self.assertIn("source_path_mismatch", errors)
        self.assertIn("entity_path_mismatch", errors)

    def test_missing_response_is_rejected(self):
        envelope = dict(self.envelope)
        del envelope["response"]
        _, errors = validate_envelope(json.dumps(envelope).encode())
        self.assertIn("response_missing", errors)

    def test_malformed_gzip_or_json_is_rejected(self):
        _, errors = validate_envelope(b"not-json")
        self.assertTrue(errors[0].startswith("invalid_json_or_gzip:"))


    def test_validate_prefix_filters_run_id_inside_extract_date_partition(self):
        class Body:
            def __init__(self, value): self.value = value
            def read(self): return self.value

        envelope = self.envelope
        class FakeS3:
            def __init__(self):
                self.objects = {
                    "bronze/upstox/equity_ohlcv/extract_date=2026-11-02/run_id=run-a/x/part.json.gz":
                        gzip.compress(json.dumps(envelope).encode()),
                    "bronze/upstox/equity_ohlcv/extract_date=2026-11-02/run_id=run-b/x/part.json.gz":
                        gzip.compress(json.dumps(envelope).encode()),
                }
                self.puts = []
            def get_paginator(self, _name):
                outer = self
                class Paginator:
                    def paginate(self, Bucket, Prefix):
                        yield {"Contents": [{"Key": key} for key in outer.objects if key.startswith(Prefix)]}
                return Paginator()
            def get_object(self, Bucket, Key): return {"Body": Body(self.objects[Key])}
            def put_object(self, **kwargs): self.puts.append(kwargs)

        fake = FakeS3()
        report = validate_prefix(s3=fake, bucket="bucket", source="upstox", entity="equity_ohlcv",
                                 run_date="2026-11-02", run_id_filter="run-a")
        self.assertEqual(report["object_count"], 1)
        self.assertEqual(report["valid_object_count"], 1)

    def test_unsupported_entity_is_rejected(self):
        envelope = json.loads(json.dumps(self.envelope))
        envelope["metadata"]["entity"] = "something_else"
        _, errors = validate_envelope(json.dumps(envelope).encode())
        self.assertIn("unsupported_source_entity", errors)


if __name__ == "__main__":
    unittest.main()
