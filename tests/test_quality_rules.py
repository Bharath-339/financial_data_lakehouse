import unittest
import os
import sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "lakehouse_src"))
from lakehouse.quality_rules import to_decimal, validate_ohlcv, validate_fx, validate_crypto, rbi_field_candidates


class QualityRulesTests(unittest.TestCase):
    def test_decimal_normalises_commas(self):
        self.assertEqual(str(to_decimal("1,234.50")), "1234.50")
        self.assertIsNone(to_decimal("not-a-number"))

    def test_valid_ohlcv(self):
        row = {"instrument_key": "NSE_EQ|ABC", "trading_date": "2026-10-30",
               "open_price": "10", "high_price": "15", "low_price": "9", "close_price": "13", "volume": 100}
        self.assertEqual(validate_ohlcv(row), [])

    def test_invalid_ohlcv_detects_high_low(self):
        row = {"instrument_key": "ABC", "trading_date": "2026-10-30",
               "open_price": "10", "high_price": "11", "low_price": "12", "close_price": "13", "volume": -1}
        errors = validate_ohlcv(row)
        self.assertIn("high_price_below_ohlc", errors)
        self.assertIn("low_price_above_ohlc", errors)
        self.assertIn("volume_negative", errors)

    def test_fx_and_crypto_validation(self):
        self.assertEqual(validate_fx({"rate_date": "2026-10-30", "base_currency": "USD",
                                      "quote_currency": "INR", "exchange_rate": "83.1"}), [])
        self.assertEqual(validate_crypto({"crypto_id": "bitcoin", "snapshot_date": "2026-10-30",
                                          "current_price": "100"}), [])

    def test_rbi_aliases_are_explicit(self):
        aliases = rbi_field_candidates()
        self.assertIn("observation_date", aliases["period"])
        self.assertIn("obs_value", aliases["value"])


if __name__ == "__main__":
    unittest.main()
