"""Optional local Spark tests. Run with pyspark installed and Java available."""
import os
import unittest
import sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "lakehouse_src"))

try:
    from pyspark.sql import SparkSession, types as T
    from lakehouse.spark.bronze_to_silver import (transform_upstox, transform_upstox_instrument_master,
        transform_frankfurter, transform_rbi_catalogue, transform_rbi_metadata)
    from lakehouse.spark.silver_to_gold import _add_scd2_columns
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False


@unittest.skipUnless(HAS_SPARK, "Install pyspark==3.5.6 to run Spark transformation tests")
class SparkTransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("lakehouse-transform-tests")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        if HAS_SPARK:
            cls.spark.stop()

    def _metadata_schema(self):
        return T.StructType([
            T.StructField("source", T.StringType()), T.StructField("entity", T.StringType()),
            T.StructField("entity_key", T.StringType()), T.StructField("run_id", T.StringType()),
            T.StructField("extract_date", T.StringType()), T.StructField("ingested_at", T.StringType()),
            T.StructField("request", T.MapType(T.StringType(), T.StringType())),
        ])

    def test_upstox_candle_date_preserves_ist_date_and_validates_ohlc(self):
        candle_schema = T.ArrayType(T.ArrayType(T.StringType()))
        response_schema = T.StructType([T.StructField("status", T.StringType()),
            T.StructField("data", T.StructType([T.StructField("candles", candle_schema)]))])
        schema = T.StructType([T.StructField("metadata", self._metadata_schema()),
                               T.StructField("response", response_schema), T.StructField("bronze_source_file", T.StringType())])
        rows = [({"source":"upstox","entity":"equity_ohlcv","entity_key":"NSE_EQ|ABC","run_id":"r1",
                  "extract_date":"2026-11-01","ingested_at":"2026-11-01T12:00:00Z","request":{}},
                 {"status":"success","data":{"candles":[["2026-10-30T00:00:00+05:30","10","12","9","11","100","0"]]}}, "s3://bucket/object.json.gz")]
        source = self.spark.createDataFrame(rows, schema)
        valid, invalid = transform_upstox(source)
        self.assertEqual(valid.count(), 1)
        self.assertEqual(invalid.count(), 0)
        self.assertEqual(str(valid.first()["trading_date"]), "2026-10-30")

    def test_upstox_instrument_master_normalises_stable_instrument_key(self):
        item_schema = T.StructType([
            T.StructField("segment", T.StringType()),
            T.StructField("name", T.StringType()),
            T.StructField("exchange", T.StringType()),
            T.StructField("isin", T.StringType()),
            T.StructField("instrument_type", T.StringType()),
            T.StructField("instrument_key", T.StringType()),
            T.StructField("lot_size", T.LongType()),
            T.StructField("exchange_token", T.StringType()),
            T.StructField("tick_size", T.DoubleType()),
            T.StructField("trading_symbol", T.StringType()),
            T.StructField("short_name", T.StringType()),
            T.StructField("security_type", T.StringType()),
        ])
        schema = T.StructType([
            T.StructField("metadata", self._metadata_schema()),
            T.StructField("response", T.ArrayType(item_schema)),
            T.StructField("bronze_source_file", T.StringType()),
        ])
        rows = [({"source":"upstox", "entity":"instrument_master", "entity_key":"complete",
                  "run_id":"r1", "extract_date":"2026-11-02", "ingested_at":"2026-11-02T06:15:00Z",
                  "request":{}}, [{"segment":"NSE_EQ", "name":"Reliance Industries Limited",
                  "exchange":"NSE", "isin":"INE002A01018", "instrument_type":"EQ",
                  "instrument_key":"NSE_EQ|INE002A01018", "lot_size":1, "exchange_token":"2885",
                  "tick_size":0.05, "trading_symbol":"RELIANCE", "short_name":"RELIANCE",
                  "security_type":"NORMAL"}], "s3://bucket/master.json.gz")]
        valid, invalid = transform_upstox_instrument_master(self.spark.createDataFrame(rows, schema))
        self.assertEqual(valid.count(), 1)
        self.assertEqual(invalid.count(), 0)
        record = valid.first()
        self.assertEqual(record["instrument_key"], "NSE_EQ|INE002A01018")
        self.assertEqual(record["segment"], "NSE_EQ")
        self.assertEqual(str(record["master_date"]), "2026-11-02")

    def test_rbi_catalogue_preserves_discovered_table_rows(self):
        metadata_schema = self._metadata_schema()
        catalogue_item = T.StructType([
            T.StructField("schema", T.StringType()),
            T.StructField("table", T.StringType()),
            T.StructField("title", T.StringType()),
            T.StructField("description", T.StringType()),
        ])
        schema = T.StructType([
            T.StructField("metadata", metadata_schema),
            T.StructField("response", T.StructType([T.StructField("tables", T.ArrayType(catalogue_item))])),
            T.StructField("bronze_source_file", T.StringType()),
        ])
        rows = [({"source":"rbi", "entity":"table_catalogue", "entity_key":"all-tables",
                  "run_id":"r1", "extract_date":"2026-11-02", "ingested_at":"2026-11-02T06:00:00Z",
                  "request":{}}, {"tables":[{"schema":"financial_sector", "table":"repo_rate",
                  "title":"Repo rate", "description":"Policy rate"}]}, "s3://bucket/catalogue.json.gz")]
        valid, review = transform_rbi_catalogue(self.spark.createDataFrame(rows, schema))
        self.assertEqual(valid.count(), 1)
        self.assertEqual(review.count(), 0)
        self.assertEqual(valid.first()["rbi_table"], "repo_rate")

    def test_rbi_catalogue_missing_schema_is_quarantined_for_review(self):
        metadata_schema = self._metadata_schema()
        catalogue_item = T.StructType([
            T.StructField("schema", T.StringType()), T.StructField("table", T.StringType()),
            T.StructField("title", T.StringType()), T.StructField("description", T.StringType()),
        ])
        schema = T.StructType([
            T.StructField("metadata", metadata_schema),
            T.StructField("response", T.StructType([T.StructField("tables", T.ArrayType(catalogue_item))])),
            T.StructField("bronze_source_file", T.StringType()),
        ])
        rows = [({"source":"rbi", "entity":"table_catalogue", "entity_key":"all-tables",
                  "run_id":"r2", "extract_date":"2026-11-02", "ingested_at":"2026-11-02T06:00:00Z",
                  "request":{}}, {"tables":[{"schema":None, "table":"repo_rate",
                  "title":"Repo rate", "description":"Policy rate"}]}, "s3://bucket/catalogue.json.gz")]
        valid_and_review, review = transform_rbi_catalogue(self.spark.createDataFrame(rows, schema))
        self.assertEqual(valid_and_review.count(), 1)
        self.assertEqual(review.count(), 1)
        self.assertEqual(review.first()["dq_status"], "REVIEW")
        self.assertIn("catalogue_schema_identifier_missing", review.first()["dq_reason"])

    def test_scd2_initial_effective_date_uses_first_observation(self):
        base = self.spark.createDataFrame(
            [("NSE_EQ|ABC", "ABC", "ABC Ltd", "2026-01-02", "2026-02-01")],
            ["instrument_key", "symbol", "company_name", "first_seen_date", "change_effective_date"],
        ).withColumn("first_seen_date", __import__("pyspark.sql.functions", fromlist=["to_date"]).to_date("first_seen_date"))
        staged = _add_scd2_columns(base, "instrument_key", ["symbol", "company_name"],
                                   "first_seen_date", None, "instrument_sk")
        self.assertEqual(str(staged.first()["effective_from"]), "2026-01-02")
        self.assertNotIn("change_effective_date", staged.columns)

    def test_scd2_changed_instrument_uses_master_effective_date(self):
        from pyspark.sql import functions as F
        base = self.spark.createDataFrame(
            [("NSE_EQ|ABC", "XYZ", "XYZ Ltd", "2026-01-02", "2026-02-01")],
            ["instrument_key", "symbol", "company_name", "first_seen_date", "change_effective_date"],
        ).withColumn("first_seen_date", F.to_date("first_seen_date")).withColumn(
            "change_effective_date", F.to_date("change_effective_date"))
        old_hash = self.spark.createDataFrame(
            [("NSE_EQ|ABC", "old-hash", "2026-01-02", True)],
            ["instrument_key", "attribute_hash", "effective_from", "is_current"],
        ).withColumn("effective_from", F.to_date("effective_from"))
        staged = _add_scd2_columns(base, "instrument_key", ["symbol", "company_name"],
                                   "first_seen_date", old_hash, "instrument_sk")
        self.assertEqual(str(staged.first()["effective_from"]), "2026-02-01")

    def test_rbi_metadata_keeps_document_hash_and_table_key(self):
        response_schema = T.StructType([T.StructField("columns", T.ArrayType(T.StringType()))])
        schema = T.StructType([
            T.StructField("metadata", self._metadata_schema()),
            T.StructField("response", response_schema),
            T.StructField("bronze_source_file", T.StringType()),
        ])
        rows = [({"source":"rbi", "entity":"table_metadata", "entity_key":"financial_sector.repo_rate",
                  "run_id":"r1", "extract_date":"2026-11-02", "ingested_at":"2026-11-02T06:00:00Z",
                  "request":{}}, {"columns":["period", "value"]}, "s3://bucket/metadata.json.gz")]
        valid, review = transform_rbi_metadata(self.spark.createDataFrame(rows, schema))
        self.assertEqual(valid.count(), 1)
        self.assertEqual(review.count(), 0)
        record = valid.first()
        self.assertEqual(record["rbi_schema"], "financial_sector")
        self.assertEqual(record["rbi_table"], "repo_rate")
        self.assertTrue(record["metadata_hash"])

    def test_frankfurter_normalises_currency_codes(self):
        rate_schema = T.ArrayType(T.StructType([T.StructField("date", T.StringType()),
            T.StructField("base", T.StringType()), T.StructField("quote", T.StringType()),
            T.StructField("rate", T.DoubleType())]))
        schema = T.StructType([T.StructField("metadata", self._metadata_schema()),
                               T.StructField("response", rate_schema), T.StructField("bronze_source_file", T.StringType())])
        rows = [({"source":"frankfurter","entity":"daily_rates","entity_key":"USD_INR","run_id":"r1",
                  "extract_date":"2026-11-01","ingested_at":"2026-11-01T12:00:00Z","request":{}},
                 [{"date":"2026-10-30","base":"usd","quote":"inr","rate":83.2}], "s3://bucket/object.json.gz")]
        valid, invalid = transform_frankfurter(self.spark.createDataFrame(rows, schema))
        self.assertEqual(valid.count(), 1)
        self.assertEqual(invalid.count(), 0)
        self.assertEqual(valid.first()["base_currency"], "USD")
        self.assertEqual(valid.first()["quote_currency"], "INR")


if __name__ == "__main__":
    unittest.main()
