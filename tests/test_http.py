import os
import sys
import unittest
from email.message import Message
from io import BytesIO
from unittest.mock import Mock, patch
from urllib.error import HTTPError

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "lambda_src"))

from ingestion.http import HttpClient


class FakeResponse:
    def __init__(self, body=b'{"ok": true}'):
        self.body = body
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False
    def read(self):
        return self.body


class HttpClientTests(unittest.TestCase):
    def test_before_attempt_runs_again_for_retry(self):
        headers = Message()
        rate_limit = HTTPError("https://example.test/data", 429, "Too Many Requests", headers, BytesIO(b"slow down"))
        before_attempt = Mock()
        client = HttpClient(timeout_seconds=1, max_retries=1)

        with patch("ingestion.http.urllib.request.urlopen", side_effect=[rate_limit, FakeResponse()]), \
             patch("ingestion.http.time.sleep"):
            result = client.get_json("https://example.test/data", before_attempt=before_attempt)

        self.assertEqual(result, {"ok": True})
        self.assertEqual(before_attempt.call_count, 2)

    def test_binary_get_retries_and_calls_before_attempt(self):
        headers = Message()
        rate_limit = HTTPError("https://example.test/master", 429, "Too Many Requests", headers, BytesIO(b"slow down"))
        before_attempt = Mock()
        payload = b"\x1f\x8bcompressed-gzip-payload"
        client = HttpClient(timeout_seconds=1, max_retries=1)
        with patch("ingestion.http.urllib.request.urlopen", side_effect=[rate_limit, FakeResponse(payload)]), \
             patch("ingestion.http.time.sleep"):
            result = client.get_bytes("https://example.test/master.gz", before_attempt=before_attempt)
        self.assertEqual(result, payload)
        self.assertEqual(before_attempt.call_count, 2)


if __name__ == "__main__":
    unittest.main()
