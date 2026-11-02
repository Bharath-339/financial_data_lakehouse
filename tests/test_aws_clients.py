import os
import sys
import unittest
from unittest.mock import patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "lambda_src"))

from ingestion.aws_clients import DistributedRateLimiter


class FakeTable:
    def __init__(self):
        self.items = {}
        self.updates = []

    def get_item(self, Key, ConsistentRead=False):
        item = self.items.get(Key["pk"])
        return {"Item": dict(item)} if item else {}

    def update_item(self, Key, UpdateExpression, ConditionExpression, ExpressionAttributeValues):
        pk = Key["pk"]
        old = self.items.get(pk, {})
        previous = old.get("next_allowed_ms")
        if ConditionExpression == "attribute_not_exists(next_allowed_ms)":
            if previous is not None:
                from botocore.exceptions import ClientError
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}}, "UpdateItem")
        elif ConditionExpression == "next_allowed_ms = :previous":
            if previous != ExpressionAttributeValues[":previous"]:
                from botocore.exceptions import ClientError
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}}, "UpdateItem")
        self.items[pk] = {
            "pk": pk,
            "next_allowed_ms": ExpressionAttributeValues[":next"],
            "updated_at": ExpressionAttributeValues[":now"],
        }
        self.updates.append(pk)
        return {}


class DistributedRateLimiterTests(unittest.TestCase):
    def test_reserves_sequential_slots_across_invocations(self):
        table = FakeTable()
        limiter = DistributedRateLimiter(table)
        with patch("ingestion.aws_clients.time.time", return_value=100.0), patch("ingestion.aws_clients.time.sleep") as sleep:
            limiter.wait("coingecko", 1.2)
            limiter.wait("coingecko", 1.2)
        self.assertEqual(table.items["RATE#COINGECKO"]["next_allowed_ms"], 102400)
        sleep.assert_called_once_with(1.2)
        self.assertEqual(table.updates, ["RATE#COINGECKO", "RATE#COINGECKO"])


if __name__ == "__main__":
    unittest.main()
