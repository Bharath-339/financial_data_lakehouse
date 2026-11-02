#!/usr/bin/env python3
"""Create Glue Data Catalog namespaces. Works with AWS Glue or Floci Glue API."""
from __future__ import annotations
import os
import boto3
from botocore.exceptions import ClientError

region = os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))
endpoint = os.environ.get("AWS_ENDPOINT_URL")
client = boto3.client("glue", region_name=region, endpoint_url=endpoint)
for name, description in [
    (os.environ.get("SILVER_DATABASE", "lakehouse_silver"), "Financial Data Lakehouse Silver zone"),
    (os.environ.get("GOLD_DATABASE", "lakehouse_gold"), "Financial Data Lakehouse Gold zone"),
]:
    try:
        client.create_database(DatabaseInput={"Name": name, "Description": description})
        print(f"Created Glue database: {name}")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"AlreadyExistsException", "AlreadyExists"}:
            print(f"Glue database already exists: {name}")
        else:
            raise
