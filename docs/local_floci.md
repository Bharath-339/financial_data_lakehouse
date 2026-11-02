# Local development using Floci

This project uses Floci as the AWS API emulator. The Spark code is the same code used by AWS Glue; Floci is only the local S3/DynamoDB/Secrets Manager/Glue Catalog control-plane endpoint.

## Important Floci Glue limitation

Floci emulates the Glue Data Catalog and Glue job control-plane APIs, but its `StartJobRun` does not execute the PySpark script. Therefore:

- Use Floci CloudFormation for the Lambda, S3, DynamoDB, secrets, IAM/logging resources supported by its CloudFormation implementation.
- Create local Glue databases using `scripts/bootstrap_catalog.py` (the Glue API).
- Run the actual PySpark scripts with local `spark-submit` using `scripts/run_local_spark.sh`.
- Use the separate AWS-only `infra/template.aws-glue.yaml` to create real AWS Glue jobs.

Don't use Floci `StartJobRun` as evidence that a Spark transformation completed.

## Prerequisites

- Floci running at `http://localhost:4566` with its S3, DynamoDB, Secrets Manager, Lambda, CloudFormation, Glue and Athena services enabled.
- For Floci Athena queries, enable its Athena/DuckDB sidecar and mount the Docker socket as required by the Floci documentation.
- AWS CLI v2, `curl`, Python 3.11/3.12, Java 17 or 21 and `spark-submit` backed by PySpark 3.5.6.
- Local Java/Spark dependency resolution requires access to Maven Central once, or pre-populated Ivy/Maven caches.

## 1. Configure the local shell

```bash
cp .env.floci.example .env.floci
# Edit .env.floci only if your Floci endpoint/credentials differ.
source scripts/floci_env.sh
```

`AWS_ENDPOINT_URL` is the host endpoint used by local Python, boto3, and Spark. The CloudFormation parameter `AwsEndpointUrl` is separately set to `http://host.docker.internal:4566` by default so a Lambda container can call the host Floci process on macOS/Windows Docker Desktop. On Linux, set `LAMBDA_AWS_ENDPOINT_URL` to a host-gateway address or Floci container network address that is reachable from the Lambda runtime container.

## 2. Deploy ingestion resources to Floci

```bash
./scripts/deploy_local_floci.sh
```

The script creates an artifact bucket, deploys `infra/template.yaml`, reads generated bucket/table names from CloudFormation outputs, and writes `.env.floci.generated`. It does not deploy the AWS Glue job template, because Floci CloudFormation does not implement `AWS::Glue::Database`/`AWS::Glue::Job` as executable resources.

## 3. Bootstrap Glue Catalog and reference data

```bash
source scripts/floci_env.sh
./scripts/local_init_floci.sh
```

The script checks `/_floci/health`, creates the lakehouse bucket if it is not present, creates `lakehouse_silver` and `lakehouse_gold` using the Glue API, and uploads the reviewed reference CSVs to `s3a://<bucket>/reference/`.

Populate test secrets in Floci Secrets Manager by exporting `UPSTOX_ACCESS_TOKEN` and/or `COINGECKO_DEMO_API_KEY`, then running `scripts/set_floci_secrets.sh`. Use test credentials only. Upstox access tokens expire and need to be refreshed using the supported OAuth flow; never commit them to `.env.floci` or Git.

## 4. Install local packages

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-local.txt
```

The local Spark runner uses Spark 3.5.6 with Iceberg 1.10.0 and Hadoop AWS 3.3.4 by default. If your Spark distribution was built with a different Hadoop version, override `HADOOP_AWS_VERSION` to the matching version. Maven artifacts are fetched on first use unless already cached.

## 5. Run ingestion

After populating the local Upstox and CoinGecko secrets:

```bash
./scripts/run_local_pipeline.sh
```

This invokes the public Upstox BOD instrument master, Upstox equity candles, CoinGecko, Frankfurter and the RBI catalogue, validates the resulting Bronze envelopes, runs Bronze-to-Silver for each source (including the master snapshot and discovered RBI catalogue), and builds Gold. It deliberately does not run the sample RBI table event because its schema/table/filter values are examples, not guaranteed valid DBIE identifiers.

For step-by-step development:

```bash
aws lambda invoke --function-name <function-name> \
  --cli-binary-format raw-in-base64-out \
  --payload fileb://events/coingecko_daily.json /tmp/coingecko.json

python scripts/validate_bronze.py --bucket "$RAW_BUCKET" --source coingecko --entity market_snapshot

./scripts/run_local_spark.sh bronze-to-silver --source coingecko --entity market_snapshot
./scripts/run_local_spark.sh silver-to-gold
```

The runner accepts the same `--source`, `--entity`, `--extract-date`, `--run-id`, `--mode` and database arguments used by Glue jobs.

## 6. Query Iceberg tables

Once PySpark successfully writes Iceberg metadata and registers it with Floci Glue Catalog, use Floci Athena with database `lakehouse_gold` and the SQL files under `sql/analytics/`. Its Athena implementation uses DuckDB as the query engine, and Iceberg support depends on the registered `metadata_location` being resolvable by the Athena sidecar.

If local Iceberg table registration/metadata resolution differs from AWS behavior for your Floci version, inspect the table entry with `aws glue get-table --database-name lakehouse_gold --name mart_asset_class_daily`; do not treat a successful Glue database bootstrap alone as proof of an Iceberg write.

## 7. Local test commands

```bash
python -m compileall -q lambda_src lakehouse_src glue_jobs scripts examples tests
python -m unittest discover -s tests -v
```

The Spark transformation tests in `test_spark_transformations.py` are skipped when PySpark is not installed. Run them after installing `requirements-local.txt`. Full Iceberg + Floci endpoint integration still needs a running Floci instance and Spark dependencies; it is not part of the offline unit-test suite.
