# Build and validation report

Build date: 2026-11-02

## Automated checks run in the build environment

- Python compilation: passed (`python -m compileall -q lambda_src lakehouse_src glue_jobs scripts examples tests`).
- Unit suite: 39 tests discovered; 31 passed and 8 Spark transformation tests were skipped because PySpark is not installed in the build environment.
- Shell syntax: passed (`bash -n scripts/*.sh`).
- CloudFormation YAML parsing: both templates parsed successfully (9 resources each).
- Lambda deployment ZIP integrity: passed (`unzip -t dist/financial-ingestion.zip`).
- Shared Spark library ZIP integrity: passed (`unzip -t dist/lakehouse-src.zip`).

## Not executed in this environment

- No live Floci server was available, so CloudFormation deployment, Lambda invocation, S3/DynamoDB/Glue API integration, and local Iceberg writes were not executed.
- PySpark and its dependencies could not be installed in this environment because package-network access was unavailable. The Spark tests are included in `.github/workflows/ci.yml` and run in CI with PySpark 3.5.6 and Java 17.
- No live Upstox, CoinGecko, Frankfurter, or RBI API calls were made.
- `cfn-lint` was not installed here. The GitHub Actions workflow includes a dedicated cfn-lint job; YAML parsing alone is not equivalent to a complete CloudFormation lint/deploy validation.

## Local development note

Floci's Glue control-plane emulation does not run the submitted Spark program. For local development, run the same Spark code using `scripts/run_local_spark.sh` against Floci S3 and Glue Catalog. In AWS, deploy the Glue jobs with `scripts/deploy_glue.sh` and run them through AWS Glue.
