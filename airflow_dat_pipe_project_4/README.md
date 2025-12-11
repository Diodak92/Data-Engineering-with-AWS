# Airflow Data Pipeline with Redshift

Airflow demo project that stages event and song data from S3 into Amazon Redshift, builds fact/dimension tables, and runs data quality checks via custom operators. Helper scripts are included to create infrastructure, copy S3 data, add Airflow connections/variables, and create Redshift tables.

## Prerequisites
- Docker and Docker Compose installed locally.
- AWS CLI v2 configured (`aws configure`) with access to S3 and Redshift Serverless.
- Existing S3 bucket containing the raw data (log-data, song-data) and JSON path file.

## Infrastructure setup (Terraform)
1) From `infra/`:
   ```bash
   cd infra
   terraform init
   terraform apply
   ```
2) Outputs are cached to `infra/tf_outputs.json` for the helper scripts.
3) Terraform provisions:
   - Redshift Serverless namespace/workgroup
   - IAM user with access keys for Airflow
   - S3 bucket for the pipeline data

## Copy data into S3
1) Copy the Udacity DEND datasets into your project bucket:
   ```bash
   aws s3 sync s3://udacity-dend <your-bucket-name> 
   --exclude "*pagila/*"
   --exclude "*dac-data-pipelines/*"
   --delete
   --no-preserve-tags
   --fail-on-error=false
   ```
2) The bucket name comes from Terraform output `s3_bucket_name`.

## Create tables in Redshift
1) Run the table DDLs (default `create_tables.sql`) using Terraform-provided credentials:
   ```bash
   python infra/run_queries.py --sql-file create_tables.sql
   ```
   Uses `infra/tf_outputs.json` or runs `terraform output -json` if the cache is missing.
2) To create tables owned by your IAM DB user (avoid permission errors in the DAG), request temporary credentials:
   ```bash
   python infra/run_queries.py --use-iam --sql-file create_tables.sql
   # optional: --aws-profile <profile> --region <region> --workgroup-name <wg-name>
   ```
   `--use-iam` calls the Redshift Serverless `get_credentials` API and connects with those temporary IAM credentials before running the SQL.

## Airflow environment
1) Start Airflow locally:
   ```bash
   cd airflow-docker
   docker-compose up -d
   ```
2) Once the webserver is up, open http://localhost:8080 (default creds: `airflow` / `airflow` unless changed).

## Airflow Connections & Variables
1) After `docker-compose up -d`, run:
   ```bash
   python infra/add_airflow_con.py
   ```
2) This injects into the running Airflow webserver container:
   - Connection `redshift_serverless` (Data API) using Terraform outputs.
   - Connection `aws_credentials` with the IAM access keys Terraform created.
   - Variables: `S3_BUCKET` (name from Terraform) and `REDSHIFT_WORKGROUP`.
3) For manual setup, use Admin -> Connections/Variables in the UI with the same values.

## Running the DAG
1) Enable the DAG `final_project` in the Airflow UI.
2) Trigger a manual run. The flow:
   - Stage events and songs to Redshift staging tables.
   - Load fact/dimension tables.
   - Run parallel data quality checks.

## Custom operators
- `StageToRedshiftOperator`: loads from S3 to staging tables.
- `LoadFactOperator` / `LoadDimensionOperator`: insert/append or truncate-load into target tables.
- `DataQualityOperator`: executes SQL checks and evaluates results with simple expressions (e.g., `logic_test="== 0"` or `logic_test="value > 0"`).
