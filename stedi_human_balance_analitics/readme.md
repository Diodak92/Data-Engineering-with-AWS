# STEDI Human Balance Lakehouse

AWS Glue jobs that build a lakehouse for STEDI’s step-trainer data. The project moves JSON landing data in S3 through trusted and curated zones so it can be used for machine learning features that combine customer, accelerometer, and step-trainer readings.

## What’s Inside
- Glue ETL scripts in `glue_jobs/` for each zone to zone.
- Table definitions for the landing zone in `glue_DDL/`.
- `setup_glue_s3.sh` to pull the public STEDI dataset, push it to your S3 bucket, and create an IAM role for Glue.
- `screenshots/` for reference when configuring jobs and crawlers.

## Prerequisites
- AWS account with permission to create S3 buckets, IAM roles/policies, and Glue jobs.
- AWS CLI installed and configured (`aws configure`) with your target region.
- Git installed locally.

## Quick Start
1. **Set your bucket/role names**  
   Edit `setup_glue_s3.sh` and adjust `BUCKET_NAME`, `ROLE_NAME`, and `REGION`. If you change the bucket, also update the `LOCATION` paths in `glue_DDL/*.sql`.
2. **Stage the landing data & IAM role**  
   ```bash
   sh setup_glue_s3.sh
   ```
   The script clones the public dataset, uploads it to `s3://<bucket>/{accelerometer,customer,step_trainer}/landing/`, and creates an IAM role with S3 + Glue access.
3. **Create the Glue database and landing tables**  
   In Athena or the Glue console, create a database (e.g., `stedi_db`) and run the SQL in `glue_DDL/` to register `customer_landing`, `accelerometer_landing`, and `step_trainer_landing`.
4. **Create Glue jobs from the scripts**  
   Add a job per file in `glue_jobs/`, using the IAM role from step 2. All scripts expect the `stedi_db` database and the landing tables above.
5. **Run jobs in the following order**
   1) `customer_landing_to_trusted.py` – keep customers who opted into research.  
   2) `accelerometer_landing_to_trusted.py` – keep accelerometer rows for known customers.  
   3) `customer_trusted_to_curated.py` – customers with matching accelerometer data.  
   4) `step_trainer_trusted.py` – step-trainer rows tied to curated customers.  
   5) `machine_learning_curated.py` – feature set joining trusted accelerometer and step-trainer readings.

## Outputs by Zone
- `accelerometer_trusted`, `customer_trusted`, `step_trainer_trusted`: cleaned data scoped to known customers/devices.
- `customer_curated`: customers with both identity and accelerometer data.
- `machine_learning_curated`: joined telemetry (user, serialnumber, timestamp, distanceFromObject, x, y, z) for model training.

## Validation Tips
- In AWS Athena, query a few rows from each trusted/curated table to confirm row counts rise as expected across stages.
- Check Glue job run logs for catalog update success and record counts.
- Confirm S3 prefixes for `trusted/` and `curated/` are being populated after each job run.
