#!/usr/bin/env python3
import json
import os
import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "tf_outputs.json"
S3_LOG_DATA = "s3://udacity-dend/log_data",
S3_SONG_DATA = "s3://udacity-dend/song-data"


def load_terraform_outputs():
    """
    Prefer reading fresh values via `terraform output -json`.
    Fall back to tf_outputs.json only if it already exists.
    """
    if OUTPUT_FILE.exists():
        with OUTPUT_FILE.open(encoding="utf-8") as f:
            return json.load(f)

    cmd = ["terraform", "output", "-json"]
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "Failed to run `terraform output -json`. "
            "Did you run `terraform apply` inside infra/?"
        ) from exc

    return json.loads(result.stdout)


def require_output(key: str):
    try:
        return outputs[key]["value"]
    except KeyError as exc:  # provide actionable guidance
        raise RuntimeError(
            f"Terraform output '{key}' not found. "
            "Make sure Terraform applied successfully."
        ) from exc


outputs = load_terraform_outputs()
workgroup = require_output("workgroup_name")
namespace = require_output("namespace_name")
connection_details = require_output("connection_details")

endpoint = connection_details["host"]
port = connection_details["port"]
schema = connection_details["database"]

# Connection parameters (prefer Terraform outputs, fall back to env for backwards compatibility)
conn_id = "redshift_serverless"
conn_type = "redshift"
login = connection_details.get("username") or os.getenv("TF_VAR_admin_username")
password = connection_details.get("password") or os.getenv("TF_VAR_admin_password")

if not all([endpoint, port, schema, login, password]):
    raise RuntimeError("Missing Redshift connection details. Did Terraform finish applying?")

# Build connection URI
conn_uri = f"redshift://{login}:{password}@{endpoint}:{port}/{schema}"

# Name of your Airflow container (check with `docker ps`)
airflow_container = "airflow-docker-airflow-webserver-1"

# Run airflow CLI inside the container
subprocess.run([
    "docker", "exec", airflow_container,
    "airflow", "connections", "add", conn_id,
    "--conn-uri", conn_uri
], check=True)

print(f"Airflow connection '{conn_id}' created inside container '{airflow_container}' "
      f"for Redshift workgroup {workgroup} in namespace {namespace}")

print("Copying S3 data from Udacity bucket")
subprocess.run(
    ["aws", "s3", "cp", 
     "s3://udacity-dend/log-data/",
     "s3://tomasz-temp-bucket/log-data/",
     "--recursive"],
     check=True)
subprocess.run(
    ["aws", "s3", "cp",
     "s3://udacity-dend/song-data/",
     "s3://tomasz-temp-bucket/song-data/",
     "--recursive"],
     check=True)
subprocess.run(
    ["aws", "s3", "cp",
     "s3://udacity-dend/log_json_path.json",
     "s3://tomasz-temp-bucket/",
     "--recursive"],
     check=True)