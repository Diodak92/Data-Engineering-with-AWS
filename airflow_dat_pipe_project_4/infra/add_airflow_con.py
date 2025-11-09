#!/usr/bin/env python3
import json
import os
import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "tf_outputs.json"

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
aws_credentials = require_output("aws_airflow_admin_credentials")
aws_region = (
    outputs.get("aws_region", {}).get("value")
    or os.getenv("AWS_REGION")
    or os.getenv("AWS_DEFAULT_REGION")
)

endpoint = connection_details["host"]
port = connection_details["port"]
schema = connection_details["database"]

# Connection parameters (prefer Terraform outputs, fall back to env for backwards compatibility)
conn_id = "redshift_serverless"
conn_type = "redshift"
login = connection_details.get("username") or os.getenv("TF_VAR_admin_username")
password = connection_details.get("password") or os.getenv("TF_VAR_admin_password")

aws_access_key_id = aws_credentials.get("access_key_id")
aws_secret_access_key = aws_credentials.get("secret_access_key")

if not all([aws_access_key_id, aws_secret_access_key]):
    raise RuntimeError("Missing AWS IAM credentials in Terraform outputs. Did Terraform finish applying?")

if not all([endpoint, port, schema, login, password]):
    raise RuntimeError("Missing Redshift connection details. Did Terraform finish applying?")

# Build connection URI
conn_uri = f"redshift://{login}:{password}@{endpoint}:{port}/{schema}"

# Name of your Airflow container (check with `docker ps`)
airflow_container = "airflow-docker-airflow-webserver-1"

def add_connection(cmd):
    subprocess.run([
        "docker", "exec", airflow_container,
        "airflow", "connections", "add",
        *cmd,
    ], check=True)

# Create Redshift connection
add_connection([conn_id, "--conn-uri", conn_uri])

# Create AWS credentials connection
aws_conn_id = "awc_credentaials"
aws_cmd = [
    aws_conn_id,
    "--conn-type", "aws",
    "--conn-login", aws_access_key_id,
    "--conn-password", aws_secret_access_key,
]

if aws_region:
    aws_cmd.extend(["--conn-extra", json.dumps({"region_name": aws_region})])

add_connection(aws_cmd)

print(
    f"Airflow connections '{conn_id}' and '{aws_conn_id}' created inside container '{airflow_container}' "
    f"for Redshift workgroup {workgroup} in namespace {namespace}"
)
