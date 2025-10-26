#!/usr/bin/env python3
import json
import os
import subprocess

# Load Terraform outputs
with open("tf_outputs.json", encoding="utf-8") as f:
    outputs = json.load(f)


def require_output(key: str):
    try:
        return outputs[key]["value"]
    except KeyError as exc:  # provide actionable guidance
        raise RuntimeError(
            f"Terraform output '{key}' not found. "
            "Run 'terraform output -json > tf_outputs.json' after a successful apply."
        ) from exc


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
