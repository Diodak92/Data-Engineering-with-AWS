#!/usr/bin/env python3
import json
import os
import subprocess

# Load Terraform outputs
with open("tf_outputs.json") as f:
    outputs = json.load(f)

endpoint = outputs['workgroup_endpoint']['value'][0]['address']
workgroup = outputs["workgroup_name"]["value"]
namespace = outputs["namespace_name"]["value"]

# Connection parameters
conn_id = "redshift_serverless"
conn_type = "redshift"
login = os.getenv("TF_VAR_admin_username")
password = os.getenv("TF_VAR_admin_password")
schema = "dev"               # default schema
port = outputs['workgroup_endpoint']['value'][0]['port']

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
