import argparse
import json
import os
import subprocess
from pathlib import Path

import boto3
import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQL = REPO_ROOT / "create_tables.sql"
INFRA_DIR = REPO_ROOT / "infra"
OUTPUT_CACHE = INFRA_DIR / "tf_outputs.json"


def load_terraform_outputs() -> dict:
    """Read Terraform outputs (cached file or terraform CLI)."""
    if OUTPUT_CACHE.exists():
        data = json.loads(OUTPUT_CACHE.read_text(encoding="utf-8"))
    else:
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd=INFRA_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
        data = json.loads(result.stdout)
    return data


def load_connection_details(outputs: dict | None = None) -> dict:
    """Return connection details from Terraform outputs."""
    outputs = outputs or load_terraform_outputs()
    try:
        return outputs["connection_details"]["value"]
    except KeyError as exc:
        raise RuntimeError(
            "Terraform output 'connection_details' not found. Run `terraform apply` in infra/."
        ) from exc


def get_iam_db_credentials(
    workgroup_name: str,
    database: str,
    region: str,
    aws_profile: str | None = None,
) -> tuple[str, str]:
    """
    Request temporary Redshift Serverless credentials for an IAM DB user.
    Falls back to the default AWS credential chain if a profile is not supplied.
    """
    session_kwargs = {"profile_name": aws_profile} if aws_profile else {}
    session = boto3.Session(**session_kwargs)
    client = session.client("redshift-serverless", region_name=region)
    resp = client.get_credentials(
        workgroupName=workgroup_name,
        dbName=database,
        durationSeconds=3600,
    )
    return resp["dbUser"], resp["dbPassword"]


def connect_to_redshift(
    autocommit: bool,
    user: str,
    password: str,
    connection_details: dict,
) -> psycopg2.extensions.connection:
    """Open a psycopg2 connection using the provided credentials."""
    conn = psycopg2.connect(
        host=connection_details["host"],
        port=int(connection_details["port"]),
        dbname=connection_details["database"],
        user=user,
        password=password,
    )
    conn.set_session(autocommit=autocommit)
    return conn


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute a SQL file on Redshift Serverless.")
    parser.add_argument("--sql-file", type=Path, default=DEFAULT_SQL, help="Path to SQL file.")
    parser.add_argument(
        "--autocommit",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable autocommit on the Redshift connection (default: on).",
    )
    parser.add_argument(
        "--use-iam",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Request temporary credentials for an IAM DB user via Redshift Serverless.",
    )
    parser.add_argument(
        "--db-user",
        help="DB user to run the statements as (ignored when --use-iam is set).",
    )
    parser.add_argument(
        "--workgroup-name",
        help="Redshift Serverless workgroup name (defaults to Terraform output).",
    )
    parser.add_argument(
        "--region",
        help="AWS region for IAM auth (defaults to Terraform output or AWS_REGION/AWS_DEFAULT_REGION).",
    )
    parser.add_argument(
        "--aws-profile",
        help="AWS named profile to use when requesting IAM credentials.",
    )
    args = parser.parse_args()

    outputs = load_terraform_outputs()
    connection_details = load_connection_details(outputs)
    default_region = (
        outputs.get("aws_region", {}).get("value")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
    )
    workgroup_name = args.workgroup_name or outputs.get("workgroup_name", {}).get("value")

    if args.use_iam and not workgroup_name:
        raise RuntimeError("Workgroup name is required for IAM auth. Provide --workgroup-name or run Terraform.")

    db_user = args.db_user or connection_details.get("username")
    db_password = connection_details.get("password")
    region = args.region or default_region

    if args.use_iam:
        if not region:
            raise RuntimeError("AWS region is required for IAM auth. Provide --region or set AWS_REGION.")
        db_user, db_password = get_iam_db_credentials(
            workgroup_name=workgroup_name,
            database=connection_details["database"],
            region=region,
            aws_profile=args.aws_profile,
        )

    sql_text = args.sql_file.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    if not statements:
        raise RuntimeError(f"No SQL statements found in {args.sql_file}")

    conn = connect_to_redshift(
        autocommit=args.autocommit,
        user=db_user,
        password=db_password,
        connection_details=connection_details,
    )
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
        if not args.autocommit:
            conn.commit()
    finally:
        conn.close()

    print(f"Executed {len(statements)} statements from {args.sql_file}")


if __name__ == "__main__":
    main()
