import argparse
import json
import subprocess
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQL = REPO_ROOT / "create_tables.sql"
INFRA_DIR = REPO_ROOT / "infra"
OUTPUT_CACHE = INFRA_DIR / "tf_outputs.json"


def load_connection_details() -> dict:
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

    try:
        return data["connection_details"]["value"]
    except KeyError as exc:
        raise RuntimeError(
            "Terraform output 'connection_details' not found. Run `terraform apply` in infra/."
        ) from exc


def connect_to_redshift(autocommit: bool) -> psycopg2.extensions.connection:
    """Open a psycopg2 connection using Terraform-provided credentials."""
    details = load_connection_details()
    conn = psycopg2.connect(
        host=details["host"],
        port=int(details["port"]),
        dbname=details["database"],
        user=details["username"],
        password=details["password"],
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
    args = parser.parse_args()

    sql_text = args.sql_file.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    if not statements:
        raise RuntimeError(f"No SQL statements found in {args.sql_file}")

    conn = connect_to_redshift(autocommit=args.autocommit)
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
