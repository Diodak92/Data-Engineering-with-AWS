import argparse
import subprocess
from typing import Sequence

UDACITY_BUCKET = "s3://udacity-dend/"


def run_aws_sync(source: str, destination: str, *, extra_args: Sequence[str] | None = None) -> None:
    """Run an AWS CLI s3 sync command."""
    cmd = ["aws", "s3", "sync", source, destination]
    if extra_args:
        cmd.extend(extra_args)
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy Udacity DEND datasets into a personal S3 bucket.")
    parser.add_argument(
        "--target-bucket",
        required=True,
        help="Name of your destination bucket (without s3:// prefix).",
    )
    args = parser.parse_args()
    target = args.target_bucket.strip().rstrip("/")

    log_json_path = f"s3://{target}/"

    print(f"Syncing files from {UDACITY_BUCKET}")
    run_aws_sync(UDACITY_BUCKET, log_json_path)
    print("S3 data copy completed.")


if __name__ == "__main__":
    main()
