import argparse
import subprocess
from typing import Sequence

UDACITY_LOG_JSON = "s3://udacity-dend/log_json_path.json"
UDACITY_LOG_DATA = "s3://udacity-dend/log-data/"
UDACITY_SONG_DATA = "s3://udacity-dend/song-data/"


def run_aws_cp(source: str, destination: str, *, recursive: bool = True, extra_args: Sequence[str] | None = None) -> None:
    """Run an AWS CLI s3 cp command, optionally recursive."""
    cmd = ["aws", "s3", "cp", source, destination]
    if recursive:
        cmd.append("--recursive")
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
    log_dst = f"s3://{target}/log-data/"
    song_dst = f"s3://{target}/song-data/"

    print(f"Copying log json file to {log_json_path}")
    run_aws_cp(UDACITY_LOG_JSON, log_json_path, recursive=False)

    print(f"Copying log data to {log_dst}")
    run_aws_cp(UDACITY_LOG_DATA, log_dst)

    print(f"Copying song data to {song_dst}")
    run_aws_cp(UDACITY_SONG_DATA, song_dst)

    print("S3 data copy completed.")


if __name__ == "__main__":
    main()
