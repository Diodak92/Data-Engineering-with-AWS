from typing import List, Sequence

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


class StageToRedshiftOperator(S3ToRedshiftOperator):
    """Custom wrapper around S3ToRedshiftOperator with sane COPY defaults."""

    ui_color = '#358140'
    template_fields: Sequence[str] = (
        "s3_bucket",
        "s3_key",
        "schema",
        "table",
        "json_path",
    )

    def __init__(
        self,
        table: str,
        s3_bucket: str,
        s3_key: str,
        schema: str,
        redshift_conn_id: str,
        aws_conn_id: str,
        encoding: str = "UTF8",
        json_path: str | None = None,
        copy_options: Sequence[str] | None = None,
        method: str = "APPEND",
        **kwargs,
    ):
        self.json_path = json_path

        options: List[str] = list(copy_options or [])
        encoding_option_present = any("encoding" in option.lower() for option in options)
        if not encoding_option_present:
            options.insert(0, f"ENCODING AS {encoding}")

        json_option_present = any("json" in option.lower() for option in options)
        if not json_option_present:
            options.insert(0, f"FORMAT AS JSON '{json_path}'" if json_path else "FORMAT AS JSON 'auto'")

        super().__init__(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            redshift_conn_id=redshift_conn_id,
            aws_conn_id=aws_conn_id,
            copy_options=options,
            method=method,
            **kwargs,
        )
