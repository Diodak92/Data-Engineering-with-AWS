from typing import List, Sequence
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


class StageToRedshiftOperator(S3ToRedshiftOperator):

    ui_color = '#358140'

    def __init__(
        self,
        table: str,
        s3_bucket: str,
        s3_key: str,
        schema: str,
        redshift_conn_id: str,
        aws_conn_id: str,
        json_path: str | None = None,
        copy_options: Sequence[str] | None = None,
        method: str = "APPEND",
        **kwargs,
    ):
        self.log.info(
            f"Staging from s3://{s3_bucket}/{s3_key} into {schema}.{table} (method={method})"
            )

        options: List[str] = list(copy_options or [])
        json_option_present = any("json" in option.lower() for option in options)
        if not json_option_present:
            default_json_option = (
                f"FORMAT AS JSON '{json_path}'" if json_path else "FORMAT AS JSON 'auto'"
            )
            options.insert(0, default_json_option)
            self.log.info(f"Added default JSON option to COPY command: {default_json_option}")

        self.log.debug(f"Final COPY options: {options}")

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

        self.log.info(f"Data inserted successfully into {schema}.{table}")
