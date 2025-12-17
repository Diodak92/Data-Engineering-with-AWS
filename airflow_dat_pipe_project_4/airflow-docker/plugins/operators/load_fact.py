from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


class LoadFactOperator(RedshiftDataOperator):

    ui_color = '#F98866'

    def __init__(
                self,
                workgroup_name: str | None = None,
                cluster_identifier: str | None = None,
                database: str | None = None,
                db_user: str | None = None,
                target_table : str = "",
                sql: str | list[str] = "",
                aws_conn_id: str = "aws_default",
                poll_interval: int = 10,
                wait_for_completion: bool = True,
                **kwargs,
                ):
        
        if workgroup_name and cluster_identifier:
            raise ValueError("Provide only one of workgroup_name or cluster_identifier for LoadFactOperator.")

        # Only pass the configured Redshift target to avoid Data API validation errors.
        redshift_target: dict[str, str] = {}
        if workgroup_name:
            redshift_target["workgroup_name"] = workgroup_name
        if cluster_identifier:
            redshift_target["cluster_identifier"] = cluster_identifier
        if not redshift_target:
            raise ValueError("LoadFactOperator requires either workgroup_name (serverless) or cluster_identifier (provisioned).")
        
        sql_insert = f"INSERT INTO {target_table} {sql}"
        self.target_table = target_table

        super().__init__(
            database=database,
            db_user=db_user,
            sql=sql_insert,
            aws_conn_id=aws_conn_id,
            poll_interval=poll_interval,
            wait_for_completion=wait_for_completion,
            **redshift_target,
            **kwargs,
        )

    def execute(self, context):
        """Log start/end around the Redshift Data API execution."""
        self.log.info("Starting load into %s", self.target_table)
        self.log.info("Executing SQL: %s", self.sql)
        result = super().execute(context)
        self.log.info("Finished load into %s", self.target_table)
        return result
