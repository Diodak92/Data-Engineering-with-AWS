from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


class LoadDimensionOperator(RedshiftDataOperator):
    
    ui_color = '#80BD9E'

    def __init__(
                self,
                workgroup_name: str | None = None,
                cluster_identifier: str | None = None,
                database: str | None = None,
                db_user: str | None = None,
                target_table : str = "",
                sql: str | list[str] = "",
                truncate : bool = False,
                aws_conn_id: str = "aws_default",
                poll_interval: int = 10,
                wait_for_completion: bool = True,
                **kwargs,
                ):
        
        if workgroup_name and cluster_identifier:
            raise ValueError("Provide only one of workgroup_name or cluster_identifier for LoadDimensionOperator.")

        # Only pass the configured Redshift target to avoid Data API validation errors.
        redshift_target: dict[str, str] = {}
        if workgroup_name:
            redshift_target["workgroup_name"] = workgroup_name
        if cluster_identifier:
            redshift_target["cluster_identifier"] = cluster_identifier
        if not redshift_target:
            raise ValueError("LoadDimensionOperator requires either workgroup_name (serverless) or cluster_identifier (provisioned).")
        
        sql_insert = f"INSERT INTO {target_table} {sql}"
        sql_queries = [sql_insert]

        if truncate:
            self.log.info(f"Truncating table {target_table} before insert")
            sql_queries.insert(0, f"TRUNCATE {target_table}")

        self.log.info(f"Inserting data into {target_table}")

        super().__init__(
            database=database,
            db_user=db_user,
            sql=sql_queries,
            aws_conn_id=aws_conn_id,
            poll_interval=poll_interval,
            wait_for_completion=wait_for_completion,
            **redshift_target,
            **kwargs,
        )
        self.log.info(f"Data inserted successfully into {target_table}")
