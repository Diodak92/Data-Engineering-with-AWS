from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


class LoadFactOperator(RedshiftDataOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 workgroup_name: str | None = None,
                 cluster_identifier: str | None = None,
                 database: str | None = None,
                 db_user: str | None = None,
                 sql: str | list[str] = "",
                 aws_conn_id: str = "aws_default",
                 poll_interval: int = 10,
                 wait_for_completion: bool = True,
                 **kwargs,
        ):
        if workgroup_name and cluster_identifier:
            raise ValueError("Provide only one of workgroup_name or cluster_identifier for LoadFactOperator.")

        # Only pass through the Redshift target that is actually configured to avoid Data API validation errors.
        redshift_target: dict[str, str] = {}
        if workgroup_name:
            redshift_target["workgroup_name"] = workgroup_name
        if cluster_identifier:
            redshift_target["cluster_identifier"] = cluster_identifier
        if not redshift_target:
            raise ValueError("LoadFactOperator requires either workgroup_name (serverless) or cluster_identifier (provisioned).")

        super().__init__(
            database=database,
            db_user=db_user,
            sql=sql,
            aws_conn_id=aws_conn_id,
            poll_interval=poll_interval,
            wait_for_completion=wait_for_completion,
            **redshift_target,
            **kwargs,
        )
