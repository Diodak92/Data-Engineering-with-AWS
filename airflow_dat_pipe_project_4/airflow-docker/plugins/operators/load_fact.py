from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

class LoadFactOperator(RedshiftDataOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 cluster_identifier : str,
                 database : str,
                 db_user : str,
                 sql : str, 
                 aws_conn_id : str,
                 poll_interval : str = 10,
                 wait_for_completion=True,
                 **kwargs,
        ):
        
        super().__init__(
            cluster_identifier=cluster_identifier,
            database=database,
            db_user=db_user,
            sql=sql,
            aws_conn_id=aws_conn_id,
            poll_interval=poll_interval,
            wait_for_completion=wait_for_completion,
            **kwargs,
        )

