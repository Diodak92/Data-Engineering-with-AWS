from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table : str,
                 s3_bucket : str,
                 s3_key : str,
                 schema : str,
                 redshift_conn_id : str,
                 aws_conn_id : str,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        
        # Docs
        #https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/transfers/s3_to_redshift/index.html
        self.transfer_s3_to_redshift = S3ToRedshiftOperator(
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            schema=schema,
            redshift_conn_id=redshift_conn_id,
            aws_conn_id=aws_conn_id,
            copy_options=["json"],
            method='APPEND',
        )

    def execute(self, context):
        self.log.info(
            "Loading S3://%s/%s into %s.%s",
            self.transfer_s3_to_redshift.s3_bucket,
            self.transfer_s3_to_redshift.s3_key,
            self.transfer_s3_to_redshift.schema,
            self.transfer_s3_to_redshift.table,
        )
        self.transfer_s3_to_redshift.execute(context)




