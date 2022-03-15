from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    def __init__(self,
                 aws_credentials: str,
                 redshift_conn: str,
                 s3_bucket: str,
                 s3_key: str,
                 target_table: str,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn = redshift_conn
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')




