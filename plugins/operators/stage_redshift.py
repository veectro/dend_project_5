import os
from pprint import pprint

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.baseoperator import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    def __init__(self,
                 aws_credentials: str,
                 redshift_conn: str,
                 s3_bucket: str,
                 s3_key: str,
                 target_table: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn = redshift_conn
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')

        # Getting credentials from AWS Connection
        s3_hook = S3Hook(self.aws_credentials)
        pprint(s3_hook.get_credentials())
        s3_keys = s3_hook.list_keys(self.s3_bucket, self.s3_key)
        pprint(s3_keys)
        redshift = PostgresHook(connection=self.redshift_conn)
        pprint(self.redshift_conn)
        pprint(os.environ)

