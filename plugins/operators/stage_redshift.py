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
                 redshift_arn:str,
                 region: str = 'us-west-2',
                 json_format = 'auto',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn = redshift_conn
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table
        self.redshift_arn = redshift_arn
        self.region = region
        self.json_format = json_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')

        # Connecting to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn)
        pprint(self.redshift_conn)
        pprint(os.environ)

        # Creating staging table
        # based on https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html
        # for authentication : https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html
        copy_query = f"""
            COPY {self.target_table}
            FROM 's3://{self.s3_bucket}/{self.s3_key}'
            IAM_ROLE '{self.redshift_arn}'
            REGION AS '{self.region}'
            FORMAT as json '{self.json_format}'
        """
        redshift_hook.run(copy_query)

