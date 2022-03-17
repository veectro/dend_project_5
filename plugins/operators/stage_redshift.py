from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    """
    Loads data from S3 to Redshift staging tables.

    :param redshift_conn_id: Redshift connection ID
    :param table: Table name
    :param s3_bucket: Source S3 bucket name
    :param s3_key: Source S3 key name
    :param json_path: JSON path location that defining the data structure
    """
    ui_color = '#358140'

    def __init__(self,
                 redshift_conn_id: str,
                 s3_bucket: str,
                 s3_key: str,
                 target_table: str,
                 redshift_arn: str,
                 region: str = 'us-west-2',
                 json_format='auto',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table
        self.redshift_arn = redshift_arn
        self.region = region
        self.json_format = json_format

    def execute(self, context):
        self.log.info('Beginning to stage data to redshift')

        # Connecting to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

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
        self.log.info('Successfully stage data to redshift')
