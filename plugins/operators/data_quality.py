from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    """
    Checks the data quality of the tables in the database.

    :param redshift_conn_id: Redshift connection ID
    :param test_queries: list of queries to be tested
    :param expected_results: list of expected results for each query
    """
    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 test_queries: List[str],
                 expected_results: List[int],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_queries = test_queries
        self.expected_results = expected_results

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
