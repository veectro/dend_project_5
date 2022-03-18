from typing import List, Dict, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    """
    Checks the data quality of the tables in the database.

    :param redshift_conn_id: Redshift connection ID
    :param queries_tuples: list of tuples containing the query and the expected result
    :param expected_results: list of expected results for each query
    """
    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 queries_tuples: List[Tuple[str, int]],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries_tuples = queries_tuples

    def execute(self, context):
        self.log.info('DataQualityOperator started')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        errs = []
        for query, expected_result in self.queries_tuples:
            self.log.info(f'Running query: {query}')
            records = redshift_hook.get_records(query)
            if records[0][0] != expected_result:
                errs.append(f'{query} returned {records[0][0]} instead of {expected_result}')

        if len(errs) > 0:
            raise ValueError(f'Data quality check failed: {errs}')
