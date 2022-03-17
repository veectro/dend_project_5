from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    def __init__(self,
                 table_name: str,
                 redshift_conn_id: str,
                 query: str,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        self.log.info(f'Loading dimension table into {self.table_name}')

        # Connecting to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Executing the query
        table_insert_sql = f"""
            INSERT INTO {self.table_name}
            {self.query}
        """
        redshift_hook.run(table_insert_sql)
        self.log.info('Successfully loading dimension table into Redshift')
