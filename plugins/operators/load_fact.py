from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    def __init__(self,
                 table_name: str,
                 redshift_conn_id: str,
                 query: str,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        self.log.info(f'Loading fact table into {self.table_name}')

        # Connecting to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Executing the query
        table_insert_sql = f"""
            INSERT INTO {self.table_name}
            {self.query}
        """
        redshift_hook.run(table_insert_sql)
        self.log.info('Successfully loading fact table into Redshift')
