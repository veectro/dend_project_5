from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

DOCUMENTATION_MD = """
## udac_example_dag

This DAG load the data from the S3 bucket and then load the data into the Redshift cluster in staging tables.
From the staging tables, the data is loaded into the fact and dimension tables.

### Input
S3 Buckets :   
  
- `s3://udacity-dend/log_data`
- `s3://udacity-dend/song_data`

### Output
Redshift Tables :   
  
- `staging_events`
- `staging_songs`
- `songplays`
- `artists`
- `songs`
- `users`
- `time`
"""

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False,
          doc_md=DOCUMENTATION_MD
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Using PostgresOperator to create staging tables from sql file in dags/sql
# Based on :
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/operators/postgres_operator_howto_guide.html
create_tables_in_redshift = PostgresOperator(
    task_id='Create_tables_in_postgres',
    dag=dag,
    postgres_conn_id='redshift_conn_id',
    sql='sql/create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift_conn_id',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    target_table='staging_events',
    redshift_arn=Variable.get('redshift_iam_arn'),
    json_format='s3://udacity-dend/log_json_path.json',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift_conn_id',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    target_table='staging_songs',
    redshift_arn=Variable.get('redshift_iam_arn'),
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table_name='songplays',
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table_name='users',
    query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table_name='songs',
    query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table_name='artists',
    query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    table_name='time',
    query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    test_queries=[],
    expected_results=[]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator \
>> create_tables_in_redshift \
>> [stage_events_to_redshift, stage_songs_to_redshift] \
>> load_songplays_table \
>> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
>> run_quality_checks \
>> end_operator
