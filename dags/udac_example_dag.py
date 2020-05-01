from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email_on_failure': False,
    'depends_on_past': False,
    'catchup': False

}
# sql_path contains the absolute path for create_tables.sql file
sql_path = Variable.get('sql_path')
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False,
          template_searchpath=[sql_path],
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id='redshift'

)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    table='events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path='s3://udacity-dend/log_json_path.json',
    region='us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    table='songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_path='auto',
    region='us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    append_on_insert=False,
    sql_stmnt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    append_on_insert=False,
    sql_stmnt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    append_on_insert=False,
    sql_stmnt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    append_on_insert=False,
    sql_stmnt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    append_on_insert=False,
    sql_stmnt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_table_check_dct={
        'staging_events': ("SELECT COUNT(*) FROM staging_events", 8056),
        'staging_songs': ("SELECT COUNT(*) FROM staging_songs", 14896),
        'songplays': ("SELECT COUNT(*) FROM songplays", 6820),
        'users': ("SELECT COUNT(*) FROM users", 104),
        'artists': ("SELECT COUNT(*) FROM artists", 10025),
        'songs': ("SELECT COUNT(*) FROM songs", 14896),
        'time': ("SELECT COUNT(*) FROM time", 6820)
    }
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)
