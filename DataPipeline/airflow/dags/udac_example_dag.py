from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,PostgresOperator)
from helpers import SqlQueries
import create_tables

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    #'start_date': datetime(2019, 1, 12),
    'start_date' : datetime.now(),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          template_searchpath=['/home/workspace/airflow'] 
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_events_table = PostgresOperator(
    task_id="CREATE_TABLE_PUBLIC_STAGING_EVENTS_TABLE",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_TABLE_PUBLIC_STAGING_EVENTS_SQL
)

create_staging_songs_table = PostgresOperator(
    task_id="CREATE_TABLE_PUBLIC_STAGING_SONGS_TABLE",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_TABLE_PUBLIC_STAGING_SONGS_SQL
)

create_artists_table = PostgresOperator(
    task_id="CREATE_TABLE_PUBLIC_ARTISTS_TABLE",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_TABLE_PUBLIC_ARTISTS_SQL
)


create_songs_events_table = PostgresOperator(
    task_id="CREATE_TABLE_PUBLIC_SONGS_TABLE",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_TABLE_PUBLIC_SONGS_SQL
)

create_users_table = PostgresOperator(
    task_id="CREATE_TABLE_PUBLIC_USERS_TABLE",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_TABLE_PUBLIC_USERS_SQL
)

create_songplays_table = PostgresOperator(
    task_id="CREATE_TABLE_PUBLIC_SONGPLAYS_TABLE",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_TABLE_PUBLIC_SONGPLAYS_SQL
)

create_time_table = PostgresOperator(
    task_id="CREATE_TABLE_PUBLIC_TIME_TABLE",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_TABLE_PUBLIC_TIME_SQL
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id = 'redshift',
    table = "staging_events",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/log_data",
    format_option="s3://udacity-dend/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id = "redshift",
    table = "staging_songs",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/song_data",
    format_option="auto",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = "redshift",
    insert_stmt=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = "redshift",
    insert_stmt=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = "redshift",
    insert_stmt=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = "redshift",
    insert_stmt=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = "redshift",
    insert_stmt=SqlQueries.time_table_insert,
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

create_staging_events_table >> stage_events_to_redshift
create_staging_songs_table >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator


