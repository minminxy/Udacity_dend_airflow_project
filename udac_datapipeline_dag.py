from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Default args state 1. The dag does not have dependencies on past runs, 
# 2. the task are retried 3 times on failure, 
# 3. retries happen every 5 minutes, 
# 4. catchup is turned off,
# 5. do not email on retry
def start():
    logging.info("Start of DAG")
    
def end():
    logging.info("End of DAG")

def lit_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    
    keys = hook.list_keys(bucket)
    for key in keys:
        logging.info(f"- Listing Keys from  s3://{key}")
        
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = PythonOperator(
    task_id='Begin_execution',  
    python_callable = start,
    dag=dag
)

creat_table = PostgresOperator(task_id="Create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag, 
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    file_type="json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json_path="auto",
    file_type="json"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['songplays', 'users', 'songs', 'artists', 'time'],
    redshift_conn_id = "redshift",
)

end_operator = PythonOperator(
    task_id='Stop_execution', 
    python_callable= end,
    dag=dag
)
#Set up dependencies
Begin_execution >> Create_table
Create_table >> Stage_events
Create_table >> Stage_songs
Stage_events >> Load_songplays_fact_table
Stage_songs >> Load_songplays_fact_table
Load_songplays_fact_table >> Load_user_dim_table >> Run_data_quality_checks
Load_songplays_fact_table >> Load_song_dim_table >> Run_data_quality_checks
Load_songplays_fact_table >> Load_artist_dim_table >> Run_data_quality_checks
Load_songplays_fact_table >> Load_time_dim_table >> Run_data_quality_checks
Run_data_quality_checks >> Stop_execution