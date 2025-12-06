from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


s3_bucket_name = Variable.get("S3_BUCKET")
redshift_conn = BaseHook.get_connection("redshift_serverless")
# CLUSTER_ID = redshift_conn.extra_dejson.get("cluster_identifier")
WORKGROUP_NAME = Variable.get("REDSHIFT_WORKGROUP", default_var=None) or redshift_conn.extra_dejson.get("workgroup_name")
DATABASE = redshift_conn.schema
# For provisioned clusters we pass the admin DB user, serverless requests must skip db_user.
# DB_USER = redshift_conn.login if WORKGROUP_NAME is None else None


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'max_active_runs': 1
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
)

def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_bucket=s3_bucket_name,
        s3_key='log-data',
        schema='public',
        redshift_conn_id='redshift_serverless',
        aws_conn_id='aws_credentials',
        method='REPLACE',
        json_path = f's3://{s3_bucket_name}/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_bucket=s3_bucket_name,
        s3_key='song-data',
        schema='public',
        redshift_conn_id='redshift_serverless',
        aws_conn_id='aws_credentials',
        method='REPLACE',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        # Serverless requires workgroup_name, provisioned clusters use cluster_identifier
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE,
        target_table = "public.songplays",
        sql=SqlQueries.songplay_table_insert,
        aws_conn_id='aws_credentials',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE,
        target_table = "public.users",
        sql=SqlQueries.user_table_insert,
        truncate = True,
        aws_conn_id='aws_credentials',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE,
        target_table = "public.songs",
        sql=SqlQueries.song_table_insert,
        truncate = True,
        aws_conn_id='aws_credentials',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE,
        target_table="public.artists",
        sql=SqlQueries.artist_table_insert,
        truncate = True,
        aws_conn_id='aws_credentials',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE,
        target_table = "public.time",
        sql=SqlQueries.time_table_insert,
        truncate = True,
        aws_conn_id='aws_credentials',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE,
        #sql_check = """SELECT * FROM public.staging_songs LIMIT 1;""",
        sql_check= """
                    WITH artists_stg AS (
                    SELECT COUNT(DISTINCT(artist_id)) AS stg_artist_cnt
                    FROM public.staging_songs
                    ),
                    artists AS (
                    SELECT COUNT(DISTINCT(artistid)) AS artist_cnt
                    FROM public.songs
                    )
                    SELECT artists_stg.stg_artist_cnt = artists.artist_cnt AS cnt_test
                    FROM artists_stg, artists;
                   """,
        logic_test = "== True",
        aws_conn_id='aws_credentials',
    )

    run_song_quality_check = DataQualityOperator(
        task_id='Run_song_null_check',
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE,
        #sql_check = """SELECT * FROM public.staging_songs LIMIT 1;""",
        sql_check= """
                    SELECT COUNT(title) AS null_cnt
                    FROM public.songs
                    WHERE (title IS NULL);
                   """,
        logic_test = "== 0",
        aws_conn_id='aws_credentials',
    )

    quality_start = DummyOperator(task_id='Start_quality_checks')
    quality_end = DummyOperator(task_id='End_quality_checks')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
                             load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, 
     load_artist_dimension_table, load_time_dimension_table] >> quality_start
    quality_start >> [run_quality_checks, run_song_quality_check] >> quality_end


final_project_dag = final_project()
