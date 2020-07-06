from datetime import datetime
from os import path

from airflow import DAG, LoggingMixin
from airflow.operators.dummy_operator import DummyOperator
from operators.emr_operator import EmrOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.ddl_redshift_operator import DdlRedshiftOperator
from operators.data_quality import DataQualityOperator

# CONFIG
AWS_CREDENTIALS_ID = "aws_credentials"
AWS_REDSHIFT_CONN_ID = "redshift"
RIOT_BASE_URL = ""
S3_BUCKET = "udacity-capstone-lol"
S3_RAW_SUMMONER_DATA_KEY = "raw_data/summoner"
S3_RAW_CHAMPION_DATA_KEY = "raw_data/champion"
S3_RAW_ITEM_DATA_KEY = "raw_data/item"
S3_RAW_MATCH_DATA_KEY = "raw_data/match"
S3_TRANSFORMED_RAW_DATA_KEY = ""
dag_id = "lol_etl"
cluster_id = "change"
cluster_dns = "change"

default_args = {
    "owner": "Victor Costa",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1)
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="",
    catchup=False,
    max_active_runs=1,
    schedule_interval="@daily",
    concurrency=4,
)

log = LoggingMixin().log
ddl_sql_file_name = '../create_tables.sql'
sql_path = path.join(path.dirname(path.abspath(__file__)), ddl_sql_file_name)
sql_content = None
try:
    with open(sql_path) as reader:
        sql_content = reader.read()

except Exception as err:
    log.error(f"Failure when reading file {sql_path}")

# OPERATORS
start_operator = DummyOperator(
    task_id="Begin_Execution",
    dag=dag
)
fetch_external_summoner_data_to_s3_task = DummyOperator(
    task_id="Fetch_External_Summoner_To_S3_Data_Task",
    dag=dag
)
fetch_external_champion_to_s3_data_task = DummyOperator(
    task_id="Fetch_External_Champion_To_S3_Data_Task",
    dag=dag
)
fetch_external_item_to_s3_data_task = DummyOperator(
    task_id="Fetch_External_Item_To_S3_Data_Task",
    dag=dag
)
fetch_external_match_to_s3_data_task = DummyOperator(
    task_id="Fetch_External_Match_To_S3_Data_Task",
    dag=dag
)
stage_external_data_to_s3_task = DummyOperator(
    task_id="Stage_External_Data_To_S3_Task",
    dag=dag
)
transform_external_summoner_data_task = EmrOperator(
    task_id="Transform_External_Summoner_Data_Task",
    dag=dag,
    params={
        "cluster_id": cluster_id,
        "cluster_dns": cluster_dns,
    }
)
transform_external_champion_data_task = EmrOperator(
    task_id="Transform_External_Champion_Data_Task",
    dag=dag
)
transform_external_item_data_task = EmrOperator(
    task_id="Transform_External_Item_Data_Task",
    dag=dag
)
transform_external_match_data_task = EmrOperator(
    task_id="Transform_External_Match_Data_Task",
    dag=dag
)
run_redshift_ddls_task = DdlRedshiftOperator(
    task_id="Run_Redshift_DDLs_Task",
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    ddl_sql=sql_content,
    dag=dag,
)
load_transformed_data_to_redshift_staging_tables_task = LoadDimensionOperator(
    task_id="Load_Transformed_Data_To_Redshift_Staging_Tables_Task",
    dag=dag
)
load_summoner_dimension_table_task = LoadDimensionOperator(
    task_id="Load_Summoner_Dimension_Table_Task",
    dag=dag
)
load_champion_dimension_table_task = LoadDimensionOperator(
    task_id="Load_Champion_Dimension_Table_Task",
    dag=dag
)
load_item_dimension_table_task = LoadDimensionOperator(
    task_id="Load_Item_Dimension_Table_Task",
    dag=dag
)
load_fact_tables_task = LoadFactOperator(
    task_id="Load_Fact_Tables_Task",
    dag=dag
)
data_quality_check_task = DataQualityOperator(
    task_id="Data_Quality_Check_Task",
    dag=dag
)
end_operator = DummyOperator(
    task_id="End_Execution",
    dag=dag
)

# DAG
start_operator >> [
    fetch_external_summoner_data_to_s3_task,
    fetch_external_champion_to_s3_data_task,
    fetch_external_item_to_s3_data_task,
    fetch_external_match_to_s3_data_task,
] >> stage_external_data_to_s3_task

stage_external_data_to_s3_task >> [
    transform_external_summoner_data_task,
    transform_external_champion_data_task,
    transform_external_item_data_task,
    transform_external_match_data_task,
] >> run_redshift_ddls_task

run_redshift_ddls_task >> load_transformed_data_to_redshift_staging_tables_task

load_transformed_data_to_redshift_staging_tables_task >> [
    load_summoner_dimension_table_task,
    load_champion_dimension_table_task,
    load_item_dimension_table_task,
] >> load_fact_tables_task

load_fact_tables_task >> data_quality_check_task

data_quality_check_task >> end_operator
