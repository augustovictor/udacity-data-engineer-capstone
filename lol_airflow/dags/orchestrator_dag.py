from datetime import datetime
from os import path

from airflow import DAG, LoggingMixin
from airflow.contrib.operators.emr_create_job_flow_operator import \
    EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from helpers import SqlDmls
from operators import DataQualityOperator
from operators import DdlRedshiftOperator
from operators import (
    FetchAndStageItemsExternalData,
    FetchAndStageChampionsExternalData,
    StageToRedshiftOperator, DataQualityValidator)
from operators import LoadDimensionOperator
from operators import LoadFactOperator

# CONFIG
AWS_CREDENTIALS_ID = "aws_credentials"
AWS_CREDENTIALS_EMR_ID = "aws_credentials_emr"
AWS_REDSHIFT_CONN_ID = "redshift"
RIOT_BASE_URL = ""
S3_BUCKET = "udacity-capstone-lol"
S3_RAW_SUMMONER_DATA_KEY = "lol_raw_data/summoner"
S3_RAW_CHAMPION_DATA_KEY = "lol_raw_data/champion"
S3_RAW_ITEM_DATA_KEY = "lol_raw_data/item"
S3_RAW_MATCH_DATA_KEY = "lol_raw_data/match"
S3_TRANSFORMED_RAW_MATCH_DATA_KEY = "lol_transformed_raw_data/match"
dag_id = "lol_etl"
iam_redshift_role="aws_iam_role=arn:aws:iam::782148276433:role/sparkify.dw.role"
table_name_fact_game_match = "fact_game_match"
table_name_staging_game_match = "staging_game_match"
emr_create_cluster_task_id = "Emr_Create_Cluster_And_Execute_Processing_Task"

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
ddl_sql_file_name = "../create_tables.sql"
sql_path = path.join(path.dirname(path.abspath(__file__)), ddl_sql_file_name)
sql_content = None
try:
    with open(sql_path) as reader:
        sql_content = reader.read()

except Exception as err:
    log.error(f"Failure when reading file {sql_path}")

def fetch_riot_items_data():
    api_key = Variable.get("RIOT_API_KEY")

# VALIDATORS
validator_staging_game_match = DataQualityValidator(
    sql_statement=f"SELECT COUNT(*) FROM {table_name_staging_game_match}",
    result_to_assert=0,
    should_assert_for_equality=False,
)

validator_fact_game_match = DataQualityValidator(
    sql_statement=f"SELECT COUNT(*) FROM {table_name_fact_game_match}",
    result_to_assert=0,
    should_assert_for_equality=False,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    data_quality_validations=[
        validator_staging_game_match,
        validator_fact_game_match
    ],
    dag=dag
)

# OPERATORS
start_operator = DummyOperator(
    task_id="Begin_Execution",
    dag=dag,
)
# fetch_external_summoner_data_to_s3_task = DummyOperator(
#     task_id="Fetch_External_Summoner_To_S3_Data_Task",
#     dag=dag,
# )
fetch_external_champion_to_s3_data_task = FetchAndStageChampionsExternalData(
    task_id="Fetch_And_Stage_Champions_External_Data",
    aws_credentials_id=AWS_CREDENTIALS_ID,
    base_url="http://ddragon.leagueoflegends.com/cdn/10.13.1/data/en_US",
    s3_bucket=S3_BUCKET,
    s3_key=S3_RAW_CHAMPION_DATA_KEY,
    dag=dag,
)
fetch_external_item_to_s3_data_task = FetchAndStageItemsExternalData(
    task_id="Fetch_And_Stage_Items_External_Data",
    aws_credentials_id=AWS_CREDENTIALS_ID,
    base_url="http://ddragon.leagueoflegends.com/cdn/10.13.1/data/en_US/item.json",
    s3_bucket=S3_BUCKET,
    s3_key=S3_RAW_ITEM_DATA_KEY,
    dag=dag,
)
# fetch_external_match_to_s3_data_task = FetchAndStageMatchesExternalData(
#     task_id="Fetch_External_Match_To_S3_Data_Task",
#     s3_bucket=S3_BUCKET,
#     s3_key=S3_RAW_MATCH_DATA_KEY,
#     dag=dag,
# )
stage_external_data_to_s3_task = DummyOperator(
    task_id="Stage_External_Data_To_S3_Task",
    # dag=dag,
)
# transform_external_summoner_data_and_stage_task = EmrOperator(
#     task_id="Transform_External_Summoner_Data_And_Stage_Task",
#     dag=dag,
#     cluster_id=cluster_id,
#     cluster_dns=cluster_dns,
# )
# EMR
SPARK_STEPS = [
    {
        'Name': 'lol_transform_raw_data',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--master',
                'yarn',
                's3://udacity-capstone-lol/lol_pyspark/spark_etl.py',
            ],
        }
    }
] # https://stackoverflow.com/questions/53048106/emr-dag-terminates-before-all-steps-are-completed
JOB_FLOW_OVERRIDES = {
    'Name': 'lol_spark',
    'ReleaseLabel': 'emr-5.29.0',
    "LogUri": f"s3://{S3_BUCKET}/emr_logs",
    'Instances': {
        'Ec2KeyName': 'lol-spark',
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm3.xlarge',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'Steps': SPARK_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    "Applications": [
        {"Name": "Spark"}
    ],
    'BootstrapActions': [
        {
            'Name': 'install python dependencies',
            'ScriptBootstrapAction': {
                'Path': 's3://udacity-capstone-lol/lol_pyspark/install_python_modules.sh',
            }
        }
    ],
    # 'Configurations': [
    #     {
    #         'Classification': 'export',
    #         'Properties': {
    #             'PYSPARK_PYTHON': '/usr/bin/python3'
    #         },
    #     },
    # ],
}
run_emr_create_job_flow_task = EmrCreateJobFlowOperator(
    task_id=emr_create_cluster_task_id,
    aws_conn_id=AWS_CREDENTIALS_EMR_ID,
    emr_conn_id="emr_default",
    region_name="us-west-2", # Remove deprecated
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    do_xcom_push=True, # Remove deprecated
    dag=dag,
)
emr_job_sensor = EmrJobFlowSensor(
    task_id='check_job_flow',
    job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{emr_create_cluster_task_id}', key='return_value') }}}}",
    # step_id="{{ task_instance.xcom_pull(task_ids='TASK_TO_WATCH', key='return_value')[0] }}", # Here gos an EmrAddStepsOperator's id
    aws_conn_id=AWS_CREDENTIALS_EMR_ID,
    dag=dag,
)

# SPARK_STEPS = [{
#     'Name': 'test step',
#     'ActionOnFailure': 'CONTINUE',
#     'HadoopJarStep': {
#         'Jar': 'command-runner.jar',
#         'Args': [
#             'spark-submit', '--deploy-mode', 'cluster', '--class', 'com.naturalint.data.spark.api.scala.NiSparkAppMain', 's3://ni-data-infra/jars/feeder-factorization-etl-1.0-SNAPSHOT.jar', '--ni-main-class', 'com.naturalint.data.etl.feeder.FactorizationEtl', '--job-id', '133', '--config-file', 's3://galactic-feeder-staging/factorization_input/133.json', '--raw-conversions-table', 'galactic_feeder_staging.conversions_raw', '--previous-runs-table', 'galactic_feeder_staging.factorization_output_partitions', '--parquet-output-location', 's3://galactic-feeder-staging/factorization_output_partitions', '--csv-output-location', 's3://galactic-feeder-staging/output'
#         ]
#     }
# }]
# add_emr_step_task = EmrAddStepsOperator(
#     aws_conn_id=AWS_CREDENTIALS_ID,
#     job_flow_id="{{ task_instance.xcom_pull('Run_Emr_Create_Job_Flow_Task', key='return_value')[0] }}",
#     # job_flow_name="",
#     steps=SPARK_STEPS,
#     do_xcom_push=True,
#     dag=dag,
# )
# emr_step_sensor_task = EmrStepSensor(
#     task_id="Watch_Previous_Step",
#
#     dag=dag,
# )
# terminate_emr_cluster_task = EmrTerminateJobFlowOperator(
#     task_id="Emr_Terminate_Cluster",
#     aws_conn_id=AWS_CREDENTIALS_EMR_ID,
#     job_flow_id="{{ task_instance.xcom_pull('Run_Emr_Create_Job_Flow_Task', key='return_value') }}",
#     trigger_rule="all_done", # Runs even when the job fails
#     # dag=dag,
# )

# transform_external_item_data_and_stage_task = EmrOperator(
#     task_id="Transform_External_Item_Data_And_Stage_Task",
#     cluster_id=cluster_id,
#     cluster_dns=cluster_dns,
#     # dag=dag,
# )
# Ready on lol_pyspark
# transform_external_match_data_and_stage_task = EmrOperator(
#     task_id="Transform_External_Match_Data_And_Stage_Task",
#     cluster_id=cluster_id,
#     cluster_dns=cluster_dns,
#     # dag=dag,
# )
run_redshift_ddls_task = DdlRedshiftOperator(
    task_id="Run_Redshift_DDLs_Task",
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    ddl_sql=sql_content,
    dag=dag,
)
load_transformed_data_to_redshift_staging_tables_task = StageToRedshiftOperator(
    task_id="Load_Transformed_Data_To_Redshift_Staging_Tables_Task",
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    iam_redshift_role=iam_redshift_role,
    target_table=table_name_staging_game_match,
    s3_bucket=S3_BUCKET,
    s3_key=S3_TRANSFORMED_RAW_MATCH_DATA_KEY,
    dag=dag,
)
# load_summoner_dimension_table_task = LoadDimensionOperator(
#     task_id="Load_Summoner_Dimension_Table_Task",
#     redshift_conn_id=AWS_REDSHIFT_CONN_ID,
#     final_table="",
#     dql_sql=SqlDmls.summoner_table_insert,
#     dag=dag,
# )
load_champion_dimension_table_task = LoadDimensionOperator(
    task_id="Load_Champion_Dimension_Table_Task",
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    final_table="",
    dql_sql=SqlDmls.champion_table_insert,
    # dag=dag,
)
load_item_dimension_table_task = LoadDimensionOperator(
    task_id="Load_Item_Dimension_Table_Task",
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    final_table="",
    dql_sql=SqlDmls.item_table_insert,
    # dag=dag,
)
load_fact_match_table_task = LoadFactOperator(
    task_id="Load_Fact_Tables_Task",
    redshift_conn_id=AWS_REDSHIFT_CONN_ID,
    final_table=table_name_fact_game_match,
    dql_sql=SqlDmls.match_table_insert,
    dag=dag,
)
end_operator = DummyOperator(
    task_id="End_Execution",
    dag=dag,
)

# DAG
start_operator >> [
    fetch_external_champion_to_s3_data_task,
    fetch_external_item_to_s3_data_task,
] >> run_emr_create_job_flow_task

run_emr_create_job_flow_task >> emr_job_sensor

emr_job_sensor >> run_redshift_ddls_task

run_redshift_ddls_task >> load_transformed_data_to_redshift_staging_tables_task

# Uncomment after staging champion and item data
# load_transformed_data_to_redshift_staging_tables_task >> [
#     load_champion_dimension_table_task,
#     load_item_dimension_table_task,
# ] >> load_fact_match_table_task

# Remove after staging champion and item data
load_transformed_data_to_redshift_staging_tables_task >> load_fact_match_table_task

load_fact_match_table_task >> run_quality_checks

run_quality_checks >> end_operator
