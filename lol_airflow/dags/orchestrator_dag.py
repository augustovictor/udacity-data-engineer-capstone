from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag_id="lol_etl"

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
)

start_operator = DummyOperator(task_id="Begin_Execution", dag=dag)

fetch_external_data_task = DummyOperator(task_id="fetch_external_data_task", dag=dag)
stage_external_data_to_s3_task = DummyOperator(task_id="stage_external_data_to_s3_task", dag=dag)
transform_external_data_task = DummyOperator(task_id="transform_external_data_task", dag=dag)
run_redshift_ddls_task = DummyOperator(task_id="run_redshift_ddls_task", dag=dag)
load_transformed_data_to_redshift_staging_tables_task = DummyOperator(task_id="load_transformed_data_to_redshift_staging_tables_task", dag=dag)
populate_dimension_tables_task = DummyOperator(task_id="populate_dimension_tables_task", dag=dag)
populate_fact_tables_task = DummyOperator(task_id="populate_fact_tables_task", dag=dag)
data_quality_check_task = DummyOperator(task_id="data_quality_check_task", dag=dag)

end_operator = DummyOperator(task_id="End_Execution", dag=dag)

start_operator >> fetch_external_data_task

fetch_external_data_task >> stage_external_data_to_s3_task

stage_external_data_to_s3_task >> transform_external_data_task

transform_external_data_task >> run_redshift_ddls_task

run_redshift_ddls_task >> load_transformed_data_to_redshift_staging_tables_task

load_transformed_data_to_redshift_staging_tables_task >> populate_dimension_tables_task

populate_dimension_tables_task >> populate_fact_tables_task

populate_fact_tables_task >> data_quality_check_task

data_quality_check_task >> end_operator
