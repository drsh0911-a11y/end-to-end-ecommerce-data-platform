from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_medallion_pipeline',
    default_args=default_args,
    description='Medallion architecture pipeline: Bronze -> Silver -> Gold',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_bronze = BashOperator(
        task_id='ingest_to_bronze',
        bash_command='python /opt/airflow/scripts/bronze_ingestion.py',
    )

    run_silver = BashOperator(
        task_id='transform_to_silver',
        bash_command='python /opt/airflow/scripts/silver_transformation.py',
    )

    run_gold = BashOperator(
        task_id='transform_to_gold',
        bash_command='python /opt/airflow/scripts/gold_transformation.py',
    )

    run_bronze >> run_silver >> run_gold