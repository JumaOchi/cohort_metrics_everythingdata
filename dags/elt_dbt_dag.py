# dags/etl_dbt_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os
from dotenv import load_dotenv

# load .env into Python environment
load_dotenv("/opt/airflow/.env")
env_vars = os.environ.copy()

default_args = {
    "owner": "juma",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="elt_dbt_pipeline",
    default_args=default_args,
    description="Load Excel -> Supabase, then run dbt (run + test)",
    schedule_interval="@daily",   # change to "*/2 * * * *" for 2-min demo runs
    start_date=datetime(2025, 9, 11),
    catchup=False,
) as dag:

    load_data = BashOperator(
        task_id="load_excel_to_supabase",
        bash_command="python /opt/airflow/scripts/load_xlsx_to_supabase.py",
        env=env_vars,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt deps && dbt run --profiles-dir .",
        env=env_vars,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
        env=env_vars,
    )

    #  execution order
    load_data >> dbt_run >> dbt_test
