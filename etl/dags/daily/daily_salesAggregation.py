from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'neinAlkem',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='DBT_daily_sales_aggregation',
    start_date=datetime(2025,12,1),
    default_args=default_args,
    description='Daily sales aggregation from raw layer to marts layer',
    schedule='0 0 * * *', # Run everyday at 7.00 AM
    catchup=False,
    tags=['dbt','core'],
) as dag:
    
    check_raw_data = SQLExecuteQueryOperator(
        task_id='check_raw_data_updates',
        conn_id='postgres_default',
        sql="SELECT * FROM dev_raw.pos WHERE date_purchased >= CURRENT_DATE - INTERVAL '1 day' AND date_purchased < CURRENT_DATE;"
    )
    
    dbt_run = bash_task = BashOperator(
        task_id="dbt_run_models",
        bash_command="""
            cd /opt/airflow/dbt_project && dbt run --profiles-dir . --target dev
        """
    )
    
    dbt_test = bash_task = BashOperator(
        task_id="dbt_test_models",
        bash_command="""
            cd /opt/airflow/dbt_project && dbt test --profiles-dir .
        """
    )
    
check_raw_data >> dbt_run >> dbt_test