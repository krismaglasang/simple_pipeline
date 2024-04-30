from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import DAG

from datetime import datetime, timedelta

default_args = {
    "owner": "krismaglasang",
    "retries": 5,
    "retry_delay": timedelta(minutes=3)
}

def print_query_results(ti):
    print(ti.xcom_pull(task_ids="source_db"))

with DAG(
    default_args=default_args,
    dag_id="sourcing_v2",
    description="Source data from Postgres DB",
    start_date=datetime(2024, 4, 25),
    schedule_interval='0 * * * *'
) as dag:
    query_db = SQLExecuteQueryOperator(
        task_id="source_db",
        conn_id="postgres_docker",
        sql="select * from imdb_dataset limit 3",
        do_xcom_push=True
    )
    
    print_results = PythonOperator(
        task_id="print_query",
        python_callable=print_query_results
    )
    
    query_db >> print_results
