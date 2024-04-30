from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {
    "owner": "krismaglasang",
    "retries": 5,
    "retry_delay": timedelta(minutes=3)
}

@dag(
    default_args=default_args,
    dag_id="sourcing_v3",
    description="Source data from Postgres DB",
    start_date=datetime(2024, 4, 29),
    schedule_interval='0 * * * *'
)
def source_and_dump():
    @task
    def source_db():
        return SQLExecuteQueryOperator(
            task_id="source_db",
            conn_id="postgres_docker",
            sql="select * from imdb_dataset limit 3",
            do_xcom_push=True
        ).execute(context={})
        
    @task
    def print_query_results(query_result):
        print(query_result)
        
    print_query_results(source_db())
    
source_and_dump_instance = source_and_dump()