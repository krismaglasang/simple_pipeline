from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

default_args = {
    "owner": "krismaglasang",
    "retries": 5,
    "retry_delay": timedelta(minutes=3)
}

@dag(
    default_args=default_args,
    dag_id="sourcing",
    description="Source data from Postgres DB",
    start_date=datetime(2024, 4, 25),
    schedule_interval='0 * * * *'
)
def source_and_dump():
    
    @task
    def source_db():
        conn =  psycopg2.connect(
            dbname=os.getenv("IMDB_PG_DB"),
            user=os.getenv("IMDB_PG_USERNAME"),
            password=os.getenv("IMDB_PG_PASSWORD"),
            host="localhost",
            port=os.getenv("IMDB_PG_PORT"), 
        )
        
        cursor = conn.cursor()
        query = "select * from imdb_dataset limit 3"
        cursor.execute(query)
        [print(item) for item in cursor.fetchall()]
        
    source_db()
    
source_and_dump_instance = source_and_dump()
    
    