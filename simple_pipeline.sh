source .env
airflow webserver -p ${AIRFLOW_PORT} -D & airflow scheduler