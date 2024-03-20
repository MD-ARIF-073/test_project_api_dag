import json
from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.api import auth
import airflow

with DAG(
    dag_id = 'test_project_api_dag',
    start_date = datetime(2024, 3, 20),
    catchup = False,
    schedule_interval= '*/10 * * * * *'
) as dag:
    
    api_connection = Connection.get_connection_from_secrets("test_project_api")
    
    # task_is_api_active = HttpSensor(
    #     task_id = 'is_api_active',
    #     http_conn_id='beza_service_public_api',
    #     endpoint='banks/',
    #     method='GET'
    # )

    task_get_api_response = SimpleHttpOperator(
        task_id='get_api_response',
        http_conn_id= api_connection.conn_id,
        method= 'GET',
        endpoint = 'books/list-info',
        response_filter = lambda response: json.loads(response.text),  # A lambda function to process the HTTP response by converting it from JSON to a Python object using json.loads(response.text)
        log_response = True,
        dag=dag
        
    )

    # logging.info(f'TEST LOG : {task_get_api_response.headers}')

    # task_is_api_active >> 
    task_get_api_response
    
    