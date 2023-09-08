# Standard libraries
from datetime import datetime
import json
import logging
import os

# Third-party libraries
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
import pytz

# Local modules
from data_360.utils.weather_api import run_weather_forecast_pipeline
import settings

# Constants
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), 'queries')
FILE_LOCAL = ['data_1', 'data_2']
MEXICO_TIMEZONE = pytz.timezone('America/Mexico_City')

# Obtain Airflow variables
URL = Variable.get("url_api_conaguna")
HEADERS_JSON = Variable.get("headers_api_conaguna")
HEADERS = json.loads(HEADERS_JSON)

# Time calculation
utc_now = datetime.utcnow()
mexico_now = utc_now.replace(tzinfo=pytz.utc).astimezone(MEXICO_TIMEZONE)
TIMESTAMP = mexico_now.strftime('%Y-%m-%dT%H')

# Default operators' arguments
default_operators_args = settings.get_default_args()
default_operators_args['owner'] = "cristian urcuqui"

def load_data_to_postgres(file_path, table_name, **kwargs):
    # Obtain the connection from Airflow
    hook = PostgresHook(postgres_conn_id='curso_postgres_conn')
    engine = hook.get_sqlalchemy_engine()

    # Read the DataFrame
    logging.info(f"Reading file from: {file_path}")
    df = pd.read_csv(file_path)

    # Load data into the table
    df.to_sql(table_name, engine, if_exists='append', index=False)

dag = DAG(
    'weather_data_dag',
    default_args=default_operators_args,
    description='DAG to fetch weather data and save to GCS',
    schedule_interval='0 * * * *',
    start_date=datetime(2023, 4, 17),
    max_active_runs=1,
    catchup=False,
    template_searchpath=QUERIES_BASE_PATH,
)

# Tasks
run_this = PostgresOperator(
    task_id='run_sql_script',
    postgres_conn_id='curso_postgres_conn',
    sql='copy_postgres.sql',
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    op_kwargs={
        'file_path': f'/opt/airflow/dags/data_360/files/pronostico_conagua_{TIMESTAMP}.csv',
        'table_name': 'service_pronostico_por_municipios_gz_temp'
    },
    dag=dag,
)

pronostico_municipios = PostgresOperator(
    task_id='pronostico_municipios',
    sql='pronostico_municipios.sql',
    postgres_conn_id='curso_postgres_conn',
    dag=dag
)

data_pronostico = PostgresOperator(
    task_id='data_pronostico',
    sql='data_pronostico.sql',
    postgres_conn_id='curso_postgres_conn',
    dag=dag
)

for data in FILE_LOCAL:
    load_data_task_local = PythonOperator(
        task_id=f'load_data_to_postgres_{data}',
        python_callable=load_data_to_postgres,
        op_kwargs={
            'file_path': f'/opt/airflow/dags/data_360/files/{data}.csv',
            'table_name': 'data_municipios'
        },
        dag=dag,
    )
    data_pronostico >> load_data_task_local >> pronostico_municipios

local_load = PythonOperator(
    task_id='local_load',
    python_callable=run_weather_forecast_pipeline,
    op_args=[URL, HEADERS, f"/opt/airflow/dags/data_360/files/pronostico_conagua_{TIMESTAMP}.csv"],
    dag=dag
)

create_table = PostgresOperator(
    task_id='create_table',
    sql='CREATE_TABLE.sql',
    postgres_conn_id='curso_postgres_conn',
    dag=dag
)


# Task dependencies
create_table >> local_load >> load_data_task >> run_this >>  data_pronostico >> pronostico_municipios
