from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from data_360.utils.__init__ import cargar_archivo
from data_360.utils.data_to_snowflake import cargar_archivo_mas_reciente_a_snowflake,after_load_data_to_snowflake
from data_360.utils.load_file_local import load_data_to_snowflake
import settings 


queries_base_path = os.path.join(os.path.dirname(__file__), 'queries')
default_operators_args = settings.get_default_args()
default_operators_args['owner'] = "cristian urcuqui"

dag = DAG(
    'weather_data_dag',
    default_args=default_operators_args,
    description='DAG to fetch weather data and save to GCS',
    schedule_interval='0 * * * *',
    start_date=datetime(2023, 4, 17),
    max_active_runs=1,
    catchup=False,
    template_searchpath=queries_base_path,
)


s3_load = PythonOperator(
            task_id='s3_load',
            python_callable=cargar_archivo,
            dag=dag,
        )

to_snowflake = PythonOperator(
            task_id='to_snowflake',
            python_callable=cargar_archivo_mas_reciente_a_snowflake,
            provide_context=True,
            dag=dag,
        )

after_audit = PythonOperator(
            task_id='after_audit',
            python_callable=after_load_data_to_snowflake,
            op_kwargs={
                'task' : 'to_snowflake',
                'table': 'SERVICE_PRONOSTICO_POR_MUNICIPIOS_GZ',
                'database': 'CONAGUA_PRONOSTICO',
                'schema': 'API_PRONOSTICO_CONAGUA_MX'
            },
            dag=dag,
        )

pronostico_municipios = SnowflakeOperator(
            task_id='pronostico_municipios',
            sql='PRONOSTICO_MUNICIPIOS.sql',
            snowflake_conn_id= 'snowflake_conn_id',
            dag=dag
    )

trancate_table = SnowflakeOperator(
            task_id='trancate_table',
            sql='CREATE_TABLE.sql',
            snowflake_conn_id= 'snowflake_conn_id',
            dag=dag
    )

load_local = PythonOperator(
            task_id='load_local',
            python_callable=load_data_to_snowflake,
            op_kwargs={
                'file_path1': '/opt/airflow/dags/data_360/files/data_1.csv',
                'file_path2': '/opt/airflow/dags/data_360/files/data_2.csv',
                'table_name': 'DATA_MUNICIPIOS',
                'snowflake_conn_id': 'snowflake_conn_id',
                'database_name' : 'CONAGUA_PRONOSTICO',
                'schema_name': 'API_PRONOSTICO_CONAGUA_MX',
            },
            dag=dag,
        )

data_pronostico = SnowflakeOperator(
            task_id='data_pronostico',
            sql='DATA_PRONOSTICO_MUNICIPIO.sql',
            snowflake_conn_id= 'snowflake_conn_id',
            dag=dag
    )


trancate_table >> s3_load >> to_snowflake  >> after_audit >> pronostico_municipios >> [load_local >> data_pronostico]

