from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Definir los argumentos predeterminados
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instanciar un objeto DAG
dag = DAG(
    'crear_tabla_curso',
    default_args=default_args,
    description='Un DAG simple para crear una tabla en PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 5),
    catchup=False,
)

# Tarea para crear una tabla
crear_tabla = PostgresOperator(
    task_id='crear_tabla',
    postgres_conn_id='curso_postgres_conn',  # Aquí debes usar el ID de la conexión que has creado
    sql="""
    CREATE TABLE IF NOT EXISTS mi_tabla (
        id SERIAL PRIMARY KEY,
        nombre VARCHAR(255) NOT NULL,
        edad INT
    );
    """,
    dag=dag,
)

# Definir el orden de las tareas
crear_tabla
