from contextlib import closing
from datetime import datetime
import io
import pytz
from settings import execute_snowflake
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

snowflake_conn_id = 'snowflake_conn_id'

def cargar_archivo_mas_reciente_a_snowflake():
    # Configura tus variables
    s3_bucket_name = 'datadd360'
    s3_prefix = 'pronostico/'
    aws_conn_id = 's3_conn_id'
    snowflake_stage = 'data_DD360'

    # Usa S3Hook para listar los objetos en el bucket
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    objects = s3_hook.list_keys(bucket_name=s3_bucket_name, prefix=s3_prefix)

    # Encuentra el archivo más reciente
    latest_file = max(objects, key=lambda x: x.split('/')[-1])

    # Crea una conexión a Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Usa la función COPY INTO de Snowflake para cargar el archivo más reciente desde S3
      # Crea una tabla temporal para almacenar los datos
    create_temp_table_query = f"""
        USE DATABASE CONAGUA_PRONOSTICO;
        USE SCHEMA API_PRONOSTICO_CONAGUA_MX;
        CREATE OR REPLACE TRANSIENT TABLE TEMP_PRONOSTICO_POR_MUNICIPIOS_GZ
        LIKE SERVICE_PRONOSTICO_POR_MUNICIPIOS_GZ;
    """
    snowflake_hook.run(create_temp_table_query)

    # Copia los datos desde S3 a la tabla temporal
    copy_into_temp_query = f"""
        USE DATABASE CONAGUA_PRONOSTICO;
        USE SCHEMA API_PRONOSTICO_CONAGUA_MX;
        COPY INTO TEMP_PRONOSTICO_POR_MUNICIPIOS_GZ
        (nmun,desciel,dh,dirvienc,dirvieng,dpt,dsem,hloc,hr,ides,idmun,lat,lon,nes,nhor,prec,probprec,raf,temp,velvien)
        FROM @{snowflake_stage}/{latest_file}
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    """
    snowflake_hook.run(copy_into_temp_query)

    # Inserta los datos desde la tabla temporal a la tabla de destino con la columna insertd_at
    insert_query = f"""
        USE DATABASE CONAGUA_PRONOSTICO;
        USE SCHEMA API_PRONOSTICO_CONAGUA_MX;
        INSERT INTO SERVICE_PRONOSTICO_POR_MUNICIPIOS_GZ
        (desciel, dh, dirvienc, dirvieng, dpt, dsem, hloc, hr, ides, idmun, lat, lon, nes, nhor, nmun, prec, probprec, raf, temp, velvien, insertd_at)
        SELECT
            temp.desciel, temp.dh, temp.dirvienc, temp.dirvieng, temp.dpt, temp.dsem, temp.hloc, temp.hr, temp.ides, temp.idmun, temp.lat, temp.lon, temp.nes, temp.nhor, temp.nmun, temp.prec, temp.probprec, temp.raf, temp.temp, temp.velvien,
            CONVERT_TIMEZONE('America/Mexico_City', CURRENT_TIMESTAMP()) AS insertd_at
        FROM TEMP_PRONOSTICO_POR_MUNICIPIOS_GZ temp
        LEFT JOIN SERVICE_PRONOSTICO_POR_MUNICIPIOS_GZ dest ON temp.nmun = dest.nmun AND temp.hloc = dest.hloc
        WHERE dest.nmun IS NULL;
    """
    snowflake_hook.run(insert_query)
    rows_inserted = snowflake_hook.get_cursor().rowcount

    return rows_inserted



def after_load_data_to_snowflake(task, table, database,schema, **context):
    registros_insertados = context['ti'].xcom_pull(task_ids=f'{task}', key='return_value')
    fecha_inicio = context['dag_run'].start_date
    if fecha_inicio.tzinfo is None:
        fecha_inicio = fecha_inicio.replace(tzinfo=pytz.UTC)

    # Obtener la fecha de finalización actual y asegurarse de que tenga información de zona horaria
    fecha_finalizacion = datetime.now(pytz.UTC)
    tiempo_duracion = (fecha_finalizacion - fecha_inicio).total_seconds()

    # Resto del có

    query =(f"""

            INSERT INTO CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.SERVICE_PRONOSTICO_AUDIT 
            (registros_insertados,
            tiempo_duracion, 
            fecha_inicio, 
            fecha_finalizacion, 
            nombre_tabla, 
            base_de_datos, 
            esquema) 
            VALUES (
            {0 if registros_insertados is None else registros_insertados},
            {tiempo_duracion},
            '{fecha_inicio}',
            '{fecha_finalizacion}',
            '{table}', 
            '{database}', 
            '{schema}');""")
        
    execute_snowflake(query, snowflake_conn_id, True)
