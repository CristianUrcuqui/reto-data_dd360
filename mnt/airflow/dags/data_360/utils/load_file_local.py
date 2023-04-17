import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

def read_files_to_dataframe(file_path1, file_path2):
    df1 = pd.read_csv(file_path1)
    df2 = pd.read_csv(file_path2)
    combined_df = pd.concat([df1, df2], ignore_index=True)
    combined_df.columns.str.upper()

    return combined_df

def load_dataframe_to_snowflake(df, table_name, snowflake_conn_id=None, database_name=None, schema_name = None):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    with snowflake_hook.get_conn() as connection:
        # Establece la base de datos para usar
        if database_name is not None:
            with connection.cursor() as cursor:
                cursor.execute(f"USE DATABASE {database_name}")
                cursor.execute(f"USE SCHEMA {schema_name}")

        success, nchunks, nrows, _ = write_pandas(connection, df, table_name)
        if success:
            print(f'Se han cargado {nrows} filas en la tabla {table_name} en {nchunks} partes.')
        else:
            print('Error al cargar los datos en Snowflake.')


def load_data_to_snowflake(file_path1, file_path2, table_name, snowflake_conn_id, database_name,schema_name):
    combined_df = read_files_to_dataframe(file_path1, file_path2)
    load_dataframe_to_snowflake(combined_df, table_name, snowflake_conn_id,database_name,schema_name)
