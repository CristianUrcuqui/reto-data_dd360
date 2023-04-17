from datetime import datetime
import gzip
import io
import json
import logging
import os
import requests
import pandas as pd
import pytz
from botocore.exceptions import NoCredentialsError
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



mexico_timezone = pytz.timezone('America/Mexico_City')
utc_now = datetime.utcnow()
mexico_now = utc_now.replace(tzinfo=pytz.utc).astimezone(mexico_timezone)



def pronostico_conagua():
    url = "https://smn.conagua.gob.mx/webservices/?method=3"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    
    try:
        response = requests.get(url, headers=headers)
        gz_stream = io.BytesIO(response.content)
        
        with gzip.GzipFile(fileobj=gz_stream, mode="rb") as f:
            try:
                json_data = json.load(f)
            except EOFError:
                logging.error("No se descargo el archivo.")
                return None
        
        df_data = []
        for item in json_data:
            row = {}
            for key, value in item.items():
                row[key] = value
            df_data.append(row)

        data = pd.DataFrame(df_data)

        return data
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return None


def ultimo_registro(data):
    if data is None:
        logging.error("No se pueden obtener los últimos registros porque los datos son None.")
        return None

    data['hloc'] = pd.to_datetime(data['hloc'])
    last_values = data.sort_values(by='hloc').groupby('nmun', as_index=False).last()
    return last_values


def guardar_to_s3(df, s3_bucket_name, file_name):
    if df is None:
        logging.error("No se pueden guardar los datos en Amazon S3 porque el DataFrame es None.")
        return

    csv_data = df.to_csv(index=False)
    local_file_name = f'/tmp/{file_name}'
    with open(local_file_name, 'w') as f:
        f.write(csv_data)

    # Crea una instancia de S3Hook utilizando la conexión de Airflow
    s3_hook = S3Hook(aws_conn_id="s3_conn_id")  

    try:
        # Usa S3Hook para subir el archivo a S3
        s3_hook.load_file(
            filename=local_file_name,
            key=f'pronostico/{file_name}',
            bucket_name=s3_bucket_name,
            replace=True  # Sobrescribe el archivo si ya existe
        )
        print(f"Archivo {file_name} Se cargo el archivo correctamente.")
    except Exception as e:
        logging.error(f"Fallo el cargue : {str(e)}")

    # Elimine el archivo local después de subirlo a S3
    os.remove(local_file_name)

# def cargar_ultimo_archivo_s3(s3_bucket_name, prefix):
#     s3_hook = S3Hook(aws_conn_id="s3_conn_id")
#     files = s3_hook.list_keys(bucket_name=s3_bucket_name, prefix=prefix)
#     if files:
#         last_file = sorted(files)[-1]
#         last_file_content = s3_hook.read_key(key=last_file, bucket_name=s3_bucket_name)

#         # Verifica si el contenido del archivo no está vacío antes de leerlo con pd.read_csv
#         if last_file_content.strip():
#             return pd.read_csv(io.StringIO(last_file_content))
#         else:
#             logging.warning(f"El último archivo en S3 ({last_file}) está vacío.")
#             return None
#     else:
#         return None


def cargar_archivo():
    timestamp = mexico_now.strftime('%Y-%m-%dT%H-%M-%S')
    file_name = f"pronostico_conagua_{timestamp}.csv"
    data = pronostico_conagua()
    last_values = ultimo_registro(data)

    # Carga el último archivo desde S3
    s3_bucket_name = 'datadd360'
    prefix = 'pronostico/'
    #last_file_df = cargar_ultimo_archivo_s3(s3_bucket_name, prefix)

    if last_values is None:
        logging.error("No se pueden guardar los últimos registros en Amazon S3.")
        return

    # if last_file_df is not None:
    #     # Compara el archivo actual con el anterior y filtra los registros nuevos
    #     merged_df = pd.merge(last_values, last_file_df, on='nmun', how='outer', indicator=True)
    #     new_records = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    #     # Guarda solo los registros nuevos en S3
    #     if not new_records.empty:
    #         guardar_to_s3(new_records, s3_bucket_name, file_name)
    #     else:
    #         logging.info("No hay registros nuevos para guardar en Amazon S3.")
    # else:
    #     # Si no hay archivo anterior, guarda el archivo actual completo
    guardar_to_s3(last_values, s3_bucket_name, file_name)
