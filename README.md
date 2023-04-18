# reto-data_dd360
## Estructura del proyecto

Estos son los archivos y directorios más relevantes del proyecto:
```
.
├── docker #Python dependencies
    ├── airflow
        ├── Dockerfile
        ├── requirements.txt
        ├── start-airflow.sh
    ├── docs
        ├── queries_snowflake.sql # queries ejecutadas en snowflake
    postgres
        ├── Dockerfile
        ├── init-hive-db.txt
├──mnt
    ├── airflow
        ├── dags/ # contiene los dags y puede organizar sus DAG en subcarpetas 
├── .env
├── docker-compose
```
# Comenzando

## Requisitos de instalación

- [Docker](https://docs.docker.com/install/) `2.1.0.1` or greater.

## Iniciando Airflow 
```
Start airflow docker-compose up -d   
Stop airflow docker-compose stop
restart airflow docker-compose restart
```

## Acceder a la interfaz web

Una vez que construya y ejecute Airflow, abra un navegador web y navegue a [http://localhost:8080](http://localhost:8080).
Utilice las siguientes credenciales predeterminadas para iniciar sesión:

> usuario: airflow  
> Contraseña: airflow

Una vez iniciada la sesión, accederá a la interfaz principal de Airflow.

## Se deben crear dos Conexiones para que funcione el flujo 

Se enviarán al correo las credenciales de un usuario creado.
Algo más que podemos ver son los principios en gobiernos de datos.

![conexión de Snowflake](https://github.com/CristianUrcuqui/reto-data_dd360/blob/master/docker/docs/img/conexion_sf.png)

Además se creó un usuario de AWS con acceso a s3.

![conexción de s3](https://github.com/CristianUrcuqui/reto-data_dd360/blob/master/docker/docs/img/s3_conn.png)

##  Iniciado el reto 
```
Teniendo una secuencia nos vamos a guiar en los puntos expuesto es el documento enviado.

El equipo de Ciencia de Datos se ha acercado a ti ya que necesitan incorporar información
sobre clima dentro de sus modelos predictivos. Para ello, han encontrado el siguiente servicio
web: https://smn.conagua.gob.mx/es/web-service-api. Te piden que los apoyes en lo siguiente:

La ruta en la que se guardan toda la lógica es en dags/data_360/utils

* Cada hora debes consumir el último registro devuelto por el servicio de pronóstico por municipio y por hora.

-Para resolver este primer punto se ha creado un archivo .Py  __init__.py. 
-este archivo inicia el proceso descargando la información de la API de pronósticos.

-pronostico_conagua() -> descarga los datos, al acceder a la api lo primero para tener en cuenta, se descarga un archivo .GZ este archivo se debe descomprimir para después leer el .JSON. Esta función retorna la variable data, la cual tiene los datos ya procesados en un dataframe con los datos semi estructurados.
-ultimo_registro(data) -> esta función nos devuelve el último registros de acuerdo al municipio y hora 
-guardar_to_s3(df, s3_bucket_name, file_name) -> esta función se encarga de guardar el archivo en un bucket, se le debe pasar los parámetros data que es del punto a. el nombre del bucket y el nombre del archivo el cual se va guardar con una marca de tiempo de acuerdo a la hora en que se ejecute.
-cargar_archivo() -> se encarga de llamar la función del punto C y aquí es donde le pasamos los parámetros. Esta función es la que se llamará en el DAG. 
```

