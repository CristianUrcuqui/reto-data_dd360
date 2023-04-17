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

## Iniciado Airflow 
```
inicar airflow docker-compose up -d   
parar airflow docker-compose stop
reiniciar airflow docker-compose restart
```

## Acceder a la interfaz web

Una vez que construya y ejecute Airflow, abra un navegador web y navegue a [http://localhost:8080](http://localhost:8080).
Utilice las siguientes credenciales predeterminadas para iniciar sesión:

> usuario: airflow  
> Contraseña: airflow

Una vez iniciada la sesión, accederá a la interfaz principal de Airflow.

## Se deben crear dos Conexiones para que funcione el flujo 

Se enviarán al correo las credenciales de un usuario creado.
Algo más que podemos ver son los principios en gobiernos de datos

![conexión de Snowflake](https://github.com/CristianUrcuqui/reto-data_dd360/blob/master/docker/docs/img/conexion_sf.png)

Además se creó un usuario de AWS con acceso a s3 de igual se envia los access key  

![conexción de s3](https://github.com/CristianUrcuqui/reto-data_dd360/blob/master/docker/docs/img/s3_conn.png)

