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
docker-compose up -d   
docker-compose stop
docker-compose restart
```
