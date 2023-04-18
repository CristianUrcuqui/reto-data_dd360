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

Teniendo una secuencia nos vamos a guiar en los puntos expuesto es el documento enviado.

El equipo de Ciencia de Datos se ha acercado a ti ya que necesitan incorporar información
sobre clima dentro de sus modelos predictivos. Para ello, han encontrado el siguiente servicio
web: https://smn.conagua.gob.mx/es/web-service-api. Te piden que los apoyes en lo siguiente:

La ruta en la que se guardan toda la lógica es en dags/data_360/utils

### Parte 1
```
* Cada hora debes consumir el último registro devuelto por el servicio de pronóstico por municipio y por hora.

-Para resolver este primer punto se ha creado un archivo .Py  __init__.py. 
-este archivo inicia el proceso descargando la información de la API de pronósticos.

-pronostico_conagua() -> descarga los datos, al acceder a la api lo primero para tener en cuenta, se descarga un archivo .GZ este archivo se debe descomprimir para después leer el .JSON. Esta función retorna la variable data, la cual tiene los datos ya procesados en un dataframe con los datos semi estructurados.
-ultimo_registro(data) -> esta función nos devuelve el último registros de acuerdo al municipio y hora 
-guardar_to_s3(df, s3_bucket_name, file_name) -> esta función se encarga de guardar el archivo en un bucket, se le debe pasar los parámetros data que es del punto a. el nombre del bucket y el nombre del archivo el cual se va guardar con una marca de tiempo de acuerdo a la hora en que se ejecute.
-cargar_archivo() -> se encarga de llamar la función del punto C y aquí es donde le pasamos los parámetros. Esta función es la que se llamará en el DAG. 
```
### parte 2
```
* A partir de los datos extraídos en el punto 1, generar una tabla a nivel municipio en la que cada registro contenga
* el promedio de temperatura y precipitación de las últimas dos horas.

- cargar_archivo_mas_reciente_a_snowflake() -> esta funcion se encarga de escoger el archivo más reciente del bucket de s3, posterior a eso se carga a snowflake,
- el primer paso es crear una tabla temporal create_temp_table_query
- creamos un stage en snowflake el cual se va encargar de hacer la comunicación entre s3 y snowflake
mas en https://docs.snowflake.com/en/sql-reference/sql/create-stage
- insert_query insertamos la data de la tabla temporal en la tabla original con un current, 
además esto nos ayuda a evitar duplicidad en los datos.
para esta función retornamos el número de row insertados, esto con el fin de auditar los datos que se inserta, 
por que hacer esto? pensé en esto debido    a que esta tabla es nuestra tabla máster,
y en necesario hacer esto en caso de querer conciliar la información de acuerdo a la data que se inserta en 
s3 y la data que se inserta en snowflake 

la función es la que se va utilizar en el dag.

lo enunciado anteriormente se ejecuta en la función after_load_data_to_snowflake(task, table, database,schema, **context)
esta función se usará en el dag se le deben pasar los parámetros, task de XCOM
Nombre de la tabla que queremos auditar, base de datos y esquema.

```
> Consultar la siguente tabla: SELECT * FROM CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.SERVICE_PRONOSTICO_POR_MUNICIPIOS_GZ;

