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

además se utiliza el operador Snowflake operator para ejecutar el archivo PRONOSTICO_MUNICIPIOS.sql el cual tiene el análisis de los promedios 

la función es la que se va utilizar en el dag 


```
> SELECT * FROM CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.SERVICE_PRONOSTICO_POR_MUNICIPIOS_GZ; Contiene los datos maestros > >
> SELECT * FROM CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.PRONOSTICO_MUNICIPIOS; Contiene los datos del análisis de promedios 

### Parte 3
```
* Hay una carpeta “data_municipios” que contiene datos a nivel municipio organizados
por fecha, cada vez que tu proceso se ejecute debe generar una tabla en la cual se
crucen los datos más recientes de esta carpeta con los datos generados en el punto 2.

- para esto cargamos los archivos en un carpeta local ubicación dags/data_360/files

- con los archivos en el entorno local podemos crear un función la cual mezcla los dos archivos 
read_files_to_dataframe -> el cual requiere el archivo 1 y el 2 esto retorna la combinación de los df combined_df

- load_dataframe_to_snowflake -> cargamos el resultado del df a snowflake 

- load_data_to_snowflake -> esta función es la que llamaremos en el dag le pasamos estos paramentos (file_path1, file_path2, table_name, snowflake_conn_id, database_name,schema_name)

la tabla en la que guarda la data se trunca cada vez que inicia el proceso.
```
esta tabla contiene los datos de los archivos locales 
> SELECT * FROM CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.DATA_MUNICIPIOS;
```
El nombre del archivo DATA_PRONOSTICO_MUNICIPIO.sql cruza los datos cada que se ejecuta el proceso y crea la siguiente tabla 
```
> SELECT * FROM CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.DATA_PRONOSTICO_MUNICIPIO;

### Parte 4 
```
Versiona todas tus tablas de acuerdo a la fecha y hora en la que se ejecutó el proceso,
en el caso del entregable del punto 3, además, genera una versión “current” que
siempre contenga una copia de los datos más recientes.

Las tablas creadas contiene una la columna insertd_at, la cual contiene la fecha en la que se insertan los datos a las tablas, teniendo asi una marca de tiempo de los datos insertados 

además se creo una vista en snowflake 

esta tabla contiene los datos más recientes del punto 3 
```
> SELECT * FROM  CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.DATA_PRONOSTICO_MUNICIPIO_version_current;

# Extras 
## Logs
```
Este proceso guarda los logs que se generan en airflow, además estos logs se guardan en snowflake  para ello se ha creado un archivo .Py llamado settings.py el cual contiene funciones que nos ayudan a guardar los logs en snowflake y ejecutar consultas en snowflake 
```
se pueden consultar los logs de airflow en la siguiente tabla 

> SELECT * FROM CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.AIRFLOW_TASK_LOGS;


# Dag 

El Dag llamado weather_data_dag.py se ejecuta cada hora.

## Preguntas adicionales
● ¿Qué mejoras propondrías a tu solución para siguientes versiones?
He tratado de utilizar herramientas que están a la vanguardia de este tipo de retos como lo es S3; snowflake. intente hacer un ejercicio lo más real posible, por ende creo que una mejora para una siguiente versión seria ver nuevas arquitecturas como análisis en tiempo real, con KSQL y Kafka, O tocar temas de data catalog para no perder la trazabilidad de los datos en cuanto a definiciones. demás de alertamientos y notificaciones, puede ser por slack o al correo informado si falla o si queremos mensajes más informativos,
el código propuesto es algo genérico para el reto, habría que modificacione algunas funciones para que se pueden adaptar a cualquier tiempo de requerimiento de este tipo.

● Tu solución le ha gustado al equipo y deciden incorporar otros procesos, habrá nuevas
personas colaborando contigo, ¿Qué aspectos o herramientas considerarías para
escalar, organizar y automatizar tu solución?

una forma de escalar esto es creando un gobierno de datos, de esa forma los equipos solo se centran en lo que les interesan, además de proteger la integridad de los datos. 

empezar a usar roles para cada desarrollador, con el fin de poder asegurarse que el código que se proporciona es de calidad.

airflow es una buena herramienta de orquestación, por ende seria lo mejor empezar a mirar como se migar a un kubernetes y empezar a trabajar con jenkins para hacer pasos a producción mientras la máquina sigue prendida  

github o bitbucket son herramientas para control de versión que se puede seguir usado

snowflake es una buena herramienta para procesar información en grandes masas, se adapta muy bien a cualquier entorno.

Aws, S3 a mi parecer es una buena práctica almacenar los datos, ya que se pueden utilizar en los requerimientos que queramos, sin tener que llenar la memoria de nuestra instancia de airflow 





