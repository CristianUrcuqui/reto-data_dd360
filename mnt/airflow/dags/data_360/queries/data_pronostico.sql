--INSERTA INFORMACION SOLAMENTE TRAYENDO LA DATA DE LAS ULTIMAS DOS HORAS
INSERT INTO PRONOSTICO_MUNICIPIOS
    (id_municipio, nombre_municipio, precipitacion_promedio, temperatura_promedio, inserted_at, dia_local)
WITH CTE_PROMEDIO AS (
    SELECT
        idmun AS id_municipio,
        nmun AS nombre_municipio,
        AVG(temp) AS temperatura_promedio,
        AVG(probprec) AS precipitacion_promedio,
        hloc
    FROM SERVICE_PRONOSTICO_POR_MUNICIPIOS_GZ
    --WHERE hloc BETWEEN (CURRENT_TIMESTAMP - INTERVAL '2 HOUR') AT TIME ZONE 'America/Mexico_City' AND 
    --CURRENT_TIMESTAMP AT TIME ZONE 'America/Mexico_City'
    GROUP BY idmun, nmun, hloc
)
SELECT 
    id_municipio,
    nombre_municipio,
    precipitacion_promedio,
    temperatura_promedio,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Mexico_City' AS inserted_at,
    hloc
FROM CTE_PROMEDIO
WHERE NOT EXISTS (
    SELECT 1
    FROM PRONOSTICO_MUNICIPIOS PM
    WHERE PM.id_municipio = CTE_PROMEDIO.id_municipio
    AND PM.dia_local = CTE_PROMEDIO.hloc
);

