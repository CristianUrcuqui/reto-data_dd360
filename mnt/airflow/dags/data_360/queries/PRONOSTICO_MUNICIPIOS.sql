--INSERTA INFORMACION SOLAMENTE TRAYENDO LA DATA DE LAS ULTIMAS DOS HORAS
INSERT INTO CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.PRONOSTICO_MUNICIPIOS
    (id_municipio, nombre_municipio, precipitacion_promedio, temperatura_promedio, insertd_at, dia_local)
WITH CTE_PROMEDIO AS (
    SELECT
        idmun AS id_municipio,
        nmun AS nombre_municipio,
        AVG(temp) AS temperatura_promedio,
        AVG(probprec) AS precipitacion_promedio,
        hloc
    FROM CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.SERVICE_PRONOSTICO_POR_MUNICIPIOS_GZ
    --WHERE hloc BETWEEN CONVERT_TIMEZONE('America/Mexico_City', CURRENT_TIMESTAMP() - INTERVAL '2 HOUR') AND 
    --CONVERT_TIMEZONE('America/Mexico_City', CURRENT_TIMESTAMP())
    GROUP BY idmun, nmun, hloc
)
SELECT 
    id_municipio,
    nombre_municipio,
    precipitacion_promedio,
    temperatura_promedio,
    CONVERT_TIMEZONE('America/Mexico_City', CURRENT_TIMESTAMP()) AS insertd_at,
    hloc
FROM CTE_PROMEDIO
WHERE NOT EXISTS (
    SELECT 1
    FROM CONAGUA_PRONOSTICO.API_PRONOSTICO_CONAGUA_MX.PRONOSTICO_MUNICIPIOS PM
    WHERE PM.id_municipio = CTE_PROMEDIO.id_municipio
    AND PM.dia_local = CTE_PROMEDIO.hloc
);
