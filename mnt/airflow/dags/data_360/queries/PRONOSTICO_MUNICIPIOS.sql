INSERT INTO data_pronostico_municipio (
    cve_ent,
    cve_mun,
    nombre_municipio,
    precipitacion_promedio,
    temperatura_promedio,
    value,
    inserted_at
)
SELECT distinct
    dm.cve_ent  ,
    dm.cve_mun ,
    p2.nombre_municipio,
    p2.precipitacion_promedio,
    p2.temperatura_promedio ,
    dm.value_mun,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Mexico_City' AS inserted_at
FROM
    data_municipios dm
INNER JOIN
    pronostico_municipios p2
ON
    dm.cve_mun = p2.id_municipio
WHERE
    dm.cve_ent = (SELECT max(DATE_PART('DAY', dia_local::date)) FROM PRONOSTICO_MUNICIPIOS);
