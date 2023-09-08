-- Tabla PRONOSTICO_MUNICIPIOS
CREATE TABLE IF NOT EXISTS pronostico_municipios (
    id SERIAL PRIMARY KEY,
    id_municipio INTEGER NOT NULL,
    nombre_municipio VARCHAR(255),
    dia_local VARCHAR(255),
    temperatura_maxima_promedio NUMERIC,
    temperatura_minima_promedio NUMERIC,
    precipitacion_promedio NUMERIC,
    temperatura_promedio NUMERIC,
    inserted_at TIMESTAMPTZ DEFAULT current_timestamp
);


CREATE TABLE IF NOT EXISTS service_pronostico_por_municipios_gz (
    desciel VARCHAR(255),
    dh NUMERIC,
    dirvienc VARCHAR(255),
    dirvieng DOUBLE PRECISION,
    dpt DOUBLE PRECISION,
    dsem VARCHAR(255),
    hloc VARCHAR(255),
    hr DOUBLE PRECISION,
    ides NUMERIC,
    idmun NUMERIC,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    nes VARCHAR(255),
    nhor NUMERIC,
    nmun VARCHAR(255),
    prec DOUBLE PRECISION,
    probprec DOUBLE PRECISION,
    raf DOUBLE PRECISION,
    temp DOUBLE PRECISION,
    velvien DOUBLE PRECISION,
    insertd_at TIMESTAMPTZ
);


-- Tabla AIRFLOW_TASK_LOGS
CREATE TABLE IF NOT EXISTS airflow_task_logs (
    id SERIAL PRIMARY KEY,
    dag VARCHAR(255),
    task VARCHAR(255),
    last_timestamp TIMESTAMPTZ,
    state VARCHAR(50),
    exception TEXT,
    owner VARCHAR(255),
    finished_at TIMESTAMPTZ,
    duration FLOAT,
    operator VARCHAR(255),
    cron VARCHAR(255),
    inserted_at TIMESTAMPTZ DEFAULT current_timestamp
);


-- Tabla DATA_PRONOSTICO_MUNICIPIO
CREATE TABLE IF NOT EXISTS data_pronostico_municipio (
    id SERIAL PRIMARY KEY,
    cve_ent INT NOT NULL,
    cve_mun INT NOT NULL,
    nombre_municipio VARCHAR(255),
    temperatura_maxima_promedio FLOAT,
    temperatura_minima_promedio FLOAT,
    precipitacion_promedio FLOAT,
    temperatura_promedio FLOAT,
    value FLOAT,
    inserted_at TIMESTAMPTZ DEFAULT current_timestamp
);

-- Limpiar la tabla

