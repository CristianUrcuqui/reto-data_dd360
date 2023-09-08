
-- Load data into the temporary table from CSV file (this path should be updated)

-- Insert data from the temporary table into the main table, avoiding conflicts
INSERT INTO service_pronostico_por_municipios_gz
(nmun,desciel,dh,dirvienc,dirvieng,dpt,dsem,hloc,hr,ides,idmun,lat,lon,nes,nhor,prec,probprec,raf,temp,velvien, insertd_at )
SELECT 
temp.nmun,temp.desciel,temp.dh,temp.dirvienc,
temp.dirvieng,temp.dpt,temp.dsem,temp.hloc,temp.hr,temp.ides,
temp.idmun,temp.lat,temp.lon,temp.nes,temp.nhor,temp.prec,temp.probprec,temp.raf,temp.temp,temp.velvien, timezone('America/Mexico_City', now())
FROM service_pronostico_por_municipios_gz_temp temp
LEFT JOIN service_pronostico_por_municipios_gz dest ON temp.nmun = dest.nmun AND temp.hloc = dest.hloc
WHERE dest.nmun IS NULL;

-- Drop the temporary table
DROP TABLE service_pronostico_por_municipios_gz_temp;
