#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  DROP DATABASE IF EXISTS curso;
  DROP ROLE IF EXISTS curso_user;
  CREATE USER curso_user WITH PASSWORD 'curso_password';
  CREATE DATABASE curso;
  GRANT ALL PRIVILEGES ON DATABASE curso TO curso_user;
  \c curso
  \t
  \o /tmp/grant-privs
SELECT 'GRANT SELECT,INSERT,UPDATE,DELETE ON "' || schemaname || '"."' || tablename || '" TO curso_user ;'
FROM pg_tables
WHERE tableowner = CURRENT_USER and schemaname = 'public';
  \o
  \i /tmp/grant-privs
EOSQL
