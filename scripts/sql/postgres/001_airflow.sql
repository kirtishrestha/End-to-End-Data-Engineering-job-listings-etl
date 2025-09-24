CREATE DATABASE airflow_metadata_db;
CREATE DATABASE superset_db;

CREATE USER airflow_user WITH PASSWORD 'airflow_pass';

ALTER ROLE airflow_user SUPERUSER;
CREATE USER superset_user WITH PASSWORD 'superset_pass';
ALTER ROLE superset_user SUPERUSER;

GRANT ALL PRIVILEGES ON DATABASE airflow_metadata_db TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE superset_db TO superset_user;

-- GRANT USAGE ON SCHEMA landing TO your_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA landing TO your_user;