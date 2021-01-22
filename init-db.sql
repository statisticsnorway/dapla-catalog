-- noinspection SqlNoDataSourceInspectionForFile

-- Catalog
CREATE USER catalog WITH PASSWORD 'catalog';
CREATE DATABASE catalog;
GRANT ALL PRIVILEGES ON DATABASE catalog TO catalog;
ALTER ROLE catalog SUPERUSER;
