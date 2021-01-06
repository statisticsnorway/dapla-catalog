-- noinspection SqlNoDataSourceInspectionForFile

CREATE EXTENSION IF NOT EXISTS ltree;
ALTER TABLE dataset
    ADD COLUMN path_ltree ltree;

CREATE INDEX path_gist_idx ON dataset USING GIST (path_ltree);