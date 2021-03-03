CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TABLE catalog_table
(
    path ltree,
    metadata_location varchar(1023),
    previous_metadata_location varchar(1023),
    PRIMARY KEY (path)
);

CREATE INDEX catalog_table_path_idx ON catalog_table USING GIST (path);