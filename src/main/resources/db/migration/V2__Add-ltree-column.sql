-- noinspection SqlNoDataSourceInspectionForFile

CREATE EXTENSION ltree;
ALTER TABLE dataset
    ADD COLUMN path_ltree ltree;

-- Replace special chars by _ and then all / to ..
UPDATE Dataset
SET path_ltree = text2ltree(
        regexp_replace(
                regexp_replace(
                        substring(path from 2),
                        '[^/|\w]+', '_', 'g'
                    ),
                '/', '.', 'g'
            )
    );