ALTER TABLE dataset
    DROP COLUMN path;

ALTER TABLE dataset
    RENAME COLUMN path_ltree to path;

ALTER TABLE dataset
    ADD PRIMARY KEY (path, version);