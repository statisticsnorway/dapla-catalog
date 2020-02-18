-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE Dataset
(
    path varchar(1023),
    version    timestamp,
    document   jsonb,
    PRIMARY KEY (path, version)
);
