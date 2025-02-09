
CREATE SCHEMA IF NOT EXISTS backup_schema;

CREATE TABLE backup_schema.Tags AS
TABLE public.Tags;

SELECT * FROM backup_schema.Tags LIMIT 10;
