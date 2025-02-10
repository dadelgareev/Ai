
CREATE SCHEMA IF NOT EXISTS backup_schema;

CREATE TABLE backup_schema.Tags AS
TABLE public.Tags;

SELECT * FROM backup_schema.Tags LIMIT 10;

-- 1. Добавляем новую колонку с автоинкрементом
ALTER TABLE backup_schema."Tags"
ADD COLUMN new_id SERIAL PRIMARY KEY;

-- 2. Удаляем старый UUID ID
ALTER TABLE backup_schema."Tags"
DROP COLUMN "Id";

-- 3. Переименовываем новую колонку
ALTER TABLE backup_schema."Tags"
RENAME COLUMN new_id TO "Id";