-- Создаем новую таблицу TagsNew
CREATE TABLE backup_schema."TagsNew" (
    "Id" SERIAL PRIMARY KEY,
    "TagsData" JSONB  -- Хранит ассоциативный массив {TagKeyId: TagValueId}
);

-- Создаем таблицу для ключей тегов
CREATE TABLE backup_schema."TagKeys" (
    "Id" SERIAL PRIMARY KEY,
    "Name" TEXT UNIQUE NOT NULL
);

-- Создаем таблицу для значений тегов
CREATE TABLE backup_schema."TagValues" (
    "Id" SERIAL PRIMARY KEY,
    "Name" TEXT NOT NULL,
    "TagKeyId" INTEGER REFERENCES backup_schema."TagKeys"("Id") ON DELETE CASCADE
);

-- Вставляем данные в TagKeys из Tags
INSERT INTO backup_schema."TagKeys" ("Name")
SELECT DISTINCT "TagKey"
FROM backup_schema."Tags"
ON CONFLICT ("Name") DO NOTHING;

-- Вставляем данные в TagValues из Tags
INSERT INTO backup_schema."TagValues" ("Id","Name")