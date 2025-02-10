CREATE TABLE backup_schema."TagValuesNew" (
    "Id" SERIAL PRIMARY KEY,
    "Value" TEXT NOT NULL,
    "TagKeyId" INTEGER NOT NULL,
    "OldId" INTEGER NOT NULL
);

INSERT INTO backup_schema."TagValuesNew" ("Value", "TagKeyId", "OldId")
SELECT
    tv."Value",
    tv."TagKeyId",
    tv."Id" AS "OldId"
FROM backup_schema."TagValues" tv
WHERE tv."TagKeyId" != 1  -- Оставляем неизменные записи
UNION ALL
SELECT
    trim(unnested_colors),  -- Разбиваем по цветам и убираем пробелы
    tv."TagKeyId",
    tv."Id" AS "OldId"
FROM backup_schema."TagValues" tv,
LATERAL unnest(string_to_array(tv."Value", ',')) AS unnested_colors
WHERE tv."TagKeyId" = 1;  -- Обрабатываем только цвета

-- Обновляем TagsJSONNew, заменяя старые Id на массив новых
INSERT INTO backup_schema."TagsJSONNew" ("CardId", "TagsJson")
SELECT
    tj."CardId",
    jsonb_set(
        tj."TagsJson",
        '{1}',  -- Поле "1" (цвет)
        COALESCE(
            (SELECT jsonb_agg(tvn."Id")  -- Собираем новые Id в массив
             FROM backup_schema."TagValuesNew" tvn
             WHERE tvn."OldId" = (tj."TagsJson"->>'1')::INTEGER),
            '[]'::jsonb  -- Если нет значений, записываем пустой массив
        )
    )
FROM backup_schema."TagsJSON" tj
WHERE tj."TagsJson" ? '1'  -- Фильтруем только записи с ключом "1"
AND EXISTS (
    SELECT 1 FROM backup_schema."TagValuesNew" tvn
    WHERE tvn."OldId" = (tj."TagsJson"->>'1')::INTEGER
);

