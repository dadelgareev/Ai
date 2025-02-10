CREATE TABLE IF NOT EXISTS "TagsNew" (
    "Id" SERIAL PRIMARY KEY,
    "CardId" UUID NOT NULL,
    "TagKey" TEXT NOT NULL,
    "TagValue" TEXT NOT NULL
);

INSERT INTO "TagsNew" ("CardId", "TagKey", "TagValue")
SELECT
    c."Id" AS "CardId",
    t."TagKey",
    t."TagValue"
FROM "Cards" c
JOIN "CardTags" ct ON c."Id" = ct."CardId"
JOIN "Tags" t ON ct."TagId" = t."Id";

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

INSERT INTO backup_schema."TagKeys" ("Id","Name") VALUES
(0,'Стиль'),
(1,'Цвет'),
(2,'Сезон'),
(3,'Принты');

INSERT INTO backup_schema."TagValues" ("Name", "TagKeyId")
SELECT DISTINCT t."TagValue", tk."Id"
FROM backup_schema."TagsNew" t
JOIN backup_schema."TagKeys" tk ON
    (t."TagKey" = 'Рисунок' AND tk."Id" = 3)
    OR (t."TagKey" IN ('Сезон', 'Сезон 2') AND tk."Id" = 2)
    OR (t."TagKey" = 'Цвет' AND tk."Id" = 1);

CREATE TABLE public."TagsJSON" (
    "Id" SERIAL PRIMARY KEY,
    "CardId" UUID NOT NULL,
    "TagsJson" JSONB NOT NULL
);

-- 1. Удаляем существующие записи, чтобы избежать конфликта
DELETE FROM public."TagsJSON"
WHERE "CardId" IN (
    SELECT DISTINCT c."Id"
    FROM public."Cards" c
    JOIN public."SubCategories" sc ON c."Id" = sc."CardId"
    JOIN public."SubCategory" subcat ON sc."SubCategoryId" = subcat."Id"
    JOIN public."Categories" cat ON subcat."CategoryId" = cat."Id"
    WHERE cat."Name" IN (
        'Домашняя одежда', 'Купальники и пляжная одежда', 'Нижнее белье', 'Носки, чулки, колготки', 'Прочее',
        'Блузы и рубашки', 'Брюки', 'Джинсы', 'Комбинезоны', 'Платья и сарафаны', 'Топы и майки', 'Футболки и поло',
        'Худи и свитшоты', 'Шорты', 'Юбки', 'Балетки', 'Ботинки', 'Кроссовки и кеды', 'Мокасины и топсайдеры',
        'Сандалии', 'Слипоны', 'Джемперы, свитеры, кардиганы', 'Верхняя одежда', 'Резиновая обувь', 'Сабо и мюли',
        'Пиджаки и костюмы', 'Вечерняя обувь', 'Обувь с увеличенной полнотой', 'Сапоги', 'Туфли', 'Ботильоны'
    )
);

-- 2. Вставляем новые данные в TagsJSON
INSERT INTO public."TagsJSON" ("CardId", "TagsJson")
SELECT
    c."Id" AS "CardId",
    jsonb_build_object(tk."Id", tv."Id") AS "TagsJson"
FROM public."Cards" c
JOIN public."SubCategories" sc ON c."Id" = sc."CardId"
JOIN public."SubCategory" subcat ON sc."SubCategoryId" = subcat."Id"
JOIN public."Categories" cat ON subcat."CategoryId" = cat."Id"
JOIN public."TagKeys" tk ON tk."Name" = 'Стиль'
JOIN public."TagValues" tv ON tv."TagKeyId" = tk."Id"
WHERE (
    (cat."Name" IN ('Домашняя одежда', 'Купальники и пляжная одежда', 'Нижнее белье', 'Носки, чулки, колготки', 'Прочее') AND tv."Name" = 'Свободный') OR
    (cat."Name" IN ('Блузы и рубашки', 'Брюки', 'Джинсы', 'Комбинезоны', 'Платья и сарафаны', 'Топы и майки', 'Футболки и поло',
                    'Худи и свитшоты', 'Шорты', 'Юбки', 'Балетки', 'Ботинки', 'Кроссовки и кеды', 'Мокасины и топсайдеры',
                    'Сандалии', 'Слипоны') AND tv."Name" = 'Повседневный') OR
    (cat."Name" IN ('Джемперы, свитеры, кардиганы', 'Верхняя одежда', 'Резиновая обувь', 'Сабо и мюли', 'Кроссовки и кеды') AND tv."Name" = 'Спортивный') OR
    (cat."Name" IN ('Пиджаки и костюмы', 'Вечерняя обувь', 'Обувь с увеличенной полнотой', 'Сапоги', 'Туфли', 'Ботильоны') AND tv."Name" = 'Деловой')
);


UPDATE public."TagsJSON" AS tj
SET "TagsJson" = tj."TagsJson" || new_data."TagsJson"
FROM (
    SELECT
        t."CardId",
        jsonb_object_agg(tv."TagKeyId", tv."Id") AS "TagsJson"
    FROM public."TagsNew" t
    JOIN public."TagKeys" tk ON t."TagKey" = tk."Name"
    JOIN public."TagValues" tv ON t."TagValue" = tv."Name" AND tk."Id" = tv."TagKeyId"
    GROUP BY t."CardId"
) AS new_data
WHERE tj."CardId" = new_data."CardId";
