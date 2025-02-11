TRUNCATE TABLE public."TagValues";

INSERT INTO public."TagValues" ("Id", "Value", "TagKeyId")
SELECT "Id", "Value", "TagKeyId"
FROM backup_schema."TagValues";

SELECT setval(pg_get_serial_sequence('public."TagValues"', 'Id'), COALESCE(MAX("Id"), 1), false)
FROM public."TagValues";


UPDATE public."Cards" c
SET "Tags" = t."TagsJsonIDArray"
FROM backup_schema."TagsJSONIDArray" t
WHERE c."Id" = t."CardId";

