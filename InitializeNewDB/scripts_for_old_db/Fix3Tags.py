import json
import psycopg2

DB_CONFIG_ROW = {
    "host": "193.232.55.5",
    "database": "postgres",
    "user": "postgres",
    "password": "super",
    "port": 5500
}


def fetch_tag_values():
    """Загружает соответствие Value -> Id из TagValuesNew"""
    query = 'SELECT "Value", "Id" FROM backup_schema."TagValuesNew";'

    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return {value: tag_id for value, tag_id in cur.fetchall()}


def fetch_tags_json_readable():
    """Загружает данные из TagsJSONReadableArray"""
    query = 'SELECT "Id", "CardId", "TagsJsonReadableArray" FROM backup_schema."TagsJSONReadableArray";'

    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()  # [(Id, CardId, TagsJson), ...]


def convert_tags_to_ids(tags_json, tag_value_map):
    """Заменяет текстовые значения в JSON на их Id"""
    converted_tags = {
        tag_key: [tag_value_map[val] for val in values if val in tag_value_map]
        for tag_key, values in tags_json.items()
    }
    return json.dumps(converted_tags)


def insert_tags_json_id_array(data):
    """Записывает преобразованные данные в TagsJSONIDArray"""
    query = '''
        INSERT INTO backup_schema."TagsJSONIDArray" ("Id", "CardId", "TagsJSONIDArray")
        VALUES (%s, %s, %s);
    '''

    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            cur.executemany(query, data)
            conn.commit()


def migrate_tags():
    """Основной процесс миграции"""
    tag_value_map = fetch_tag_values()
    tags_json_readable = fetch_tags_json_readable()

    converted_data = [
        (row_id, card_id, convert_tags_to_ids(tags_json, tag_value_map))
        for row_id, card_id, tags_json in tags_json_readable
    ]

    insert_tags_json_id_array(converted_data)
    print(f"Успешно перенесено {len(converted_data)} записей в TagsJSONIDArray.")


# Запуск скрипта
if __name__ == "__main__":
    migrate_tags()
