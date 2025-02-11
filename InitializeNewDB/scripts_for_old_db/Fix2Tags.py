import json
import psycopg2
from db.db_config import DB_CONFIG_ROW


def fetch_tagsjson_readable():
    """Получает данные из TagsJSONReadable"""
    query = 'SELECT "CardId", "TagsJsonReadable" FROM backup_schema."TagsJSONReadable";'
    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()  # [(CardId, TagsJsonReadable), ...]


def transform_tagsjson(tagsjson):
    """Преобразует JSON-объект в нужный формат"""
    transformed_json = {}
    for key, value in tagsjson.items():
        transformed_json[key] = value.split(", ") if isinstance(value, str) else [value]
    return transformed_json


def insert_tagsjson_readable_array(data):
    """Вставляет данные в TagsJSONReadableArray"""
    query = '''
        INSERT INTO backup_schema."TagsJSONReadableArray" ("CardId", "TagsJsonReadableArray")
        VALUES (%s, %s);
    '''
    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            cur.executemany(query, data)
            conn.commit()


def migrate_tagsjson_readable():
    """Основной процесс миграции"""
    data = fetch_tagsjson_readable()

    if not data:
        print("Нет данных для обработки.")
        return
    print("Есть данные")
    transformed_data = [(card_id, json.dumps(transform_tagsjson(tagsjson))) for card_id, tagsjson in data]
    print("Вставляем данные")
    insert_tagsjson_readable_array(transformed_data)
    print(f"Успешно перенесено {len(transformed_data)} записей.")


# Запускаем скрипт
if __name__ == "__main__":
    migrate_tagsjson_readable()
