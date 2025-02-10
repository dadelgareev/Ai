import json
import psycopg2
from db.db_config import DB_CONFIG_ROW

def fetch_tag_values():
    """Загружает соответствие OldId -> [NewId, NewId, ...]"""
    query = """
        SELECT "OldId", json_agg("Id") 
        FROM backup_schema."TagValuesNew"
        WHERE "OldId" IS NOT NULL
        GROUP BY "OldId";
    """
    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return {str(row[0]): row[1] for row in cur.fetchall()}  # { "123": [456, 789] }

def fetch_tags_json():
    """Загружает данные из TagsJSON."""
    query = 'SELECT "CardId", "TagsJson" FROM backup_schema."TagsJSON";'
    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()  # [(CardId, {"0": 751, "1": 123, "2": 367}), ...]

def update_tags_json():
    """Обновляет TagsJson, заменяя OldId на список новых значений."""
    tag_values_map = fetch_tag_values()  # Загружаем OldId -> [NewId, NewId]
    tags_json_data = fetch_tags_json()  # Загружаем TagsJSON

    new_tags_json = []
    for card_id, tags_json in tags_json_data:

        if "1" in tags_json:  # Проверяем, есть ли у карточки цвет
            old_id = str(tags_json["1"])  # Берем старый Id цвета
            if old_id in tag_values_map:
                tags_json["1"] = tag_values_map[old_id]  # Заменяем на массив новых Id

        new_tags_json.append((card_id, json.dumps(tags_json)))  # Готовим для вставки

    insert_tags_json(new_tags_json)
    print(f"✅ Успешно обновлено {len(new_tags_json)} записей в TagsJSONNew.")

def insert_tags_json(data):
    """Вставляет обновленные данные в TagsJSONNew."""
    query = '''
        INSERT INTO backup_schema."TagsJSONNew" ("CardId", "TagsJson")
        VALUES (%s, %s);
    '''
    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            cur.executemany(query, data)
            conn.commit()

# Запуск скрипта
if __name__ == "__main__":
    update_tags_json()
