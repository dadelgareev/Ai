import json
import psycopg2
from db.db_config import DB_CONFIG_ROW, DB_CONFIG_ADEL


BATCH_SIZE = 1000  # Размер одной пачки


def fetch_tagsjson():
    """Извлекает данные из public.TagsJSON в базе ADEL."""
    query = 'SELECT "CardId", "TagsJson" FROM public."TagsJSON";'

    with psycopg2.connect(**DB_CONFIG_ADEL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()  # [(CardId, TagsJson), ...]


def insert_tagsjson(data):
    """Вставляет данные в backup_schema.TagsJSON в базе ROW пачками."""
    query = '''
        INSERT INTO backup_schema."TagsJSON" ("CardId", "TagsJson")
        VALUES (%s, %s);
    '''

    with psycopg2.connect(**DB_CONFIG_ROW) as conn:
        with conn.cursor() as cur:
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i : i + BATCH_SIZE]  # Берем порцию записей
                cur.executemany(query, batch)
                conn.commit()
                print(f"Вставлено {len(batch)} записей...")


def migrate_tagsjson():
    """Основной процесс миграции данных."""
    data = fetch_tagsjson()

    if not data:
        print("Нет данных для переноса.")
        return

    # Преобразуем JSONB в строку перед вставкой
    formatted_data = [(card_id, json.dumps(tags_json)) for card_id, tags_json in data]
    print("Дошли")
    insert_tagsjson(formatted_data)
    print(f"✅ Успешно перенесено {len(formatted_data)} записей.")


# Запускаем миграцию
if __name__ == "__main__":
    migrate_tagsjson()
