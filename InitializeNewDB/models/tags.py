import json
import uuid
from db.db_connection import get_connection, DB_CONFIG_DEV, DB_CONFIG_ROW


def fetch_tags_from_row():
    """
    Получает уникальные теги из базы данных ROW (таблица card_row).
    """
    query = """
    SELECT DISTINCT tag_key, tag_value
    FROM card_row
    WHERE tag_key IS NOT NULL AND tag_value IS NOT NULL;
    """
    return fetch_data_from_db(DB_CONFIG_ROW, query, key_column_index=0, value_column_index=1)


def fetch_data_from_db(config, query, key_column_index=0, value_column_index=None):
    """
    Универсальная функция для получения данных из базы данных.
    """
    with get_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

            if value_column_index is not None:
                return {row[key_column_index]: row[value_column_index] for row in rows}  # {key: value}
            else:
                return {row[key_column_index] for row in rows}  # {key}


def generate_tag_uuid(tag_key, tag_value):
    """
    Генерирует UUID на основе TagKey и TagValue.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{tag_key}|{tag_value}"))


def save_tags_to_json(output_file="tags.json"):
    """
    Получает теги из базы данных ROW и сохраняет их в JSON файл.
    """
    row_tags = fetch_tags_from_row()

    # Генерация UUID для каждой пары tag_key и tag_value
    tags_with_uuid = {
        f"{tag_key}|{tag_value}": generate_tag_uuid(tag_key, tag_value)
        for tag_key, tag_value in row_tags.items()
    }

    # Сохраняем в JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tags_with_uuid, f, ensure_ascii=False, indent=4)
    print(f"Теги сохранены в JSON файл: {output_file}")


def load_tags_from_json(input_file="tags.json"):
    """
    Загружает теги из JSON-файла.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Файл {input_file} не найден.")
        return {}


def insert_tags_into_dev(json_file="tags.json"):
    """
    Вставляет теги из JSON в таблицу Tags базы данных DEV.
    """
    # Загружаем теги из JSON
    tags = load_tags_from_json(json_file)
    if not tags:
        print("JSON-файл пуст или отсутствует.")
        return

    # Проверим, какие теги уже существуют в таблице Tags
    existing_tags = fetch_data_from_db(DB_CONFIG_DEV, 'SELECT "TagKey", "TagValue" FROM public."Tags";', key_column_index=0, value_column_index=1)

    # Определяем теги, которых нет в базе данных
    tags_to_insert = {
        (tag_key, tag_value): tag_uuid
        for (tag_key, tag_value), tag_uuid in tags.items()
        if (tag_key, tag_value) not in existing_tags
    }

    if not tags_to_insert:
        print("Все теги уже существуют в базе данных.")
        return

    # Вставляем новые теги
    insert_query = 'INSERT INTO public."Tags" ("TagKey", "TagValue", "Id") VALUES (%s, %s, %s);'
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for (tag_key, tag_value), tag_uuid in tags_to_insert.items():
                cursor.execute(insert_query, (tag_key, tag_value, tag_uuid))
                print(f"Добавлен тег: TagKey = {tag_key}, TagValue = {tag_value}, UUID: {tag_uuid}")
            conn.commit()
        print("Теги успешно добавлены в базу данных DEV.")


if __name__ == "__main__":
    # Шаг 1: Получение тегов и сохранение их в JSON
    print("Получение тегов и сохранение их в JSON файл...")
    save_tags_to_json("tags.json")

    # Шаг 2: Вставка тегов в базу данных DEV из JSON
    print("Вставка тегов из JSON в базу данных DEV...")
    insert_tags_into_dev("tags.json")
    print("Процесс завершён.")
