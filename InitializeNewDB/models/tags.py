import json
import uuid

import psycopg2
import psycopg2.extras

from db.db_connection import get_connection, DB_CONFIG_DEV, DB_CONFIG_ROW


def fetch_tags_from_row(batch_size=100000):
    """
    Получает теги из базы данных ROW постранично (batch processing), сохраняя все пары tag_key: tag_value.
    """
    query = """
    SELECT tags
    FROM card_row
    WHERE tags IS NOT NULL
    LIMIT %s OFFSET %s;
    """

    offset = 0
    extracted_tags = []  # Храним список словарей

    with get_connection(DB_CONFIG_ROW) as conn:
        with conn.cursor() as cursor:
            while True:
                cursor.execute(query, (batch_size, offset))
                rows = cursor.fetchall()

                if not rows:
                    break  # Если больше нет данных, выходим из цикла

                for row in rows:
                    tags_str = row[0]  # JSON-строка с тегами

                    try:
                        tags_dict = json.loads(tags_str)  # Преобразуем строку в словарь
                        tag_entries = [
                            {"TagKey": tag_key, "TagValue": tag_value}
                            for tag_key, tag_value in tags_dict.items() if tag_key != "Артикул"
                        ]
                        extracted_tags.extend(tag_entries)  # Добавляем списком

                    except json.JSONDecodeError:
                        print(f"Ошибка парсинга JSON: {tags_str}")

                offset += batch_size  # Смещаем оффсет для следующей пачки
                print(f"Обработано {offset} записей...")

    print(f"Всего тегов собрано: {len(extracted_tags)}")
    return extracted_tags


def fetch_tags_from_dev(batch_size=100000):
    """
    Получает существующие теги из базы данных DEV постранично.
    """
    query = """
    SELECT "TagKey", "TagValue"
    FROM public."Tags"
    LIMIT %s OFFSET %s;
    """

    offset = 0
    dev_tags = {}

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            while True:
                cursor.execute(query, (batch_size, offset))
                rows = cursor.fetchall()

                if not rows:
                    break

                for tag_key, tag_value in rows:
                    if tag_key == "Артикул":
                        continue
                    dev_tags[tag_key] = tag_value

                offset += batch_size  # Смещаем оффсет


    return dev_tags


def generate_tag_uuid(tag_key, tag_value):
    """
    Генерирует UUID на основе TagKey и TagValue.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{tag_key}|{tag_value}"))


def save_tags_to_json(output_file="tags.json"):
    """
    Получает теги из базы данных ROW постранично, преобразует их и сохраняет в JSON-файл.
    Теперь теги хранятся в формате {"TagKey|TagValue": UUID}.
    """
    row_tags = fetch_tags_from_row()  # [{"TagKey": key, "TagValue": value}, ...]
    dev_tags = fetch_tags_from_dev()  # { "TagKey|TagValue": UUID }

    new_tags = {}

    for tag_entry in row_tags:
        tag_key = tag_entry["TagKey"]
        tag_value = tag_entry["TagValue"]
        tag_combined = f"{tag_key}|{tag_value}"

        if tag_combined not in dev_tags:
            new_tags[tag_combined] = generate_tag_uuid(tag_key, tag_value)

    if not new_tags:
        print("Новых тегов нет, JSON не обновлён.")
        return

    # Сохраняем в JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(new_tags, f, ensure_ascii=False, indent=4)

    print(f"Теги сохранены в JSON файл: {output_file}")

def load_tags_from_json(input_file="tags.json"):
    """
    Загружает JSON-файл с категориями.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Файл {input_file} не найден.")
        return {}


def insert_tags_into_dev(json_file="tags.json", batch_size=1000):
    """
    Вставляет недостающие теги из JSON в базу данных DEV пачками.
    """
    # Загружаем теги из JSON
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            tags_raw = json.load(f)  # tags_raw — это { "TagKey|TagValue": "UUID" }
    except FileNotFoundError:
        print(f"Файл {json_file} не найден.")
        return

    # Преобразуем в список словарей
    tags = [
        {"TagKey": key.split("|")[0], "TagValue": key.split("|")[1], "Id": value}
        for key, value in tags_raw.items()
    ]

    if not tags:
        print("JSON-файл пуст, новых тегов нет.")
        return

    # Получаем текущие теги из базы данных DEV
    existing_tags = fetch_tags_from_dev()
    print(existing_tags)
    existing_tags_set = {(key, value) for key, value in existing_tags.items()}


    # Фильтруем теги, которых ещё нет в DEV
    new_tags = [
        {"TagKey": tag_dict["TagKey"], "TagValue": tag_dict["TagValue"], "Id": tag_dict["Id"]}
        for tag_dict in tags
        if (tag_dict["TagKey"], tag_dict["TagValue"]) not in existing_tags_set
    ]

    if not new_tags:
        print("Все теги уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие теги пачками
    insert_query = 'INSERT INTO public."Tags" ("TagKey", "TagValue", "Id") VALUES %s;'

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(new_tags), batch_size):
                batch = new_tags[i: i + batch_size]
                values = [(tag["TagKey"], tag["TagValue"], tag["Id"]) for tag in batch]

                # Вставляем пачкой
                psycopg2.extras.execute_values(cursor, insert_query, values)
                conn.commit()
                print(f"Добавлено {len(batch)} тегов в DEV.")

    print("Недостающие теги успешно добавлены в базу данных DEV.")


if __name__ == "__main__":
    #save_tags_to_json("../output_json/tags.json")
    insert_tags_into_dev("../output_json/tags.json")