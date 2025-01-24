import json
import os

from db.db_connection import get_connection, DB_CONFIG_DEV, DB_CONFIG_ROW

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

def fetch_categories_from_dev():
    """
    Получает категории из базы данных DEV.
    """
    query = "SELECT id, category_name FROM ecommerce.categories;"
    return fetch_data_from_db(DB_CONFIG_DEV, query, key_column_index=1, value_column_index=0)

def parse_categories_constants_json(input_file="categories_constant.json"):
    with open(input_file, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)

        result = {}
        for index, category in enumerate(data.keys(), start=1):
            result[index] = category

        print(result)
        return result


def insert_categories_constant_into_dev(json_file="categories.json"):
    """
    Вставляет недостающие категории из JSON в базу данных DEV.
    """
    # Загружаем данные из JSON
    categories = parse_categories_constants_json(json_file)
    if not categories:
        print("JSON-файл пуст или отсутствует.")
        return

    # Получаем текущие категории из базы данных DEV
    existing_categories = fetch_categories_from_dev()

    # Вычисляем категории для добавления
    categories_to_add = {id_: category for id_, category in categories.items() if
                         category not in existing_categories}

    if not categories_to_add:
        print("Все категории уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие категории
    insert_query = "INSERT INTO ecommerce.categories (id, category_name) VALUES (%s,%s) RETURNING id;"
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for id_, category in categories_to_add.items():
                cursor.execute(insert_query, (id_, category,))
                new_id = cursor.fetchone()[0]
                print(f"Добавлена категория: {category}, ID: {new_id}")
            conn.commit()
        print("Недостающие категории успешно добавлены в базу данных DEV.")

if __name__ == "__main__":
    # Проверяем, существует ли папка output_json
    os.makedirs("../source", exist_ok=True)  # Создаёт папку, если её нет

    print("Добавление недостающих категорий в базу данных DEV...")
    insert_categories_constant_into_dev("../source/categories_constant.json")
    print("Процесс завершён.")
