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


def fetch_categories_from_row():
    """
    Получает уникальные категории из базы данных ROW.
    """
    query = "SELECT DISTINCT category FROM card_row WHERE category IS NOT NULL;"
    return fetch_data_from_db(DB_CONFIG_ROW, query)


def generate_category_json(output_file="categories.json"):
    """
    Создаёт JSON-файл, содержащий {id: category_name}, объединяя данные из DEV и ROW.
    """
    dev_categories = fetch_categories_from_dev()
    row_categories = fetch_categories_from_row()

    # Находим категории из ROW, которых нет в DEV
    new_categories = row_categories - set(dev_categories.keys())

    # Создаём итоговый JSON
    result_json = dev_categories.copy()

    # Добавляем новые категории с временными id, начиная с max id + 1
    current_max_id = max(dev_categories.values(), default=0)
    for index, category in enumerate(new_categories, start=current_max_id + 1):
        result_json[category] = index

    # Сохраняем JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=4)
        print(f"JSON сохранён в файл {output_file}")


def load_category_json(input_file="categories.json"):
    """
    Загружает JSON-файл с категориями.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Файл {input_file} не найден.")
        return {}


def insert_categories_into_dev(json_file="categories.json"):
    """
    Вставляет недостающие категории из JSON в базу данных DEV.
    """
    # Загружаем данные из JSON
    categories = load_category_json(json_file)
    if not categories:
        print("JSON-файл пуст или отсутствует.")
        return

    # Получаем текущие категории из базы данных DEV
    existing_categories = fetch_categories_from_dev()

    # Вычисляем категории для добавления
    categories_to_add = {category: id_ for category, id_ in categories.items() if category not in existing_categories}

    if not categories_to_add:
        print("Все категории уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие категории
    insert_query = "INSERT INTO ecommerce.categories (category_name) VALUES (%s) RETURNING id;"
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for category in categories_to_add.keys():
                cursor.execute(insert_query, (category,))
                new_id = cursor.fetchone()[0]
                print(f"Добавлена категория: {category}, ID: {new_id}")
            conn.commit()
        print("Недостающие категории успешно добавлены в базу данных DEV.")

if __name__ == "__main__":
    # Проверяем, существует ли папка output_json
    os.makedirs("../output_json", exist_ok=True)  # Создаёт папку, если её нет


    print("Генерация JSON с категориями...")
    generate_category_json("../output_json/categories.json")
    print("JSON создан.")


    print("Добавление недостающих категорий в базу данных DEV...")
    insert_categories_into_dev("../output_json/categories.json")
    print("Процесс завершён.")
