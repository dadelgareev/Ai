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

def fetch_subcategories_from_dev():
    """
    Получает подкатегории из базы данных DEV.
    """
    query = "SELECT id, subcategory_name FROM ecommerce.subcategories;"
    return fetch_data_from_db(DB_CONFIG_DEV, query, key_column_index=1, value_column_index=0)


def fetch_subcategories_from_row():
    """
    Получает уникальные подкатегории из базы данных ROW.
    """
    query = "SELECT DISTINCT subcategory FROM card_row WHERE subcategory IS NOT NULL;"
    return fetch_data_from_db(DB_CONFIG_ROW, query)


def generate_subcategory_json(output_file="subcategories.json"):
    """
    Создаёт JSON-файл, содержащий {id: subcategory_name}, объединяя данные из DEV и ROW.
    """
    dev_subcategories = fetch_subcategories_from_dev()
    row_subcategories = fetch_subcategories_from_row()

    # Находим подкатегории из ROW, которых нет в DEV
    new_subcategories = row_subcategories - set(dev_subcategories.keys())

    # Создаём итоговый JSON
    result_json = dev_subcategories.copy()

    # Добавляем новые подкатегории с временными id, начиная с max id + 1
    current_max_id = max(dev_subcategories.values(), default=0)
    for index, subcategory in enumerate(new_subcategories, start=current_max_id + 1):
        result_json[subcategory] = index

    # Сохраняем JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=4)
        print(f"JSON сохранён в файл {output_file}")


def load_subcategory_json(input_file="subcategories.json"):
    """
    Загружает JSON-файл с подкатегориями.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Файл {input_file} не найден.")
        return {}


def insert_subcategories_into_dev(json_file="subcategories.json"):
    """
    Вставляет недостающие подкатегории из JSON в базу данных DEV.
    """
    # Загружаем данные из JSON
    subcategories = load_subcategory_json(json_file)
    if not subcategories:
        print("JSON-файл пуст или отсутствует.")
        return

    # Получаем текущие подкатегории из базы данных DEV
    existing_subcategories = fetch_subcategories_from_dev()

    # Вычисляем подкатегории для добавления
    subcategories_to_add = {subcategory: id_ for subcategory, id_ in subcategories.items() if subcategory not in existing_subcategories}

    if not subcategories_to_add:
        print("Все подкатегории уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие подкатегории
    insert_query = "INSERT INTO ecommerce.subcategories (subcategory_name) VALUES (%s) RETURNING id;"
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for subcategory in subcategories_to_add.keys():
                cursor.execute(insert_query, (subcategory,))
                new_id = cursor.fetchone()[0]
                print(f"Добавлена подкатегория: {subcategory}, ID: {new_id}")
            conn.commit()
        print("Недостающие подкатегории успешно добавлены в базу данных DEV.")

if __name__ == "__main__":
    # Проверяем, существует ли папка output_json
    os.makedirs("../output_json", exist_ok=True)  # Создаёт папку, если её нет


    print("Генерация JSON с подкатегориями...")
    generate_subcategory_json("../output_json/subcategories.json")
    print("JSON создан.")


    print("Добавление недостающих подкатегорий в базу данных DEV...")
    insert_subcategories_into_dev("../output_json/subcategories.json")
    print("Процесс завершён.")
