import json
import os
import uuid

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
    query = 'SELECT "Name","CategoryId" FROM public."SubCategory";'
    return fetch_data_from_db(DB_CONFIG_DEV, query, key_column_index=0, value_column_index=1)


def fetch_subcategories_from_row():

    """
    Получает уникальные подкатегории из базы данных ROW.
    """
    query = """
    SELECT DISTINCT subcategory, category
    FROM card_row
    WHERE category IS NOT NULL AND subcategory IS NOT NULL
      AND category != 'Не указано' AND subcategory != 'Не указано';
    """
    return fetch_data_from_db(DB_CONFIG_ROW, query, key_column_index=0, value_column_index=1)


def generate_subcategory_json(output_file="subcategories.json", category_file="../output_json/categories.json"):
    """
    Создаёт JSON-файл, содержащий {"category_id|subcategory_name": uuid}, объединяя данные из ROW и категорий.
    """
    # Получаем подкатегории и категории из ROW
    row_subcategories = fetch_subcategories_from_row()

    # Загружаем категории из JSON
    with open(category_file, 'r', encoding="utf-8") as json_file:
        category_data = json.load(json_file)

    # Заменяем категории в row_subcategories на их UUID из category_data
    for key, value in row_subcategories.items():
        if value in category_data:
            row_subcategories[key] = category_data[value]
        else:
            print(f"Категория '{value}' для подкатегории '{key}' не найдена в category_data.")

    # Создаём итоговый JSON
    result_json = {}

    # Генерируем UUID для подкатегорий
    for subcategory_name, category_uuid in row_subcategories.items():
        subcategory_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, subcategory_name))  # Детерминированный UUID
        category_subcategory_key = f"{subcategory_name}|{category_uuid}"  # Используем строку как ключ
        result_json[category_subcategory_key] = subcategory_uuid

    # Сохраняем JSON в файл
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

    # Преобразуем в структуру {(subcategory_name, category_uuid): uuid} для проверки
    existing_subcategory_keys = {
        (subcategory_name, category_uuid) for subcategory_name, category_uuid in existing_subcategories.items()
    }

    # Вычисляем подкатегории для добавления
    subcategories_to_add = {
        tuple(key.split("|")): uuid  # Разделяем ключ обратно на subcategory_name и category_uuid
        for key, uuid in subcategories.items()
        if tuple(key.split("|")) not in existing_subcategory_keys
    }

    if not subcategories_to_add:
        print("Все подкатегории уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие подкатегории
    insert_query = 'INSERT INTO public."SubCategory" (id, "Name", "CategoryId") VALUES (%s, %s, %s);'
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for (subcategory_name, category_uuid), subcategory_uuid in subcategories_to_add.items():
                # category_uuid теперь используем как внешний ключ для CategoryId
                cursor.execute(insert_query, (subcategory_uuid, subcategory_name, category_uuid))
                print(f"Добавлена подкатегория: {subcategory_name}, ID: {subcategory_uuid}, Category UUID: {category_uuid}")
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
