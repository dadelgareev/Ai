import json
import os

from db.db_connection import get_connection, DB_CONFIG_DEV, DB_CONFIG_ROW

def fetch_data_from_db(config, query, key_columns_indices, value_column_index=None):
    """
    Универсальная функция для получения данных из базы данных.

    :param config: Конфигурация подключения к базе данных.
    :param query: SQL-запрос для выполнения.
    :param key_columns_indices: Список индексов колонок для создания ключа (может быть одно значение или несколько).
    :param value_column_index: Индекс колонки для значения (если None, возвращает только ключи).
    :return: Словарь {key: value} или множество ключей {key}.
    """
    with get_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

            if value_column_index is not None:
                return {
                    tuple(row[i] for i in key_columns_indices): row[value_column_index]
                    for row in rows
                }  # {key: value}
            else:
                return {
                    tuple(row[i] for i in key_columns_indices)
                    for row in rows
                }  # {key}

def fetch_category_links_from_dev():
    """
    Получает существующие связи категорий и подкатегорий из базы данных DEV.
    """
    query = "SELECT id, category_id, subcategory_id FROM ecommerce.category_links;"
    return fetch_data_from_db(
        DB_CONFIG_DEV,
        query,
        key_columns_indices=[1, 2],  # Ключ: (category_id, subcategory_id)
        value_column_index=0  # Значение: id
    )


def fetch_category_links_from_row():
    """
    Получает уникальные пары (category, subcategory) из базы данных ROW.
    """
    query = """
    SELECT DISTINCT category, subcategory
    FROM card_row
    WHERE category IS NOT NULL AND subcategory IS NOT NULL
      AND category != 'Не указано' AND subcategory != 'Не указано';
    """
    return fetch_data_from_db(
        DB_CONFIG_ROW,
        query,
        key_columns_indices=[0, 1]  # Ключ: (category, subcategory)
    )


def load_json(file_path):
    """
    Загружает JSON-файл и возвращает данные.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
        return {}


def generate_category_links_json(output_file="category_links.json", categories_file="categories.json", subcategories_file="subcategories.json"):
    """
    Создаёт JSON-файл, содержащий {(category_id, subcategory_id): id}, объединяя данные из DEV и ROW.
    """
    dev_links = fetch_category_links_from_dev()
    row_links = fetch_category_links_from_row()

    # Загружаем категории и подкатегории
    categories = load_json(categories_file)
    subcategories = load_json(subcategories_file)

    # Находим связи из ROW, которых нет в DEV
    new_links = row_links - set(dev_links.keys())

    # Создаём итоговый JSON
    result_json = {str(key): value for key, value in dev_links.items()}

    # Добавляем новые связи с временными id, начиная с max id + 1
    current_max_id = max(dev_links.values(), default=0)
    for index, (category_name, subcategory_name) in enumerate(new_links, start=current_max_id + 1):
        category_id = categories.get(category_name)
        subcategory_id = subcategories.get(subcategory_name)

        # Пропускаем связи, где id не найдено
        if category_id is None or subcategory_id is None:
            print(f"Пропущена связь: (category={category_name}, subcategory={subcategory_name}), id не найден.")
            continue

        result_json[str((category_id, subcategory_id))] = index

    # Сохраняем JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=4)
        print(f"JSON сохранён в файл {output_file}")


def insert_category_links_into_dev(json_file="category_links.json"):
    """
    Вставляет недостающие связи категорий и подкатегорий из JSON в базу данных DEV.
    """
    # Загружаем данные из JSON
    category_links = load_json(json_file)
    if not category_links:
        print("JSON-файл пуст или отсутствует.")
        return

    # Получаем текущие связи из базы данных DEV
    existing_links = fetch_category_links_from_dev()

    # Вычисляем связи для добавления
    links_to_add = {eval(key): id_ for key, id_ in category_links.items() if eval(key) not in existing_links}

    if not links_to_add:
        print("Все связи уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие связи
    insert_query = """
    INSERT INTO ecommerce.category_links (category_id, subcategory_id)
    VALUES (%s, %s) RETURNING id;
    """
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for (category_id, subcategory_id) in links_to_add.keys():
                cursor.execute(insert_query, (category_id, subcategory_id))
                new_id = cursor.fetchone()[0]
                print(f"Добавлена связь: (category_id={category_id}, subcategory_id={subcategory_id}), ID: {new_id}")
            conn.commit()
        print("Недостающие связи успешно добавлены в базу данных DEV.")

if __name__ == "__main__":
    # Проверяем, существует ли папка output_json
    os.makedirs("../output_json", exist_ok=True)  # Создаёт папку, если её нет


    print("Генерация JSON с связей категорий...")
    generate_category_links_json("../output_json/category_links.json","../output_json/categories.json","../output_json/subcategories.json")
    print("JSON создан.")


    print("Добавление недостающих связей категорий в базу данных DEV...")
    insert_category_links_into_dev("../output_json/category_links.json")
    print("Процесс завершён.")