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


def fetch_brands_from_dev():
    """
    Получает бренды из базы данных DEV.
    """
    query = "SELECT id, brand_name FROM ecommerce.brands;"
    return fetch_data_from_db(DB_CONFIG_DEV, query, key_column_index=1, value_column_index=0)


def fetch_brands_from_row():
    """
    Получает уникальные бренды из базы данных ROW.
    """
    query = "SELECT DISTINCT brand FROM card_row WHERE brand IS NOT NULL;"
    return fetch_data_from_db(DB_CONFIG_ROW, query)


def generate_brand_json(output_file="brands.json"):
    """
    Создаёт JSON-файл, содержащий {id: brand_name}, объединяя данные из DEV и ROW.
    """
    dev_brands = fetch_brands_from_dev()
    row_brands = fetch_brands_from_row()

    # Находим бренды из ROW, которых нет в DEV
    new_brands = row_brands - set(dev_brands.keys())

    # Создаём итоговый JSON
    result_json = dev_brands.copy()

    # Добавляем новые бренды с временными id, начиная с max id + 1
    current_max_id = max(dev_brands.values(), default=0)
    for index, brand in enumerate(new_brands, start=current_max_id + 1):
        result_json[brand] = index

    # Сохраняем JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=4)
        print(f"JSON сохранён в файл {output_file}")


def load_brand_json(input_file="brands.json"):
    """
    Загружает JSON-файл с брендами.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Файл {input_file} не найден.")
        return {}


def insert_brands_into_dev(json_file="brands.json"):
    """
    Вставляет недостающие бренды из JSON в базу данных DEV.
    """
    # Загружаем данные из JSON
    brands = load_brand_json(json_file)
    if not brands:
        print("JSON-файл пуст или отсутствует.")
        return

    # Получаем текущие бренды из базы данных DEV
    existing_brands = fetch_brands_from_dev()

    # Вычисляем бренды для добавления
    brands_to_add = {brand: id_ for brand, id_ in brands.items() if brand not in existing_brands}

    if not brands_to_add:
        print("Все бренды уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие бренды
    insert_query = "INSERT INTO ecommerce.brands (brand_name) VALUES (%s) RETURNING id;"
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for brand in brands_to_add.keys():
                cursor.execute(insert_query, (brand,))
                new_id = cursor.fetchone()[0]
                print(f"Добавлен бренд: {brand}, ID: {new_id}")
            conn.commit()
        print("Недостающие бренды успешно добавлены в базу данных DEV.")

if __name__ == "__main__":
    # Проверяем, существует ли папка output_json
    os.makedirs("../output_json", exist_ok=True)  # Создаёт папку, если её нет

    # Генерация JSON с брендами
    print("Генерация JSON с брендами...")
    generate_brand_json("../output_json/brands.json")
    print("JSON создан.")

    # Вставка недостающих брендов в базу данных DEV
    print("Добавление недостающих брендов в базу данных DEV...")
    insert_brands_into_dev("../output_json/brands.json")
    print("Процесс завершён.")