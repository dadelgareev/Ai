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


def fetch_genders_from_dev():
    """
    Получает гендеры из базы данных DEV.
    """
    query = "SELECT id, gender_name FROM ecommerce.genders;"
    return fetch_data_from_db(DB_CONFIG_DEV, query, key_column_index=1, value_column_index=0)


def fetch_genders_from_row():
    """
    Получает уникальные гендеры из базы данных ROW.
    """
    query = "SELECT DISTINCT gender FROM card_row WHERE gender IS NOT NULL;"
    return fetch_data_from_db(DB_CONFIG_ROW, query)


def generate_gender_json(output_file="genders.json"):
    """
    Создаёт JSON-файл, содержащий {id: gender_name}, объединяя данные из DEV и ROW.
    """
    dev_genders = fetch_genders_from_dev()
    row_genders = fetch_genders_from_row()

    # Находим гендеры из ROW, которых нет в DEV
    new_genders = row_genders - set(dev_genders.keys())

    # Создаём итоговый JSON
    result_json = dev_genders.copy()

    # Добавляем новые гендеры с временными id, начиная с max id + 1
    current_max_id = max(dev_genders.values(), default=0)
    for index, gender in enumerate(new_genders, start=current_max_id + 1):
        result_json[gender] = index

    # Сохраняем JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=4)
        print(f"JSON сохранён в файл {output_file}")


def load_gender_json(input_file="genders.json"):
    """
    Загружает JSON-файл с гендерами.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Файл {input_file} не найден.")
        return {}


def insert_genders_into_dev(json_file="genders.json"):
    """
    Вставляет недостающие гендеры из JSON в базу данных DEV.
    """
    # Загружаем данные из JSON
    genders = load_gender_json(json_file)
    if not genders:
        print("JSON-файл пуст или отсутствует.")
        return

    # Получаем текущие гендеры из базы данных DEV
    existing_genders = fetch_genders_from_dev()

    # Вычисляем гендеры для добавления
    genders_to_add = {gender: id_ for gender, id_ in genders.items() if gender not in existing_genders}

    if not genders_to_add:
        print("Все гендеры уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие гендеры
    insert_query = "INSERT INTO ecommerce.genders (gender_name) VALUES (%s) RETURNING id;"
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for gender in genders_to_add.keys():
                cursor.execute(insert_query, (gender,))
                new_id = cursor.fetchone()[0]
                print(f"Добавлен гендер: {gender}, ID: {new_id}")
            conn.commit()
        print("Недостающие гендеры успешно добавлены в базу данных DEV.")

if __name__ == "__main__":
    # Проверяем, существует ли папка output_json
    os.makedirs("../output_json", exist_ok=True)  # Создаёт папку, если её нет


    print("Генерация JSON с гендерами...")
    generate_gender_json("../output_json/genders.json")
    print("JSON создан.")


    print("Добавление недостающих гендеров в базу данных DEV...")
    insert_genders_into_dev("../output_json/genders.json")
    print("Процесс завершён.")