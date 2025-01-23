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

def fetch_sources_from_dev():
    """
    Получает источники из базы данных DEV.
    """
    query = "SELECT id, source_name FROM ecommerce.sources;"
    return fetch_data_from_db(DB_CONFIG_DEV, query, key_column_index=1, value_column_index=0)


def fetch_sources_from_row():
    """
    Получает уникальные источники из базы данных ROW.
    """
    query = "SELECT DISTINCT source FROM card_row WHERE source IS NOT NULL;"
    return fetch_data_from_db(DB_CONFIG_ROW, query)


def generate_source_json(output_file="sources.json"):
    """
    Создаёт JSON-файл, содержащий {id: source_name}, объединяя данные из DEV и ROW.
    """
    dev_sources = fetch_sources_from_dev()
    row_sources = fetch_sources_from_row()

    # Находим источники из ROW, которых нет в DEV
    new_sources = row_sources - set(dev_sources.keys())

    # Создаём итоговый JSON
    result_json = dev_sources.copy()

    # Добавляем новые источники с временными id, начиная с max id + 1
    current_max_id = max(dev_sources.values(), default=0)
    for index, source in enumerate(new_sources, start=current_max_id + 1):
        result_json[source] = index

    # Сохраняем JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=4)
        print(f"JSON сохранён в файл {output_file}")


def load_source_json(input_file="sources.json"):
    """
    Загружает JSON-файл с источниками.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Файл {input_file} не найден.")
        return {}


def insert_sources_into_dev(json_file="sources.json"):
    """
    Вставляет недостающие источники из JSON в базу данных DEV.
    """
    # Загружаем данные из JSON
    sources = load_source_json(json_file)
    if not sources:
        print("JSON-файл пуст или отсутствует.")
        return

    # Получаем текущие источники из базы данных DEV
    existing_sources = fetch_sources_from_dev()

    # Вычисляем источники для добавления
    sources_to_add = {source: id_ for source, id_ in sources.items() if source not in existing_sources}

    if not sources_to_add:
        print("Все источники уже существуют в базе данных DEV.")
        return

    # Вставляем недостающие источники
    insert_query = "INSERT INTO ecommerce.sources (source_name) VALUES (%s) RETURNING id;"
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for source in sources_to_add.keys():
                cursor.execute(insert_query, (source,))
                new_id = cursor.fetchone()[0]
                print(f"Добавлен источник: {source}, ID: {new_id}")
            conn.commit()
        print("Недостающие источники успешно добавлены в базу данных DEV.")

if __name__ == "__main__":
    # Проверяем, существует ли папка output_json
    os.makedirs("../output_json", exist_ok=True)  # Создаёт папку, если её нет


    print("Генерация JSON с источниками...")
    generate_source_json("../output_json/sources.json")
    print("JSON создан.")


    print("Добавление недостающих источников в базу данных DEV...")
    insert_sources_into_dev("../output_json/sources.json")
    print("Процесс завершён.")