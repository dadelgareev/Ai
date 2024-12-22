import csv

import psycopg2
from psycopg2.extras import Json
import os

connection_params = {
    'host': '193.232.55.5',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}


def create_products_all_table():
    """
    Создает таблицу products_all в PostgreSQL с использованием pgvector для Embedding.
    """

    create_table_query = """
    CREATE TABLE IF NOT EXISTS products_all (
        id SERIAL PRIMARY KEY,
        Source TEXT,
        Source_csv TEXT,
        Image_url TEXT,
        main_photo BOOLEAN,
        article TEXT,
        Gender TEXT,
        Category TEXT,
        Subcategory TEXT,
        Embedding VECTOR(1000),
        Price FLOAT,
        Brand TEXT,
        Tags JSON
    );
    """

    try:
        # Подключение к базе данных
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # Установка расширения pgvector, если оно еще не установлено
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

                # Создание таблицы
                cur.execute(create_table_query)
                conn.commit()
                print("Таблица products_all успешно создана.")

    except psycopg2.Error as e:
        print(f"Ошибка при работе с базой данных: {e}")
    finally:
        if conn:
            conn.close()


def find_temp_files(directory):
    """
    Находит все файлы с припиской '_temp' в указанной директории и ее поддиректориях.

    Args:
        directory (str): Путь к директории для поиска файлов.

    Returns:
        list: Список имен файлов с припиской '_temp'.
    """
    temp_files = []

    for root, _, files in os.walk(directory):
        for file in files:
            if "_temp" in file and file.endswith(".csv"):  # Проверяем приписку и расширение
                temp_files.append(os.path.join(root, file))  # Полный путь к файлу

    return temp_files


def insert_csv_rows_with_processing(table_name, csv_file_path):
    """
    Читает данные из CSV-файла, обрабатывает строки и добавляет их в таблицу PostgreSQL.

    Args:
        table_name (str): Имя таблицы для вставки данных.
        csv_file_path (str): Путь к CSV-файлу.

    Returns:
        None
    """

    try:
        # Подключение к базе данных
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor() as cur:

                with open(csv_file_path, mode='r', encoding='utf-8') as file:
                    reader = csv.DictReader(file)

                    # Формируем SQL-запрос для вставки данных
                    insert_query = f"""
                        INSERT INTO {table_name} 
                        (Source, Source_csv, Image_url, main_photo, article, Gender, Category, Subcategory, Embedding, Price, Brand, Tags) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    for i, row in enumerate(reader):
                        # Обработка строки
                        embedding = row["Embedding"].strip()
                        # Добавляем квадратные скобки, если их нет
                        if not embedding.startswith("[") and not embedding.endswith("]"):
                            embedding = f"[{embedding}]"

                        processed_row = (
                            row["Source"],  # Source
                            row["Source_csv"],  # Source_csv
                            row["Image_url"],  # Image_url
                            row["main_photo"].lower() == "true",  # main_photo (bool)
                            row["Id"],  # article
                            row["Gender"],  # Gender
                            row["Category"],  # Category
                            row["Subcategory"],  # Subcategory
                            embedding,  # Embedding (строка с квадратными скобками)
                            float(row["Price"].replace(" ", "")) if row["Price"] else None,  # Price
                            row["Brand"],  # Brand
                            row["Tags"]  # Tags (строка в формате JSON)
                        )
                        print(f"{i} - загружена")
                        # Вставка строки в таблицу
                        cur.execute(insert_query, processed_row)

                # Фиксируем изменения
                conn.commit()
                print(f"Данные из файла '{csv_file_path}' успешно вставлены в таблицу '{table_name}'.")
    except psycopg2.Error as e:
        print(f"Ошибка при работе с базой данных: {e}")


def connect_to_db(connection_params):
    """
    Функция для подключения к базе данных и возвращения курсора.
    """
    conn = psycopg2.connect(**connection_params)
    return conn, conn.cursor()


def show_products():
    # Подключение к первой базе данных (источник)
    conn_source, cursor_source = connect_to_db(connection_params)

    select_query = "SELECT * FROM products_all"

    cursor_source.execute(select_query)
    rows = cursor_source.fetchall()

    print(rows)


def read_csv(csv_file_path, table_name):
    # Открываем CSV-файл и читаем данные
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        # Формируем SQL-запрос для вставки данных
        insert_query = f"""
                    INSERT INTO {table_name} 
                    (Source, Source_csv, Image_url, main_photo, article, Gender, Category, Subcategory, Embedding, Price, Brand, Tags) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

        for row in reader:
            # Обработка строки
            processed_row = (
                row["Source"],  # Source
                row["Source_csv"],  # Source_csv
                row["Image_url"],  # Image_url
                row["main_photo"].lower() == "true",  # main_photo (bool)
                row["Id"],  # article
                row["Gender"],  # Gender
                row["Category"],  # Category
                row["Subcategory"],  # Subcategory
                row["Embedding"],  # Embedding (строка)
                float(row["Price"].replace(" ", "")) if row["Price"] else None,  # Price
                row["Brand"],  # Brand
                row["Tags"]  # Tags (строка в формате JSON)
            )
            print(processed_row)


def similar_vector(target_id):
    # Подключение к базе данных
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    # Запрос
    query = """
    SELECT 
        p2.id AS similar_id, 
        1 - (p1.embedding <=> p2.embedding) AS similarity
    FROM 
        products_all p1, 
        products_all p2
    WHERE 
        p1.id = %s
        AND p1.id != p2.id
        AND (1 - (p1.embedding <=> p2.embedding)) >= 0.9
    ORDER BY 
        similarity DESC
    LIMIT 1;
    """

    # Выполнение
    cursor.execute(query, (target_id,))
    result = cursor.fetchall()

    # Вывод результата
    if result:
        print(f"Наиболее похожий вектор для ID {target_id}: {result[0]}")
    else:
        print(f"Нет векторов с похожестью >= 0.9 для ID {target_id}")

    # Закрытие соединения
    cursor.close()
    conn.close()


def show_product_id(id):
    # Подключение к первой базе данных (источник)
    conn_source, cursor_source = connect_to_db(connection_params)

    select_query = "SELECT * FROM products_all where id = %s"

    cursor_source.execute(select_query, (id,))

    rows = cursor_source.fetchall()

    print(rows)


def create_user_preferences_table(table_name):
    """
    Создает таблицу для хранения пользовательских данных и предпочтений с векторами размерности 1000.

    Args:
        db_config (dict): Конфигурация подключения к базе данных (host, dbname, user, password).
        table_name (str): Имя таблицы для создания.

    Returns:
        None
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        user_id SERIAL PRIMARY KEY,
        preferences_embedding_1 VECTOR(1000),
        preferences_embedding_2 VECTOR(1000),
        preferences_embedding_3 VECTOR(1000),
        preferences_embedding_4 VECTOR(1000),
        preferences_embedding_5 VECTOR(1000),
        preferences_embedding_6 VECTOR(1000),
        preferences_embedding_7 VECTOR(1000),
        preferences_embedding_8 VECTOR(1000),
        preferences_embedding_9 VECTOR(1000),
        preferences_embedding_10 VECTOR(1000),
        preferences_embedding_11 VECTOR(1000),
        preferences_embedding_12 VECTOR(1000),
        preferences_embedding_13 VECTOR(1000),
        preferences_embedding_14 VECTOR(1000),
        preferences_embedding_15 VECTOR(1000),
        preferences_embedding_16 VECTOR(1000),
        preferences_embedding_17 VECTOR(1000),
        preferences_embedding_18 VECTOR(1000),
        preferences_embedding_19 VECTOR(1000),
        preferences_embedding_20 VECTOR(1000),
        preferences_embedding_21 VECTOR(1000),
        preferences_embedding_22 VECTOR(1000),
        preferences_embedding_23 VECTOR(1000),
        preferences_embedding_24 VECTOR(1000),
        preferences_embedding_25 VECTOR(1000),
        preferences_embedding_26 VECTOR(1000),
        preferences_embedding_27 VECTOR(1000),
        preferences_embedding_28 VECTOR(1000),
        preferences_embedding_29 VECTOR(1000),
        preferences_embedding_30 VECTOR(1000),
        preferences_embedding_31 VECTOR(1000),
        preferences_embedding_32 VECTOR(1000),

         -- Вектор предпочтений
        gradient_embedding_1 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_2 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_3 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_4 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_5 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_6 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_7 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_8 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_9 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_10 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_11 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_12 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_13 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_14 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_15 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_16 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_17 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_18 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_19 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_20 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_21 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_22 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_23 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_24 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_25 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_26 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_27 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_28 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_29 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_30 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_31 VECTOR(1000), -- Градиентный вектор
        gradient_embedding_32 VECTOR(1000), -- Градиентный вектор

        old_preferences_embedding_1 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_2 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_3 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_4 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_5 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_6 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_7 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_8 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_9 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_10 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_11 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_12 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_13 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_14 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_15 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_16 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_17 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_18 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_19 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_20 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_21 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_22 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_23 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_24 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_25 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_26 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_27 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_28 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_29 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_30 VECTOR(1000), -- Старый вектор предпочтений
        old_preferences_embedding_31 VECTOR(1000),-- Старый вектор предпочтений
        old_preferences_embedding_32 VECTOR(1000) -- Старый вектор предпочтений
    );
    """
    try:
        # Подключение к базе данных
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # Выполняем запрос на создание таблицы
                cur.execute(create_table_query)
                conn.commit()
                print(f"Таблица '{table_name}' успешно создана.")
    except psycopg2.Error as e:
        print(f"Ошибка при создании таблицы '{table_name}': {e}")

def describe_table(table_name):
    """
    Получает описание структуры таблицы PostgreSQL.

    Args:
        table_name (str): Имя таблицы, описание которой нужно получить.

    Returns:
        list: Список с описанием столбцов таблицы.
    """
    describe_query = """
    SELECT 
        column_name, 
        data_type, 
        is_nullable, 
        character_maximum_length
    FROM 
        information_schema.columns
    WHERE 
        table_name = %s
    ORDER BY 
        ordinal_position;
    """
    try:
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # Выполняем запрос для описания таблицы
                cur.execute(describe_query, (table_name,))
                columns = cur.fetchall()

                # Формируем удобное представление
                description = []
                for col in columns:
                    column_description = {
                        'column_name': col[0],
                        'data_type': col[1],
                        'is_nullable': col[2],
                        'character_maximum_length': col[3]
                    }
                    description.append(column_description)

                print(f"Описание таблицы '{table_name}':")
                for col in description:
                    print(
                        f"  - {col['column_name']} ({col['data_type']}), Nullable: {col['is_nullable']}, "
                        f"Max Length: {col['character_maximum_length']}"
                    )
                return description
    except psycopg2.Error as e:
        print(f"Ошибка при описании таблицы '{table_name}': {e}")
        return None

describe_table("user_info")
describe_table("products_all")



# create_user_preferences_table("user_info")

# Пример использования
directory_path = "C:\\Users\\WorkCloudDressy\\PycharmProjects\\TestPgVectorIlya"
temp_file_list = find_temp_files(directory_path)
print(len(temp_file_list))
# insert_csv_rows_with_processing("products_all","Lamoda-Man-shoes-sliponyespadrilimuj_temp.csv")
# read_csv("Lamoda-Woman-clothes-d-insy_temp.csv", "all_products")
similar_vector(7548)
show_product_id(7548)
show_product_id(6400)

# show_products()