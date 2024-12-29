import csv
import os

import psycopg2

# Глобальные параметры подключения
conn_params = {
    'host': 'localhost',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}

def create_products_lamoda_table():
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Включаем расширение pgvector
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

        # Создаём таблицу products_lamoda без поля id
        cur.execute("""
        CREATE TABLE IF NOT EXISTS products_tsum (
            source TEXT,                            -- Источник данных
            source_csv TEXT,                        -- Исходный CSV-файл
            guid TEXT,                              -- GUID (не уникальный)
            image_url TEXT,                         -- URL изображения
            main_photo TEXT,                        -- Флаг основного фото
            guid_list TEXT[], 
            id TEXT,                      -- Список GUID'ов
            gender TEXT,                            -- Пол
            category TEXT,                          -- Категория
            subcategory TEXT,                       -- Подкатегория
            embedding VECTOR(1000),                 -- Вектор размером 1000 (pgvector)
            price NUMERIC(10, 2),                   -- Цена (число с двумя знаками после запятой)
            brand TEXT,                             -- Бренд
            tags TEXT[]                             -- Теги (массив строк)
        );
        """)

        # Создаём индекс для поиска по вектору
        #cur.execute("CREATE INDEX IF NOT EXISTS idx_embedding ON products_lamoda USING ivfflat (embedding vector_cosine_ops);")

        # Сохраняем изменения
        conn.commit()
        print("Таблица products_lamoda успешно создана!")
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        # Закрываем соединение с базой данных
        if conn:
            cur.close()
            conn.close()

def insert_data_from_csv(csv_file):
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Открываем CSV-файл
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader)  # Пропускаем заголовок
            print(header)  # Проверить заголовок

            # Проходим по строкам CSV-файла
            for row in reader:

                # Подготовка данных для вставки
                source = row[0]
                source_csv = row[1]
                guid = row[2]
                image_url = row[3]
                main_photo = row[4]
                guid_list = row[5].split('|')
                id_product = row[6]
                gender = row[7]
                category = row[8]
                subcategory = row[9]
                embedding = [float(x) for x in row[10].split(',')]  # Преобразуем вектор в список float
                price = float(row[11])
                brand = row[12]
                tags = row[13].split('|')              # Разделяем теги в массив



                # SQL-запрос для вставки данных
                cur.execute("""
                    INSERT INTO products_tsum (
                        source, source_csv, guid, image_url, main_photo,
                        guid_list,id ,gender, category, subcategory,
                        embedding, price, brand, tags
                    ) VALUES (
                        %s, %s, %s, %s, %s,%s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """, (
                    source, source_csv, guid, image_url, main_photo,
                    guid_list, id_product,gender, category, subcategory,
                    embedding, price, brand, tags
                ))

        # Фиксируем изменения
        conn.commit()
        print("Данные успешно загружены в таблицу products_lamoda!", csv_file)

    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        # Закрываем соединение с базой данных
        if conn:
            cur.close()
            conn.close()

def get_all_csv():
    list_files = os.listdir()
    csv_files = []
    for file in list_files:
        if file.endswith("temp.csv"):
            csv_files.append(file)
    return csv_files


def get_unique_source_csv(conn_params, table_name):
    """
    Подключается к базе данных PostgreSQL и возвращает список уникальных значений из поля source_csv.

    Args:
        db_config (dict): Параметры подключения к базе данных (host, dbname, user, password, port).
        table_name (str): Название таблицы.

    Returns:
        list: Список уникальных значений из поля source_csv.
    """
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Выполняем SQL-запрос для получения уникальных значений
        query = f"SELECT DISTINCT source_csv FROM {table_name};"
        cursor.execute(query)

        # Извлекаем результат
        result = [row[0] for row in cursor.fetchall()]

        # Закрываем соединение
        cursor.close()
        conn.close()

        return result

    except Exception as e:
        print(f"Ошибка: {e}")
        return []


def fetch_and_write_csv(conn_params, table_name, source_csv_list, output_dir):
    """
    Делает запрос в базу данных PostgreSQL для каждого значения source_csv
    и записывает результаты в отдельные CSV-файлы.

    Args:
        conn_params (dict): Параметры подключения к базе данных (host, dbname, user, password, port).
        table_name (str): Название таблицы в базе данных.
        source_csv_list (list): Список значений source_csv для фильтрации.
        output_dir (str): Путь к директории, где сохраняются файлы.

    Returns:
        None
    """
    # Определяем поля для CSV
    fieldnames = [
        "Source", "Source_csv", "Guid", "Image_url", "Main_photo", "Guid_list",
        "Id", "Gender", "Category", "Subcategory", "Embedding", "Price", "Brand", "Tags"
    ]

    try:
        # Проверяем и создаём директорию, если её нет
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)  # Создаём папку и все родительские директории при необходимости

        # Подключаемся к базе данных
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                for source_csv_value in source_csv_list:
                    # Выполняем запрос к базе данных
                    query = f"SELECT * FROM {table_name} WHERE source_csv = %s;"
                    cursor.execute(query, (source_csv_value,))
                    rows = cursor.fetchall()

                    # Если записи найдены, записываем их в CSV
                    if rows:
                        output_file = os.path.join(output_dir, f"{source_csv_value}")  # Создаем путь к файлу
                        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file)
                            writer.writerow(fieldnames)  # Записываем заголовки
                            writer.writerows(rows)  # Записываем строки
                        print(f"Файл '{output_file}' успешно создан.")
                    else:
                        print(f"Нет записей для значения source_csv = '{source_csv_value}'.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")

csv_list =(get_unique_source_csv(conn_params,"products_tsum"))
fetch_and_write_csv(conn_params, "products_tsum",csv_list, "output_csv")
#for file in get_all_csv():
    #insert_data_from_csv(file)

# Пример вызова функции
#insert_data_from_csv('Tsum-Man-belye-19949.csv')

# Вызов функции
#create_products_lamoda_table()
