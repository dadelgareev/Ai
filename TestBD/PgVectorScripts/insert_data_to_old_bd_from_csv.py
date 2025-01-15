import csv
import json

import psycopg2
import uuid
from psycopg2.extensions import adapt
from psycopg2.extras import Json
import os

connection_params_origin = {
    'host': '193.232.55.5',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}

def get_csv_list():
    files = os.listdir(os.getcwd())
    for file in files[:]:
        if not file.endswith('.csv'):
            files.remove(file)
    return files

def get_rows_from_csvs(csv_list, batch_size=1000):
    buffer = []  # Буфер для накопления строк
    for csv_file in csv_list:
        with open(csv_file, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                buffer.append(row)  # Добавляем строку в буфер
                if len(buffer) >= batch_size:
                    yield buffer  # Возвращаем партию строк
                    buffer = []  # Очищаем буфер
    if buffer:  # Если остались строки после итерации
        yield buffer


def create_card_row_table(conn_params):
    """
    Создает таблицу card_row в базе данных.
    :param conn_params: Параметры подключения к базе данных.
    """
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS card_row (
        guid UUID,
        source_csv TEXT,
        source TEXT,
        image_url TEXT,
        main_photo BOOLEAN,  -- Используем тип BOOLEAN
        article TEXT,
        guid_list TEXT[],    -- Массив строк
        embedding VECTOR(1000),  -- Используем pgvector для векторов
        price_rub NUMERIC,
        brand TEXT,
        category TEXT,
        subcategory TEXT,
        gender TEXT,
        title TEXT,
        tags TEXT
    );
    """
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        # Убедимся, что pgvector расширение включено
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        cur.execute(sql_create_table)
        conn.commit()  # Фиксируем создание таблицы
        print("Таблица card_row успешно создана или уже существует.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при создании таблицы: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()

    
def insert_data_to_old_bd_from_csv(csv_list, conn_params):
    """
    Вставляет данные из списка CSV-файлов в таблицу card_row.
    :param csv_list: Список путей к CSV-файлам.
    :param conn_params: Параметры подключения к базе данных.
    """
    sql_query_insert = """
    INSERT INTO card_row (guid, source_csv, source, image_url, main_photo, article, guid_list, embedding, price_rub, brand, category, subcategory, gender, title, tags)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        for row in get_rows_from_csvs(csv_list):
            try:
                # Преобразуем данные из row в порядок, соответствующий SQL-запросу
                data = (
                    row.get('guid'),
                    row.get('source_csv'),
                    row.get('source'),
                    row.get('image_url'),
                    row.get('main_photo') == 'TRUE',  # Преобразование в BOOLEAN
                    row.get('article'),
                    row.get('guid_list', '{}'),  # Если пусто, то пустой массив
                    list(map(float, row['embedding'][1:-1].split(','))) if row.get('embedding') else None,  # Преобразование строки в список чисел для VECTOR
                    row.get('price_rub'),
                    row.get('brand'),
                    row.get('category'),
                    row.get('subcategory'),
                    row.get('gender'),
                    row.get('title'),
                    row.get('tags')
                )
                cur.execute(sql_query_insert, data)
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"Ошибка при вставке строки {row}: {error}")
                continue  # Пропустить строку с ошибкой

        conn.commit()  # Фиксируем транзакцию
        print("Данные успешно вставлены.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка подключения к базе данных: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()
if __name__ == '__main__':
    files = get_csv_list()
