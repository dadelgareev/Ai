import csv
import json
import sys

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
dir = r"D:\DataSet\output_csv_lamoda_WITH_DUPLICATES"


def get_csv_list(directory):
    """
    Получает список путей ко всем CSV-файлам в указанной директории.

    :param directory: Путь к директории.
    :return: Список путей к CSV-файлам.
    """
    # Получаем список всех файлов в директории
    all_files = os.listdir(directory)

    # Отбираем только файлы с расширением .csv
    csv_files = [file for file in all_files if file.endswith('.csv')]

    # Создаем пути к этим файлам
    csv_files_with_path = [os.path.join(directory, file) for file in csv_files]

    return csv_files_with_path

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

def get_rows_from_csvs_reader(csv_list, batch_size=1000):
    """
    Считывает строки из списка CSV-файлов, возвращает их партиями.
    Пропускает первую строку (заголовки).
    :param csv_list: Список путей к CSV-файлам.
    :param batch_size: Размер партии.
    :return: Итератор списков строк.
    """
    buffer = []  # Буфер для накопления строк
    for csv_file in csv_list:
        with open(csv_file, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Пропускаем заголовки (первую строку)
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
        counter = 0
        for batch in get_rows_from_csvs(csv_list):
            for row in batch:
                try:
                    # Преобразование guid_list в формат PostgreSQL
                    guid_list = row.get('Guid_list')
                    if guid_list:  # Если guid_list не пустой
                        guid_list = '{' + ','.join(json.loads(guid_list)) + '}'  # Преобразуем в формат {value1,value2}
                    else:
                        guid_list = None

                    # Преобразование embedding
                    embedding = (
                        list(map(float, row['Embedding'][1:-1].split(',')))
                        if row.get('Embedding') else None
                    )

                    # Преобразуем данные в порядок, соответствующий SQL-запросу
                    data = (
                        row.get('Guid'),
                        row.get('Source_csv'),
                        row.get('Source'),
                        row.get('Image_url'),
                        row.get('Main_photo') == 'None',  # Преобразование в BOOLEAN
                        row.get('Article'),
                        guid_list,  # Массив для PostgreSQL
                        embedding,  # Вектор данных
                        row.get('Price_rub'),
                        row.get('Brand'),
                        row.get('Category'),
                        row.get('Subcategory'),
                        row.get('Gender'),
                        row.get('Title'),
                        row.get('Tags')
                    )

                    cur.execute(sql_query_insert, data)
                    counter += 1

                except (Exception, psycopg2.DatabaseError) as row_error:
                    print(f"Ошибка при обработке строки: {row_error}")
                    raise  # Прекращаем выполнение функции при критической ошибке
            print(counter)

        conn.commit()
        print(f"Данные успешно вставлены: {counter} строк.")
    except (Exception, psycopg2.DatabaseError) as db_error:
        print(f"Ошибка подключения к базе данных: {db_error}")
        sys.exit(1)  # Завершение программы при ошибке
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_data_to_old_bd_from_csv_reader(csv_list, conn_params):
    """
    Вставляет данные из списка CSV-файлов в таблицу card_row.
    :param csv_list: Список путей к CSV-файлам.
    :param conn_params: Параметры подключения к базе данных.
    """
    sql_query_insert = """
    INSERT INTO card_row (guid, source_csv, source, image_url, main_photo, article, guid_list, embedding, price_rub, brand, category, subcategory, gender, tags)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        counter = 0
        for batch in get_rows_from_csvs_reader(csv_list):
            for row in batch:
                try:
                    # Преобразование guid_list в формат PostgreSQL
                    guid_list = row[5]
                    if guid_list:  # Если guid_list не пустой
                        guid_list = '{' + ','.join(json.loads(guid_list)) + '}'  # Преобразуем в формат {value1,value2}
                    else:
                        guid_list = None

                    # Преобразование embedding
                    embedding = (
                        list(map(float, row[10][1:-1].split(',')))
                        if row[10] else None
                    )

                    # Преобразуем данные в порядок, соответствующий SQL-запросу
                    data = (
                        row[2],
                        row[1],
                        row[0],
                        row[3],
                        row[4] == "",  # Преобразование в BOOLEAN
                        row[6],
                        guid_list,  # Массив для PostgreSQL
                        embedding,  # Вектор данных
                        float(row[11].replace(" ","")),
                        row[12],
                        row[9],
                        row[8],
                        row[7],
                        #row.get('Title'),
                        row[13]
                    )

                    cur.execute(sql_query_insert, data)
                    counter += 1

                except (Exception, psycopg2.DatabaseError) as row_error:
                    print(f"Ошибка при обработке строки: {row_error}")
                    raise  # Прекращаем выполнение функции при критической ошибке
            print(counter)

        conn.commit()
        print(f"Данные успешно вставлены: {counter} строк.")
    except (Exception, psycopg2.DatabaseError) as db_error:
        print(f"Ошибка подключения к базе данных: {db_error}")
        sys.exit(1)  # Завершение программы при ошибке
    finally:
        if conn:
            cur.close()
            conn.close()
if __name__ == '__main__':
    #create_card_row_table(connection_params_origin)
    #files = get_csv_list(dir)
    #insert_data_to_old_bd_from_csv(files, connection_params_origin)
    files = get_csv_list(dir)
    insert_data_to_old_bd_from_csv_reader(files, connection_params_origin)

