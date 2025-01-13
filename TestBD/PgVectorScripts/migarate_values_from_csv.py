import csv
import json

import psycopg2
import uuid
from psycopg2.extensions import adapt
from psycopg2.extras import Json
import os
import ast


connection_params_destination = {
    'host': '89.111.172.215',
    'database': 'postgres',
    'port': 5432,
    'user': 'postgres',
    'password': 'm4zoM4gpHhMFGRAms056NsoBPbae6AEK'
}

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

def get_csv_list():
    files_list = os.listdir(os.getcwd())
    csv_list = []
    for file in files_list:
        if file.endswith(".csv"):
            csv_list.append(file)

    return csv_list

def insert_data(csv_list):
    with psycopg2.connect(**connection_params_destination) as conn:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO"""


def get_mc_to_c_dict(conn_params):
    """
    Извлекает все строки из таблицы mc_to_c и возвращает словарь.

    :param db_config: словарь с параметрами подключения к БД (host, dbname, user, password)
    :return: словарь {id: (microcategory1_id, microcategory2_id)}
    """
    query = """
    SELECT id, microcategory1_id, microcategory2_id
    FROM card.mc_to_c;
    """

    try:
        # Подключаемся к базе данных
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # Выполняем запрос
        cursor.execute(query)
        result = cursor.fetchall()  # [(id, microcategory1_id, microcategory2_id), ...]

        # Преобразуем результат в словарь
        data = {row[0]: (row[1], row[2]) for row in result}

        return data
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return {}
    finally:
        # Закрываем соединение
        if connection:
            cursor.close()
            connection.close()

def get_microcategory1_dict(conn_params):
    query = """
    SELECT id, name
    FROM card.microcategory1;
    """

    try:
        # Подключаемся к базе данных
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # Выполняем запрос
        cursor.execute(query)
        result = cursor.fetchall()

        # Преобразуем результат в словарь
        data = {row[0]: row[1] for row in result}

        return data
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return {}
    finally:
        # Закрываем соединение
        if connection:
            cursor.close()
            connection.close()

def get_microcategory2_dict(conn_params):
    query = """
    SELECT id, name
    FROM card.microcategory2;
    """

    try:
        # Подключаемся к базе данных
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # Выполняем запрос
        cursor.execute(query)
        result = cursor.fetchall()

        # Преобразуем результат в словарь
        data = {row[0]: row[1] for row in result}

        return data
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return {}
    finally:
        # Закрываем соединение
        if connection:
            cursor.close()
            connection.close()

def get_mc_to_c_id(microcategory1_name, microcategory2_name):
    microcategory1_id = None
    microcategory2_id = None

    for key in microcategory1_data:
        if microcategory1_data[key] == microcategory1_name:
            microcategory1_id = key
            break

    for key in microcategory2_data:
        if microcategory2_data[key] == microcategory2_name:
            microcategory2_id = key
            break

    for key in mc2c_data:
        if mc2c_data[key] == (microcategory1_id, microcategory2_id):
            return key

def get_brand_dict(conn_params):
    query = """
        SELECT id, name
        FROM card.brand;
        """

    try:
        # Подключаемся к базе данных
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # Выполняем запрос
        cursor.execute(query)
        result = cursor.fetchall()

        # Преобразуем результат в словарь
        data = {row[0]: row[1] for row in result}

        return data
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return {}
    finally:
        # Закрываем соединение
        if connection:
            cursor.close()
            connection.close()

def get_gender_dict(conn_params):
    query = """
        SELECT id, name
        FROM card.gender;
        """

    try:
        # Подключаемся к базе данных
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # Выполняем запрос
        cursor.execute(query)
        result = cursor.fetchall()

        # Преобразуем результат в словарь
        data = {row[0]: row[1] for row in result}

        return data
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return {}
    finally:
        # Закрываем соединение
        if connection:
            cursor.close()
            connection.close()

def get_source_dict(conn_params):
    query = """
        SELECT id, name
        FROM card.source;
        """

    try:
        # Подключаемся к базе данных
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # Выполняем запрос
        cursor.execute(query)
        result = cursor.fetchall()

        # Преобразуем результат в словарь
        data = {row[0]: row[1] for row in result}

        return data
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return {}
    finally:
        # Закрываем соединение
        if connection:
            cursor.close()
            connection.close()

def get_tags_dict(conn_params):
    query = """
            SELECT id, name
            FROM card.tag;
            """

    try:
        # Подключаемся к базе данных
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # Выполняем запрос
        cursor.execute(query)
        result = cursor.fetchall()

        # Преобразуем результат в словарь
        data = {row[0]: row[1] for row in result}

        return data
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return {}
    finally:
        # Закрываем соединение
        if connection:
            cursor.close()
            connection.close()

def get_brand_id(brand):
    brand_id = None
    for key in brand_data:
        if brand_data[key] == brand:
            brand_id = key
            break

    return brand_id

def get_gender_id(gender):
    gender_id = None
    for key in gender_data:
        if gender_data[key] == gender:
            gender_id = key
            break
    return gender_id

def get_source_id(source):
    source_id = None
    for key in source_data:
        if source_data[key] == source:
            source_id = key
            break
    return source_id






def get_tags_list_from_csv(csv_list, batch_size=1000):
    """
    Генератор, который считывает столбец 'Tags' из CSV-файлов и возвращает список значений партиями.

    :param csv_list: Список путей к CSV-файлам.
    :param batch_size: Размер партии (по умолчанию 1000).
    :return: Генератор, возвращающий списки значений 'Tags' партиями.
    """
    buffer = []  # Буфер для накопления строк
    tags_list = []

    generator = get_rows_from_csvs(csv_list, batch_size)  # Генератор строк из CSV-файлов
    counter = 0
    for rows in generator:  # Обрабатываем строки, возвращаемые генератором
        for row in rows:
            row_dict = ast.literal_eval(row["Tags"])
            buffer.append(row_dict)
            for key in row_dict:
                tags_list.append({key: row_dict[key]})
        if counter >= batch_size:
            counter += 1
            print(counter)
            yield tags_list
            buffer = []
            tags_list = []

    if buffer:  # Если остались элементы в буфере после завершения итерации
        yield tags_list

def insert_row_to_bd_from_csv(csv_list, batch_size, conn_params, source_data, brand_data, mc_to_c_data, gender_data):
    connection = psycopg2.connect(**conn_params)
    cursor = connection.cursor()

    query_to_card = """
    INSERT INTO card.card (id, source_id, article, price_rub, brand_id, mc_to_c_id, gender_id, title)
    VALUES (%s,%s, %s, %s, %s, %s, %s, %s);
    """

    query_to_image_array = """
    INSERT INTO card.image_array (card_id, link, main, vector, image_id)
    VALUES (%s, %s, %s, %s, %s);
    """

    query_to_image = """
    INSERT INTO card.image (link)
    VALUES (%s)
    RETURNING id;
    """

    query_to_tag_array = """
    INSERT INTO card.tag_array (tag_id, card_id)
    VALUES (%s, %s);
    """

    query_to_tag = """
    INSERT INTO card.tag (tag_key, tag_value)
    VALUES (%s, %s)
    RETURNING id;
    """

    generator = get_rows_from_csvs(csv_list, batch_size)
    last_guid = None  # Храним последний обработанный guid
    for rows in generator:
        for row in rows:
            source = row["Source"]
            article = row["Article"]
            price = row["Price"]
            brand = row["Brand"]
            gender = row["Gender"]
            tags = row["Tags"]

            title = row["Title"]
            guid = row["Guid"]
            category = row["Category"]
            subcategory = row["Subcategory"]
            image_url = row["Image_url"]
            main_photo = row["Main_photo"]
            embedding = row["Embedding"]

            if main_photo == "None":
                main_photo = False
            else:
                main_photo = True

            # Получаем IDs для необходимых данных
            mc2c_id = get_mc_to_c_id(category, subcategory)
            source_id = get_source_id(source)
            brand_id = get_brand_id(brand)
            gender_id = get_gender_id(gender)

            # Проверяем, обрабатывался ли этот guid на предыдущей итерации
            if guid != last_guid:
                # Вставляем запись в card.card только если guid новый
                cursor.execute(query_to_card, (guid, source_id, article, price, brand_id, mc2c_id, gender_id, title))

            # Обрабатываем теги
            for key, value in tags.items():
                cursor.execute(query_to_tag, (key, value))
                tag_id = cursor.fetchone()[0]  # Получаем id только что вставленного тега
                cursor.execute(query_to_tag_array, (tag_id, guid))

            # Обрабатываем изображение
            cursor.execute(query_to_image, (image_url,))
            image_id = cursor.fetchone()[0]  # Получаем id только что вставленного изображения
            cursor.execute(query_to_image_array, (guid, image_url, main_photo, embedding, image_id))

            # Обновляем last_guid для проверки на следующей итерации
            last_guid = guid

    # Фиксируем изменения в базе данных
    connection.commit()

    # Закрываем соединение
    cursor.close()
    connection.close()













if __name__ == '__main__':
    csv_list = get_csv_list()
    generator = get_rows_from_csvs(csv_list)
    mc2c_data = get_mc_to_c_dict(connection_params_destination)
    microcategory1_data = get_microcategory1_dict(connection_params_destination)
    microcategory2_data = get_microcategory2_dict(connection_params_destination)
    brand_data = get_brand_dict(connection_params_destination)
    gender_data = get_gender_dict(connection_params_destination)
    source_data = get_source_dict(connection_params_destination)
    print(mc2c_data)
    print(microcategory1_data)
    print(microcategory2_data)
    print(brand_data)
    print(gender_data)
    print(get_mc_to_c_id("Платья и сарафаны","Сарафаны"))
    tags_generator = get_tags_list_from_csv(csv_list, batch_size=1000)
    #for tags in tags_generator:
        #print(tags)
        #break
