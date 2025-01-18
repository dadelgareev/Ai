import ast
import csv
import json

import psycopg2
import uuid
from psycopg2.extensions import adapt
from psycopg2.extras import Json
import os


connection_params_destination = {
    'host': '89.111.172.215',
    'database': 'postgres',
    'port': 5432,
    'user': 'postgres',
    'password': 'm4zoM4gpHhMFGRAms056NsoBPbae6AEK'
}

connection_params_origin = {
    'host': '193.232.55.5',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}

def get_pagination_rows(connection_params, table_name, page_size=1000):
    # Подключение к базе данных
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    # Инициализация смещения
    offset = 0

    try:
        while True:
            # SQL-запрос с пагинацией
            query = f"SELECT * FROM {table_name} LIMIT {page_size} OFFSET {offset};"
            cursor.execute(query)

            # Получаем текущую порцию данных
            batch = cursor.fetchall()
            if not batch:
                break  # Прерываем цикл, если данных больше нет

            # Возвращаем текущую порцию данных
            yield batch

            # Смещаем offset для следующей порции
            offset += page_size
    finally:
        # Закрытие соединения
        cursor.close()
        conn.close()

def get_id_by_categories(connection_params, microcategory1_name, microcategory2_name):
    """
    Ищет запись по именам категорий microcategory1 и microcategory2 с использованием JOIN
    в таблице card.mc_to_c и возвращает id.
    """
    # Подключение к базе данных
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    try:
        # Выполняем запрос с JOIN
        query = """
        SELECT mc_to_c.id
        FROM card.mc_to_c
        JOIN card.microcategory1 AS m1 ON mc_to_c.microcategory1_id = m1.id
        JOIN card.microcategory2 AS m2 ON mc_to_c.microcategory2_id = m2.id
        WHERE m1.name = %s AND m2.name = %s;
        """
        # Выполняем запрос с передачей параметров
        cursor.execute(query, (microcategory1_name, microcategory2_name))
        result = cursor.fetchone()

        # Если запись найдена, возвращаем ID
        if result:
            return result[0]
        else:
            return None  # Если совпадений нет

    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None

    finally:
        cursor.close()
        conn.close()


def get_brand_id(connection_params, brand_name):
    """
    Получает ID бренда по его названию из таблицы brand.

    :param connection_params: Параметры подключения к базе данных.
    :param brand_name: Название бренда.
    :return: ID бренда или None, если бренд не найден.
    """
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # SQL-запрос для получения ID бренда
        query = """
            SELECT id
            FROM card.brand
            WHERE name = %s
            LIMIT 1;
        """
        cursor.execute(query, (brand_name,))

        # Получаем результат
        result = cursor.fetchone()

        # Возвращаем ID, если найден
        if result:
            return result[0]
        else:
            return None  # Если бренд не найден

    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None

    finally:
        # Закрываем соединение
        if conn:
            cursor.close()
            conn.close()


def get_gender_id(connection_params, gender_name):
    """
    Получает ID гендера по его названию из таблицы gender.

    :param connection_params: Параметры подключения к базе данных.
    :param gender_name: Название гендера.
    :return: ID гендера или None, если гендер не найден.
    """
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # SQL-запрос для получения ID гендера
        query = """
            SELECT id
            FROM card.gender
            WHERE name = %s
            LIMIT 1;
        """
        cursor.execute(query, (gender_name,))

        # Получаем результат
        result = cursor.fetchone()

        # Возвращаем ID, если найден
        if result:
            return result[0]
        else:
            return None  # Если гендер не найден

    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None

    finally:
        # Закрываем соединение
        if conn:
            cursor.close()
            conn.close()


def get_source_id(connection_params, source_name):
    """
    Получает ID источника по его названию из таблицы source.

    :param connection_params: Параметры подключения к базе данных.
    :param source_name: Название источника.
    :return: ID источника или None, если источник не найден.
    """
    try:
        source_name = source_name.lower()
        # Подключение к базе данных
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # SQL-запрос для получения ID источника
        query = """
            SELECT id
            FROM card.source
            WHERE name = %s
            LIMIT 1;
        """
        cursor.execute(query, (source_name,))

        # Получаем результат
        result = cursor.fetchone()

        # Возвращаем ID, если найден
        if result:
            return result[0]
        else:
            return None  # Если источник не найден

    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None

    finally:
        # Закрываем соединение
        if conn:
            cursor.close()
            conn.close()



def insert_to_card_and_image(connection_params, data_tuple):
    """
    Вставляет данные в таблицу card.mc_to_c.

    :param connection_params: Параметры подключения к базе данных.
    :param data_tuple: Кортеж с данными для вставки: (source_id, article, price_rub, brand_id, mc_to_c_id, gender_id).
    :return: Возвращает True, если вставка прошла успешно, иначе False.
    """
    try:
        url_namespace = uuid.NAMESPACE_URL  # Стандартное пространство имен для URL
        # Подключение к базе данных
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        source_row = get_source_id(connection_params_destination,data_tuple[1])
        article = data_tuple[5]
        price_rub = data_tuple[10]
        brand_id = get_brand_id(connection_params_destination,data_tuple[11])
        mc_to_c_id = get_id_by_categories(connection_params_destination, data_tuple[7], data_tuple[8])
        gender_id = get_gender_id(connection_params_destination, data_tuple[6])


        vector = data_tuple[9]
        image_link = data_tuple[3]
        print(image_link)
        uuid_id = uuid.uuid5(url_namespace, image_link)
        uuid_id_str = str(uuid_id)
        main_photo = data_tuple[4]
        if not main_photo:
            return None


        # SQL-запрос для вставки данных
        query_add_2_card = """
            INSERT INTO card.card (id,source_id, article, price_rub, brand_id, mc_to_c_id, gender_id)
            VALUES (%s,%s, %s, %s, %s, %s, %s);
        """

        print(source_row)
        print(article)
        print(price_rub)
        print(brand_id)
        print(mc_to_c_id)
        print(gender_id)
        print(uuid_id)
        print(vector)
        # Выполнение запроса с передачей данных
        cursor.execute(query_add_2_card, (uuid_id_str,source_row,article,price_rub,brand_id,mc_to_c_id,gender_id))
        print("В карточку вставили!")
        query_add_2_image = """
            INSERT INTO card.image_array (card_id, link,main,vector)
            VALUES (%s,%s, %s, %s);
        """
        cursor.execute(query_add_2_image, (uuid_id_str,image_link,main_photo,vector))
        print("В картинку вставили!")
        # Фиксируем изменения в базе данных
        conn.commit()

        print("Данные успешно вставлены в таблицу.")
        return True

    except Exception as e:
        print(f"Ошибка при вставке данных в таблицу card.mc_to_c: {e}")
        return False

    finally:
        # Закрываем соединение
        if conn:
            cursor.close()
            conn.close()


def get_rows_from_bd(connection_params, table_name, batch_size=1000):
    """
    Генераторная функция для получения записей из таблицы базы данных порциями.

    :param connection_params: Параметры подключения к базе данных.
    :param table_name: Имя таблицы для извлечения данных.
    :param batch_size: Размер порции (по умолчанию 1000 записей).
    :yield: Порция данных в виде списка записей.
    """
    query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
    offset = 0

    try:
        conn = psycopg2.connect(**connection_params)
        cur = conn.cursor()

        while True:
            cur.execute(query, (batch_size, offset))
            rows = cur.fetchall()

            if not rows:  # Если данных больше нет, выходим из цикла
                break

            yield rows  # Возвращаем порцию данных
            offset += batch_size  # Увеличиваем смещение

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при чтении из базы данных: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()

def get_brand_id_from_dict(brand):
    brand_id = None
    for key in brand_data:
        if brand_data[key] == brand:
            brand_id = key
            break

    return brand_id

def get_gender_id_from_dict(gender):
    gender_id = None
    for key in gender_data:
        if gender_data[key] == gender:
            gender_id = key
            break
    return gender_id

def get_source_id_from_dict(source):
    source_id = None
    for key in source_data:
        if source_data[key] == source:
            source_id = key
            break
    return source_id

def get_tags_id_from_dict(tags):
    tags_id = None
    for key in tags:
        if tags[key] == tags:
            tags_id = key
            break
    return tags_id

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
            SELECT id, tag_key, tag_value
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
        data = {row[0]: f"{row[1]}:{row[2]}" for row in result}

        return data
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return {}
    finally:
        # Закрываем соединение
        if connection:
            cursor.close()
            connection.close()




def insert_into_table(conn_params, table_name, id, value):
    """
    Вставляет запись в указанную таблицу.

    :param conn_params: Параметры подключения к базе данных.
    :param table_name: Имя таблицы.
    :param value: Значение для вставки (строка или кортеж).
    """
    sql_query = f"INSERT INTO {table_name} (id, name) VALUES (%s,%s);"  # Предполагаем, что у таблицы есть колонка 'name'

    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(sql_query, (id,value,))
        conn.commit()
        print(f"Успешно добавлено значение '{value}' с id {id} в таблицу '{table_name}'.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при вставке значения в таблицу '{table_name}': {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


# Функция для добавления бренда
def insert_into_brand(conn_params, id, value):
    insert_into_table(conn_params, "card.brand", id, value)


# Функция для добавления пола (gender)
def insert_into_gender(conn_params, id, value):
    insert_into_table(conn_params, "card.gender", id, value)


# Функция для добавления источника (source)
def insert_into_source(conn_params, id, value):
    insert_into_table(conn_params, "card.source", id, value)

# Функция для добавления связи mc_to_c
def insert_into_mc_to_c(conn_params, id, value=None):
    insert_into_table(conn_params, "card.mc_to_c", id)

def insert_to_new_table(connection_orig, connection_dest, table_name_old):
    """
    Функция для переноса данных из одной базы в другую с использованием флага для проверки обработанных данных.
    :param connection_orig: Параметры подключения к исходной базе данных.
    :param connection_dest: Параметры подключения к целевой базе данных.
    :param table_name_old: Название таблицы-источника.
    """
    query_add_2_card = """
        INSERT INTO card.card (id, source_id, article, price_rub, brand_id, mc_to_c_id, gender_id, tilte)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    query_add_2_image_array = """
        INSERT INTO card.image_array (card_id, link, main, vector)
        VALUES (%s, %s, %s, %s);
    """

    query_add_2_tags = """
        INSERT INTO card.tags_array (card_id, tag_id)
        VALUES (%s, %s);
    """

    query_add_2_image = """
        INSERT INTO card.image (link)
        VALUES (%s) RETURNING id;
    """

    query_check_card = """
        SELECT 1 FROM card.card WHERE id = %s LIMIT 1;
    """

    try:
        conn_dest = psycopg2.connect(**connection_dest)
        cursor_dest = conn_dest.cursor()

        for rows in get_rows_from_bd(connection_orig, table_name_old):
            for row in rows:
                guid = row[0]
                flag = False  # Флаг для отслеживания существующего GUID

                # Проверяем, если guid уже существует в таблице card
                cursor_dest.execute(query_check_card, (guid,))
                if cursor_dest.fetchone():
                    flag = True

                if flag:
                    # Добавляем только данные для image и image_array
                    link = row[9]
                    main_photo = row[10]
                    main_photo = main_photo == "None"  # Преобразуем в BOOLEAN
                    vector = row[11]

                    # Вставляем данные в таблицу image
                    cursor_dest.execute(query_add_2_image, (link,))
                    image_id = cursor_dest.fetchone()[0]

                    # Вставляем данные в таблицу image_array
                    cursor_dest.execute(query_add_2_image_array, (guid, link, main_photo, vector))
                    print(f"Добавлены данные в image и image_array для GUID: {guid}")
                    continue  # Переходим к следующей строке

                # Обработка текущей строки
                source_id = row[1]
                article = row[2]
                price_rub = row[3]
                brand_id = row[4]
                gender_id = row[5]
                category = row[6]
                subcategory = row[7]
                tags = row[8]
                title = "default"

                # Преобразование данных
                source_id = get_source_id_from_dict(source_id)
                brand_id = get_brand_id_from_dict(brand_id)
                mc_to_c_id = get_mc_to_c_id(category, subcategory)
                gender_id = get_gender_id_from_dict(gender_id)
                tags_dict = ast.literal_eval(tags)

                # Вставляем данные в card
                cursor_dest.execute(query_add_2_card, (guid, source_id, article, price_rub, brand_id, mc_to_c_id, gender_id, title))
                print(f"Добавлены данные в card для GUID: {guid}")

                # Вставляем данные в tags_array
                for tag in tags_dict:
                    tag_id = get_tags_id_from_dict(tag)
                    cursor_dest.execute(query_add_2_tags, (guid, tag_id))
                print(f"Добавлены данные в tags_array для GUID: {guid}")

                # Вставляем данные в image и image_array
                link = row[9]
                main_photo = row[10]
                main_photo = main_photo == "None"  # Преобразуем в BOOLEAN
                vector = row[11]

                cursor_dest.execute(query_add_2_image, (link,))
                image_id = cursor_dest.fetchone()[0]

                cursor_dest.execute(query_add_2_image_array, (guid, link, main_photo, vector))
                print(f"Добавлены данные в image и image_array для GUID: {guid}")

        conn_dest.commit()  # Фиксируем изменения
        print("Все данные успешно перенесены.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при переносе данных: {error}")
    finally:
        if conn_dest:
            cursor_dest.close()
            conn_dest.close()



table_name_old = "card_row"

mc2c_data = get_mc_to_c_dict(connection_params_destination)
microcategory1_data = get_microcategory1_dict(connection_params_destination)
microcategory2_data = get_microcategory2_dict(connection_params_destination)
brand_data = get_brand_dict(connection_params_destination)
gender_data = get_gender_dict(connection_params_destination)
source_data = get_source_dict(connection_params_destination)
tags_data = get_tags_dict(connection_params_destination)

"""
for batch in get_pagination_rows(connection_params_origin, table_name):
    print(f"Получено {len(batch)} строк")
    for row in batch:
        print(row) # Обработка каждой строки
        insert_to_card_and_image(connection_params_destination, row)
    break
"""

#print(get_id_by_categories(connection_params_destination,"Блузы и рубашки","Рубашки с коротким рукавом"))
