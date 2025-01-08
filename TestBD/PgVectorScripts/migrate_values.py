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

table_name = "products_all"

for batch in get_pagination_rows(connection_params_origin, table_name):
    print(f"Получено {len(batch)} строк")
    for row in batch:
        print(row) # Обработка каждой строки
        insert_to_card_and_image(connection_params_destination, row)
    break

print(get_id_by_categories(connection_params_destination,"Блузы и рубашки","Рубашки с коротким рукавом"))
