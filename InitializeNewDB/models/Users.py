import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV
import psycopg2.extras

def insert_user(users):
    """
    Вставляет данные в таблицу CardImages.
    :param images: список словарей с данными изображений
    """
    query = """
    INSERT INTO public."Users" ("Id","Name", "Phone", "Gender", "AccountId")
    VALUES %s;
    """  # Убираем "Id" из запроса

    values = [
        (user["user_id"],user["user_name"], user["user_phone"], user["user_gender"] ,user["account_id"])
        for user in users
    ]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
