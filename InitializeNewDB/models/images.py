import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV
import psycopg2.extras

def insert_images(images):
    """
    Вставляет данные в таблицу CardImages.
    :param images: список словарей с данными изображений
    """
    query = """
    INSERT INTO public."CardImages" ("Id","Links", "MainLink", "CardId", "CreatedDate", "UpdatedDate")
    VALUES %s
    ON CONFLICT ("CardId") DO NOTHING;
    """  # Убираем "Id" из запроса

    values = [
        (image["card_id"],image["image_list"], image["main_link"], image["card_id"], "NOW()", "NOW()")
        for image in images
    ]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
