import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV

def insert_images(images):
    """
    Вставляет данные в таблицу CardImages
    :param images: список словарей с данными изображений
    """
    query = """
    INSERT INTO public."CardImages" ("Id", "Links", "MainLink", "CardId", "CreatedDate", "UpdatedDate")
    VALUES %s
    ON CONFLICT ("Id") DO NOTHING;
    """

    values = [
        (image["id"], image["links"], image["main_link"], image["card_id"], "NOW()", "NOW()")
        for image in images
    ]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
