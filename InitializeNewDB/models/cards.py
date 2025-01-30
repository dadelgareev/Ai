import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV

def insert_cards(cards):
    """
    Вставляет данные в таблицу Cards
    :param cards: список словарей с данными карточек
    """
    query = """
    INSERT INTO public."Cards" ("Id", "Article", "Source", "Price", "BrandId", "Gender", "CreatedDate", "UpdatedDate")
    VALUES %s
    ON CONFLICT ("Id") DO NOTHING;
    """

    values = [
        (card["id"], card["article"], card["source"], card["price"], card["brand_id"], card["gender"], "NOW()", "NOW()")
        for card in cards
    ]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()

