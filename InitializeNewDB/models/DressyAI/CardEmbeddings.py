import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV
import psycopg2.extras

def insert_embedding(cards_embedding):
    """
    Вставляет данные в таблицу CardImages.
    :param images: список словарей с данными изображений
    """
    query = """
    INSERT INTO "DressyAI"."CardsEmbenddings" ("Id","CardId", "CardEmbendding")
    VALUES %s;
    """  # Убираем "Id" из запроса

    values = [
        (card_embedding["card_id"],card_embedding["card_id"], card_embedding["embedding"])
        for card_embedding in cards_embedding
    ]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
