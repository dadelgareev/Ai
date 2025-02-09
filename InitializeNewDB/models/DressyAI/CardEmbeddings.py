import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV
import psycopg2.extras

def insert_embedding(cards_embedding):
    query = """
    INSERT INTO "DressyAI"."CardsEmbeddings" ("Id","CardId", "CardEmbedding")
    VALUES %s;
    """

    values = [
        (card_embedding["card_id"],card_embedding["card_id"], card_embedding["embedding"])
        for card_embedding in cards_embedding
    ]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
