import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV
import psycopg2.extras

def insert_user_embedding(user_embeddings):
    query = """
    INSERT INTO "DressyAI"."UsersEmbeddings" ("Id","UserEmbedding", "usercategoryid", "UserId")
    VALUES %s;
    """

    values = [
        (user_embedding["id"],user_embedding["user_embedding"], user_embedding["category_id"], user_embedding["user_id"])
        for user_embedding in user_embeddings
    ]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
