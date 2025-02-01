import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV

def delete():
    query = """
        DELETE FROM public."CardTags";
        DELETE FROM public."SubCategories";
        DELETE FROM public."CardImages";
        DELETE FROM public."Cards";
        DELETE FROM "DressyAI"."CardsEmbenddings";
    """
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()

if __name__ == '__main__':
    delete()