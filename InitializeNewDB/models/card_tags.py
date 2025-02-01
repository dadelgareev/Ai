import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV

def insert_card_tags(card_tags):
    """
    Вставляет данные в таблицу CardTags (связь карточек с тегами)
    :param card_tags: список словарей {card_id, tag_id}
    """
    query = """
    INSERT INTO public."CardTags" ("CardId", "TagId")
    VALUES %s
    ON CONFLICT ("CardId", "TagId") DO NOTHING;
    """

    values = [(ct["card_id"], ct["tag_id"]) for ct in card_tags]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
