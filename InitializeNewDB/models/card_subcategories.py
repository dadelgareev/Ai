import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV

def insert_card_subcategories(card_subcategories):
    """
    Вставляет данные в таблицу CardSubCategories (связь карточек с подкатегориями)
    :param card_subcategories: список словарей {card_id, subcategory_id}
    """
    query = """
    INSERT INTO public."SubCategories" ("CardId", "SubCategoryId")
    VALUES %s
    ON CONFLICT ("CardId", "SubCategoryId") DO NOTHING;
    """

    values = [(cs["card_id"], cs["subcategory_id"]) for cs in card_subcategories]

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
