import json

import psycopg2
import random

# Конфиг БД
DB_CONFIG = {
    "host": "193.232.55.5",
    "database": "postgres",
    "user": "postgres",
    "password": "super",
    "port": 5500
}

# ID пользователя
USER_ID = "98315d1d-4fa5-4bc6-be5e-ae69240d178f"


def get_random_categories():
    """Получает список всех категорий (UUID) и выбирает 5 случайных."""
    query = 'SELECT "Id" FROM "public"."Categories";'

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            categories = [row[0] for row in cur.fetchall()]

    return random.sample(categories, 5) if len(categories) >= 5 else categories



def get_cards_by_categories_and_tags(categories, tag_key_id, tag_value_id):
    query = '''
        SELECT DISTINCT ON (cat."Id") cards."Id", cat."Id"
        FROM "public"."Cards" cards
        JOIN "public"."SubCategories" subcat ON cards."Id" = subcat."CardId"
        JOIN "public"."SubCategory" sub ON subcat."SubCategoryId" = sub."Id"
        JOIN "public"."Categories" cat ON sub."CategoryId" = cat."Id"
        WHERE cat."Id" = ANY(%s::UUID[])
        AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements(cards."Tags"::jsonb -> %s) AS tag_values
            WHERE tag_values::text = %s
        )
        ORDER BY cat."Id", random();
    '''

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (categories, tag_key_id, str(tag_value_id)))
            return cur.fetchall()  # [(CardId, CategoryId), ...]


def get_similar_cards_for_user(user_id, categories, tag_key_id, tag_value_id):
    query = '''
        WITH user_data AS (
            SELECT "UserCategoryId", "UserEmbedding"
            FROM "DressyAI"."UsersEmbeddings"
            WHERE "UserId" = %s
        )
        SELECT DISTINCT ON (cat."Id") cards."Id", cat."Id"
        FROM "public"."Cards" cards
        JOIN "public"."SubCategories" subcat ON cards."Id" = subcat."CardId"
        JOIN "public"."SubCategory" sub ON subcat."SubCategoryId" = sub."Id"
        JOIN "public"."Categories" cat ON sub."CategoryId" = cat."Id"
        JOIN "DressyAI"."CardsEmbenddings" emb ON cards."Id" = emb."CardId"
        JOIN user_data u ON cat."Id" = u."UserCategoryId"
        WHERE cat."Id" = ANY(%s::UUID[])
        AND (1 - (emb."CardEmbendding" <=> u."UserEmbedding")) >= 0.7
        AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(cards."Tags" -> %s) tag_values  
            WHERE tag_values::int = %s
        )
        ORDER BY cat."Id", (emb."CardEmbendding" <=> u."UserEmbedding") ASC, random();
    '''

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (user_id, categories, tag_key_id, tag_value_id))
            return cur.fetchall()  # [(CardId, CategoryId), ...]


def generate_transaction_sql(user_id, cards):
    """Формирует SQL для пакетной вставки."""
    values = ",\n".join(f"('{user_id}', '{card_id}')" for card_id, _ in cards)

    sql = f'''
        INSERT INTO "public"."UsersEmbeddings" ("UserId", "CardId")
        VALUES
        {values};
    '''
    return sql


def main():
    categories = get_random_categories()
    if not categories:
        print("Нет доступных категорий!")
        return
    print(categories)
    tag_key_id = "3"
    tag_value_id = "32"

    cards = get_similar_cards_for_user(USER_ID,categories, tag_key_id, tag_value_id)
    if not cards:
        print("Нет подходящих карточек для выбранных категорий!")
        return
    print(cards)



if __name__ == "__main__":
    main()
