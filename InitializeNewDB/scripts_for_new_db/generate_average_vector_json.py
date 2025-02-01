import json
import numpy as np
import psycopg2
from db.db_connection import get_connection, DB_CONFIG_DEV

# Половые категории
GENDERS = ["Man", "Woman"]


def fetch_categories():
    """Получает список всех категорий."""
    query = 'SELECT "Id", "Name" FROM public."Categories";'
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()  # [(CategoryId, CategoryName), ...]


def fetch_embeddings_by_category(category_id, gender):
    """
    Получает данные по конкретной категории и полу, включая эмбеддинги.
    """
    query = """
        SELECT 
            ce."CardId",
            ce."Id" AS EmbeddingId,
            c."Gender",
            cat."Id" AS CategoryId,
            cat."Name" AS CategoryName,
            ce."CardEmbendding"
            FROM "DressyAI"."CardsEmbenddings" ce
            JOIN public."Cards" c ON ce."CardId" = c."Id"
            JOIN public."SubCategories" sc ON c."Id" = sc."CardId"
            JOIN public."SubCategory" subcat ON sc."SubCategoryId" = subcat."Id"
            JOIN public."Categories" cat ON subcat."CategoryId" = cat."Id";
        WHERE cat."Id" = %s AND c."Gender" = %s
    """
    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (category_id, gender))
            return cursor.fetchall()  # [(CardId, EmbeddingId, Gender, CategoryId, CategoryName, CardEmbedding), ...]


def process_all_data(output_path="output_json/all_categories.json"):
    """Обрабатывает данные, вычисляет средний эмбеддинг и сохраняет JSON."""
    categories = fetch_categories()
    all_results = []

    for category_id, category_name in categories:
        for gender in GENDERS:
            print(f"Обрабатываем категорию: {category_name} (ID: {category_id}), Пол: {gender}")

            data = fetch_embeddings_by_category(category_id, gender)

            if not data:
                print(f"Нет данных для категории {category_name} и пола {gender}")
                result = {
                    "Gender": gender,
                    "CategoryId": category_id,
                    "CategoryName": category_name,
                    "AverageEmbedding": str(np.zeros(1000).tolist())
                }
                all_results.append(result)
                continue

            embeddings = []
            for row in data:
                try:
                    embedding = json.loads(row[5])  # Преобразуем строку в массив чисел
                    embeddings.append(embedding)
                except json.JSONDecodeError:
                    print(f"Ошибка парсинга эмбеддинга для CardId {row[0]}")
                    continue

            if embeddings:
                average_embedding = np.mean(embeddings, axis=0).tolist()  # Средний эмбеддинг (преобразуем в список)
            else:
                average_embedding = []

            result = {
                "Gender": gender,
                "CategoryId": category_id,
                "CategoryName": category_name,
                "AverageEmbedding": str(average_embedding)
            }
            all_results.append(result)

    # Сохраняем JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=4)

    print(f"Сохранено {len(all_results)} записей в {output_path}")


if __name__ == "__main__":
    process_all_data("../output_json/average_embedding.json")  # Можно передать другой путь
