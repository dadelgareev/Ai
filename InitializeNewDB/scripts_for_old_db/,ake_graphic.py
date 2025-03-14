import psycopg2
import matplotlib.pyplot as plt
import numpy as np

# Конфиг БД
DB_CONFIG = {
    "host": "193.232.55.5",
    "database": "postgres",
    "user": "postgres",
    "password": "super",
    "port": 5500
}


def get_category_counts():
    query = """
        SELECT cat."Name", COUNT(cards."Id") AS card_count
        FROM public."Cards" cards
        JOIN public."SubCategories" subcat ON cards."Id" = subcat."CardId"
        JOIN public."SubCategory" sub ON subcat."SubCategoryId" = sub."Id"
        JOIN public."Categories" cat ON sub."CategoryId" = cat."Id"
        GROUP BY cat."Name"
        ORDER BY card_count DESC;
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()  # [(CategoryName, Count), ...]


def plot_category_distribution(data):
    """Строит график распределения карточек по категориям."""
    categories, counts = zip(*data)

    plt.figure(figsize=(12, 6))
    plt.barh(categories, counts, color=plt.cm.viridis(np.linspace(0, 1, len(categories))))
    plt.xlabel("Количество карточек")
    plt.ylabel("Категории")
    plt.title("Распределение карточек по категориям")
    plt.gca().invert_yaxis()  # Разворачиваем ось для лучшего отображения
    plt.show()


def main():
    data = get_category_counts()
    if data:
        plot_category_distribution(data)
    else:
        print("Данные о категориях не найдены!")


if __name__ == "__main__":
    main()
