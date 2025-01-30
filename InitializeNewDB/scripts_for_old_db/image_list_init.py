import psycopg2
from psycopg2.extras import execute_values
from db.db_connection import get_connection, DB_CONFIG_ROW

BATCH_SIZE = 5  # Размер пакета

def fetch_unique_articles(offset, batch_size):
    """Получаем уникальные значения article с пагинацией"""
    query = f"""
        SELECT DISTINCT article
        FROM card_row
        WHERE image_url IS NOT NULL
        ORDER BY article
        LIMIT {batch_size} OFFSET {offset};
    """
    with get_connection(DB_CONFIG_ROW) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()  # [(article), ...]

def fetch_records_for_article(article):
    """Получаем все записи для указанного article"""
    query = """
        SELECT guid, image_url
        FROM card_row
        WHERE article = %s AND image_url IS NOT NULL
        ORDER BY guid;
    """
    with get_connection(DB_CONFIG_ROW) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (article,))
            return cursor.fetchall()  # [(guid, image_url), ...]

def update_image_list_for_article(article, image_urls):
    """Обновляем image_list для всех записей с данным article"""
    query = """
        UPDATE card_row
        SET image_list = %s
        WHERE article = %s AND image_url IS NOT NULL;
    """
    with get_connection(DB_CONFIG_ROW) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (image_urls, article))
            conn.commit()

def process_card_row_data():
    """Основная функция обработки данных"""
    offset = 0

    while True:
        # Получаем уникальные article с пагинацией
        articles = fetch_unique_articles(offset, BATCH_SIZE)
        if not articles:
            print("Обновление завершено")
            break  # Если данных больше нет, выходим

        print(f"Обрабатываем {len(articles)} articles, OFFSET: {offset}")

        for article, in articles:
            # Получаем все записи для этого article
            records = fetch_records_for_article(article)

            # Собираем уникальные image_url для всех записей с этим article
            image_urls = list(set([image_url for _, image_url in records]))

            # Обновляем все записи для этого article
            update_image_list_for_article(article, image_urls)

        # Увеличиваем offset для следующей порции
        offset += BATCH_SIZE



if __name__ == "__main__":
    process_card_row_data()
