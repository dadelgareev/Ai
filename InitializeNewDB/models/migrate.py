import json
import os
from db.db_connection import DB_CONFIG_ROW, get_connection
from models.cards import insert_cards
from models.images import insert_images
from models.card_tags import insert_card_tags
from models.card_subcategories import insert_card_subcategories

def fetch_data_from_db(config, query, return_as_list=False):
    """
    Универсальная функция для получения данных из базы данных.
    :param config: Конфигурация подключения к базе данных.
    :param query: SQL-запрос для выполнения.
    :param return_as_list: Если True, возвращает результат как список.
    :return: Словарь {key: value}, множество ключей или список строк.
    """
    with get_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return rows if return_as_list else {row[0]: row[1] for row in rows}

def load_json(file_path):
    """
    Загружает JSON-файл в словарь или список.

    :param file_path: Путь к JSON-файлу.
    :return: Словарь или список с содержимым JSON.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Ошибка загрузки {file_path}: {e}")
        return {}

def migrate_data(batch_size=1000):
    """
    Миграция данных из card_row в таблицы Cards, CardImages, CardTags и CardSubCategories.
    """
    # Загружаем JSON-файлы
    brands = load_json("../output_json/brands.json")
    tags = load_json("../output_json/tags.json")
    categories = load_json("../output_json/categories.json")
    subcategories = load_json("../output_json/subcategories.json")

    if not all([brands, tags, subcategories]):
        print("Ошибка: один или несколько JSON-файлов отсутствуют или пусты.")
        return

    offset = 0
    processed_articles = set()  # Для отслеживания уже добавленных карточек

    while True:
        query = f"""
        SELECT guid, article, source, gender, price_rub, brand, category, subcategory, tags, title, image_url, main_photo, embedding, image_list
        FROM card_row
        WHERE article IS NOT NULL AND brand IS NOT NULL AND main_photo IS NOT NULL
        ORDER BY article
        LIMIT {batch_size} OFFSET {offset};
        """
        rows = fetch_data_from_db(DB_CONFIG_ROW, query, return_as_list=True)

        if not rows:
            print("Миграция завершена.")
            break  # Выход, если больше нет данных

        print(f"Обрабатывается пакет с OFFSET {offset}, размер: {len(rows)}")

        cards_data = []
        images_data = []
        card_tags_data = []
        card_subcategories_data = []

        for row in rows:
            uuid, article, source, gender, price, brand, category, subcategory, tag_str, title, image_url, main_photo, vector, image_list = row

            # Получаем ID сущностей
            brand_id = brands.get(brand)
            category_id = categories.get(category)
            subcategory_id = subcategories.get(f"{subcategory}|{category_id}")

            if not brand_id or not subcategory_id:
                print(f"Пропущена строка: brand={brand}, subcategory={subcategory}")
                continue

            # Проверяем, добавлена ли уже карточка
            if article not in processed_articles:
                processed_articles.add(article)

                card = {
                    "id": uuid,
                    "article": article,
                    "source": source,
                    "price": price,
                    "brand_id": brand_id,
                    "gender": gender
                }
                cards_data.append(card)


                # Добавляем связь с SubCategories
                card_subcategories_data.append({
                    "card_id": uuid,
                    "subcategory_id": subcategory_id
                })

                # Обработка тегов
                try:
                    parsed_tags = json.loads(tag_str)
                    for tag_key, tag_value in parsed_tags.items():
                        tag_uuid = tags.get(f"{tag_key}|{tag_value}")
                        if tag_uuid:
                            card_tags_data.append({
                                "card_id": uuid,
                                "tag_id": tag_uuid
                            })
                except json.JSONDecodeError:
                    print(f"Ошибка обработки тегов: {tag_str}")

                # Добавляем изображения
                image = {
                    "card_id": uuid,
                    "image_url": image_url,
                    "main_photo": main_photo,
                    "vector": vector
                }
                images_data.append(image)


        # Вставляем данные партиями
        if cards_data:
            print("Добавляем карточки...")
            insert_cards(cards_data)
        if images_data:
            print("Добавляем изображения...")
            insert_images(images_data)
        if card_subcategories_data:
            print("Добавляем связи карточек с подкатегориями...")
            insert_card_subcategories(card_subcategories_data)
        if card_tags_data:
            print("Добавляем связи карточек с тегами...")
            insert_card_tags(card_tags_data)

        offset += batch_size  # Переход к следующей партии

if __name__ == "__main__":
    try:
        migrate_data(batch_size=1000)
        print("Миграция данных успешно завершена.")
    except Exception as e:
        print(f"Ошибка при миграции данных: {e}")
