import json
import os

from models.cards import insert_cards
from models.images import insert_images

from db.db_connection import DB_CONFIG_ROW
from db.db_connection import get_connection

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

            if return_as_list:
                return rows  # Вернуть данные как список строк
            else:
                return {row[0]: row[1] for row in rows}  # По умолчанию словарь {key: value}



def load_json(file_path):
    """
    Загружает JSON-файл в словарь.

    :param file_path: Путь к JSON-файлу.
    :return: Словарь с содержимым JSON.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
        return {}
    except json.JSONDecodeError as e:
        print(f"Ошибка при разборе JSON файла {file_path}: {e}")
        return {}


def migrate_data(batch_size=1000):
    """
    Миграция данных из card_row в таблицы cards и images с использованием LIMIT и OFFSET для пагинации.
    """
    # Загружаем данные из JSON-файлов
    sources = load_json("../output_json/sources.json")
    brands = load_json("../output_json/brands.json")
    genders = load_json("../output_json/genders.json")
    categories = load_json("../output_json/categories.json")
    subcategories = load_json("../output_json/subcategories.json")
    category_links = load_json("../output_json/category_links.json")

    if not all([sources, brands, genders, categories, subcategories, category_links]):
        print("Один или несколько JSON-файлов отсутствуют или пусты.")
        return

    # Инициализируем переменные
    offset = 0
    temp_article = None  # Для отслеживания последнего обработанного article

    while True:
        # Пагинация через LIMIT и OFFSET
        query = f"""
        SELECT guid, source, article, price_rub, brand, gender, category, subcategory, tags, title, image_url, main_photo, embedding
        FROM card_row
        WHERE source IS NOT NULL AND article IS NOT NULL
          AND brand IS NOT NULL AND gender IS NOT NULL
          AND category IS NOT NULL
          ORDER BY article
        LIMIT {batch_size} OFFSET {offset};
        """

        rows = fetch_data_from_db(DB_CONFIG_ROW, query, return_as_list=True)

        if not rows:
            print("Миграция завершена.")
            break  # Завершаем, если данных больше нет

        print(f"Обрабатывается пакет с OFFSET {offset}, размер: {len(rows)}")

        cards_data = []  # Список для накопления данных о карточках
        images_data = []  # Список для накопления данных об изображениях

        for row in rows:
            article = row[2]  # article из строки


            if article != temp_article:
                # Если новый article, обрабатываем строку как новую карточку
                card, image = process_row(row, sources, brands, genders, categories, subcategories, category_links)
                if card:
                    cards_data.append(card)
                if image:
                    images_data.append(image)

                # Обновляем temp_article на текущий
                temp_article = article
            else:
                # Если article тот же, добавляем только изображение
                _, image = process_row(row, sources, brands, genders, categories, subcategories, category_links)
                if image:
                    images_data.append(image)

        # Вставляем данные в таблицы
        if cards_data:
            print("Добавляем карту")
            insert_cards(cards_data)
        if images_data:
            print("Добавляем изображение")
            insert_images(images_data)

        # Увеличиваем offset для следующего пакета
        offset += batch_size


def process_row(row, sources, brands, genders, categories, subcategories, category_links):
    """
    Преобразует строку из card_row в данные для таблиц cards и images.

    :param row: Строка из card_row.
    :param sources: Данные sources.json.
    :param brands: Данные brands.json.
    :param genders: Данные genders.json.
    :param categories: Данные categories.json.
    :return: Кортеж (данные для cards, данные для images).
    """
    uuid, source, article, price, brand, gender, category, subcategory,  tags, title, image_url, main_photo, vector = row

    # Поиск ID
    source_id = sources.get(source)
    brand_id = brands.get(brand)
    gender_id = genders.get(gender)
    category_id = categories.get(category)
    subcategory_id = subcategories.get(subcategory)
    category_links_id = category_links.get(f"{category_id,subcategory_id}")

    # Проверка на валидность
    if not all([source_id, brand_id, gender_id, category_links_id]):
        print(f"Пропущена строка: source={source}, brand={brand}, gender={gender}, category_links={(category_id, subcategory_id)}, результаты поиска: {source_id}, {brand_id}, {gender_id}, {category_links_id}")

        return None, None

    # Данные для таблицы cards
    card = {
        "id": uuid,
        "source_id": source_id,
        "article": article,
        "price": price,
        "brand_id": brand_id,
        "gender_id": gender_id,
        "category_links_id": category_links_id,
        "tags": tags,
        "title": title
    }

    # Данные для таблицы images
    image = {
        "card_id": uuid,
        "image_url": image_url,
        "main_photo": main_photo,
        "vector": vector
    }

    return card, image

if __name__ == "__main__":

    try:
        migrate_data(batch_size=10)  # Указываем размер пакета для миграции
        print("Миграция данных успешно завершена.")
    except Exception as e:
        print(f"Ошибка при миграции данных: {e}")



