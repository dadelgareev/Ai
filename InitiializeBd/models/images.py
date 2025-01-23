from db.db_connection import get_connection, DB_CONFIG_DEV


def insert_images(images):
    """
    Вставляет данные в таблицу ecommerce.images.

    :param images: Список словарей с данными для вставки.
    """
    if not images:
        print("Нет данных для вставки в таблицу images.")
        return

    query = """
    INSERT INTO ecommerce.images (
        card_id, image_url, main_photo
    ) VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for image in images:
                cursor.execute(query, (
                    image["card_id"], image["image_url"], image["main_photo"]
                ))
            conn.commit()
        print(f"{len(images)} записей добавлены в таблицу images.")
