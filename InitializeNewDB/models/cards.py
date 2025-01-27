from db.db_connection import get_connection, DB_CONFIG_DEV


def insert_cards(cards):
    """
    Вставляет данные в таблицу ecommerce.cards.

    :param cards: Список словарей с данными для вставки.
    """
    if not cards:
        print("Нет данных для вставки в таблицу cards.")
        return

    query = """
    INSERT INTO ecommerce.cards (
        id, source_id, article, price, brand_id, gender_id, category_link_id, tags, title
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """



    with get_connection(DB_CONFIG_DEV) as conn:
        with conn.cursor() as cursor:
            for card in cards:
                if card["title"] is None:
                    card["title"] = "default"
                print(f"Попытка вставить карту: {card}")
                cursor.execute(query, (
                    card["id"], card["source_id"], card["article"], card["price"],
                    card["brand_id"], card["gender_id"], card["category_links_id"],
                    card["tags"], card["title"]
                ))
            conn.commit()
        print(f"{len(cards)} записей добавлены в таблицу cards.")
