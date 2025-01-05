import psycopg2
import json

conn_params = {
    'host': '193.232.55.5',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}


def get_unique_keys_from_tags_with_pagination(conn_params, table_name, page_size=10000):
    """
    Генератор, который постранично получает колонку 'tags' из таблицы, парсит JSON и возвращает множество уникальных ключей.

    Args:
        conn_params (dict): Параметры подключения к базе данных (host, dbname, user, password, port).
        table_name (str): Название таблицы.
        page_size (int): Количество строк на одной странице (по умолчанию 10,000).

    Yields:
        set: Множество уникальных ключей для каждой страницы.
    """
    try:
        # Подключаемся к базе данных
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                offset = 0  # Начальный сдвиг
                while True:
                    # SQL-запрос для выборки данных с пагинацией
                    query = f"SELECT tags FROM {table_name} LIMIT %s OFFSET %s;"
                    cursor.execute(query, (page_size, offset))

                    # Получаем строки текущей страницы
                    rows = cursor.fetchall()

                    # Если строк больше нет — завершаем работу
                    if not rows:
                        break

                    # Множество для хранения ключей текущей страницы
                    unique_keys = set()

                    # Обработка каждой строки
                    for row in rows:
                        tags = row[0]  # Получаем значение колонки tags

                        if tags:  # Проверяем, что значение не пустое
                            try:
                                # Если данные уже словарь, используем напрямую
                                if isinstance(tags, dict):
                                    tags_dict = tags
                                else:
                                    # Преобразуем строку в JSON
                                    tags_dict = json.loads(tags)

                                # Добавляем ключи в множество
                                unique_keys.update(tags_dict.keys())
                            except Exception as e:
                                # Пропускаем некорректные строки
                                print(f"Ошибка обработки JSON: {e}, данные: {tags}")

                    # Сделаны такие, то странички
                    print(offset)

                    # Возвращаем ключи для текущей страницы
                    yield unique_keys

                    # Переходим к следующей странице
                    offset += page_size

    except Exception as e:
        print(f"Произошла ошибка: {e}")


if __name__ == "__main__":

    keys = set()

    for key in get_unique_keys_from_tags_with_pagination(conn_params, 'products_all', 10000):
        keys.update(key)

    with open('tags.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(list(keys), ensure_ascii=False, indent=4))