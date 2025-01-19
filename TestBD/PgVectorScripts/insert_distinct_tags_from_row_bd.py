import ast
import psycopg2
from psycopg2.extras import execute_values
# Параметры подключения (для примера)
connection_params_origin = {
    'host': '193.232.55.5',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}
connection_params_destination = {
    'host': '89.111.172.215',
    'database': 'postgres',
    'port': 5432,
    'user': 'postgres',
    'password': 'm4zoM4gpHhMFGRAms056NsoBPbae6AEK'
}


def get_pagination_rows(connection_params, table_name, page_size=1000):
    """Функция для пагинации данных из базы данных."""
    # Подключение к базе данных
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    # Инициализация смещения
    offset = 0

    try:
        while True:
            # SQL-запрос с пагинацией
            query = f"SELECT tags FROM {table_name} LIMIT {page_size} OFFSET {offset};"
            cursor.execute(query)

            # Получаем текущую порцию данных
            batch = cursor.fetchall()
            if not batch:
                break  # Прерываем цикл, если данных больше нет

            # Возвращаем текущую порцию данных
            yield batch

            # Смещаем offset для следующей порции
            offset += page_size
    finally:
        # Закрытие соединения
        cursor.close()
        conn.close()

def process_tags_to_set(batch, set_tags):
    """Функция для обработки тегов и добавления уникальных пар ключ-значение в множество, игнорируя 'Артикул'."""
    for row in batch:
        try:
            tags = row[0]  # Полагаем, что тег - это строка в первой ячейке
            if tags:
                # Преобразуем строку в словарь с помощью ast.literal_eval
                try:
                    tags_dict = ast.literal_eval(tags)

                    if isinstance(tags_dict, dict):  # Проверка, что это словарь
                        for key, value in tags_dict.items():
                            # Пропускаем, если ключ "Артикул"
                            if key == "Артикул":
                                continue

                            # Формируем строку ключ-значение
                            str_full = f'{key}="{value}"'
                            set_tags.add(str_full)
                    else:
                        print(f"Ожидался словарь, но найден: {type(tags_dict)}")

                except (ValueError, SyntaxError) as e:
                    print(f"Ошибка при обработке строки в тегах: {e}")
        except Exception as e:
            print(f"Ошибка при обработке строки из базы: {e}")


def save_tags_to_file(set_tags, filename='tags_output2.txt'):
    """Функция для сохранения всех уникальных тегов в текстовый файл."""
    with open(filename, 'w', encoding='utf-8') as f:
        for tag in set_tags:
            f.write(f"{tag}\n")
    print(f"Уникальные теги сохранены в {filename}")


def get_save_tags_to_txt():
    # Множество для уникальных тегов
    unique_tags = set()

    # Итерация по всем страницам данных
    for batch in get_pagination_rows(connection_params_origin, 'card_row', page_size=100000):
        process_tags_to_set(batch, unique_tags)
        print(f"Обработано {len(batch)} строк. Уникальных тегов: {len(unique_tags)}")

    # Сохраняем все уникальные теги в файл
    save_tags_to_file(unique_tags)



def insert_tag_to_new_db(connection_params, table_name, file):
    """Функция для вставки тегов из txt файла в базу данных с пагинацией по 10000 записей."""

    # Подключение к базе данных
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    # Открытие txt файла для чтения
    with open(file, 'r', encoding='utf-8') as f:
        batch_size = 100  # Размер пачки для вставки
        current_batch = []

        for line in f:
            # Убираем лишние пробелы и символы новой строки
            line = line.strip()

            # Проверяем, если строка пустая, пропускаем её
            if not line:
                continue

            # Разделяем строку на ключ и значение
            key_value = line.split('=')
            if len(key_value) == 2:
                key = key_value[0].strip()  # Ключ
                value = key_value[1].strip()  # Значение

                # Добавляем пару (ключ, значение) в текущую партию
                current_batch.append((key, value))

            # Если партия достигла размера batch_size (10000 записей), вставляем в базу
            if len(current_batch) >= batch_size:
                try:
                    # Выполняем вставку в базу с помощью execute_values
                    execute_values(cursor, f"INSERT INTO {table_name} (tag_key, tag_value) VALUES %s", current_batch)
                    conn.commit()  # Фиксируем изменения
                    print(f"Вставлено {len(current_batch)} записей.")
                except Exception as e:
                    print(f"Ошибка при вставке данных: {e}")
                    conn.rollback()  # Откатываем изменения в случае ошибки

                # Очищаем текущую партию для следующей
                current_batch = []

        # Вставка оставшихся записей, если они есть
        if current_batch:
            try:
                execute_values(cursor, f"INSERT INTO {table_name} (tag_key, tag_value) VALUES %s", current_batch)
                conn.commit()
                print(f"Вставлено {len(current_batch)} оставшихся записей.")
            except Exception as e:
                print(f"Ошибка при вставке оставшихся данных: {e}")
                conn.rollback()

    # Закрытие соединения с базой данных
    cursor.close()
    conn.close()
    print("Все данные успешно вставлены.")

if __name__ == '__main__':
    insert_tag_to_new_db(connection_params_destination, 'card.tag', 'tags_output.txt')
