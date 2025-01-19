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
            query = f"SELECT brand FROM {table_name} LIMIT {page_size} OFFSET {offset};"
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

def get_existing_brands(connection_params, table_name):
    """
    Извлекает список существующих брендов из таблицы card.brand.
    """
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT name FROM {table_name}")
        brands = {row[0] for row in cursor.fetchall()}  # Сохраняем бренды в множество для быстрого поиска
        return brands
    except Exception as e:
        print(f"Ошибка при извлечении брендов: {e}")
        return set()
    finally:
        cursor.close()
        conn.close()

def process_tags_to_set(batch, set_new_brands, set_old_brands):
    """Обрабатывает бренды и добавляет только уникальные."""
    for row in batch:
        try:
            brand = row[0]  # Извлекаем значение из кортежа
            if brand not in set_old_brands:
                set_new_brands.add(brand)
        except Exception as e:
            print(f"Ошибка при обработке строки из базы: {e}")

    print(f"Обработано {len(batch)} строк")



def save_tags_to_file(set_tags, filename='brand_output.txt'):
    """Сохраняет уникальные бренды в текстовый файл."""
    with open(filename, 'w', encoding='utf-8') as f:
        for tag in set_tags:
            f.write(f"{tag}\n")  # Пишем строку напрямую
    print(f"Уникальные бренды сохранены в {filename}")

def insert_brand_to_new_db(connection_params, table_name, file):
    """Функция для вставки брендов из txt файла в базу данных с пагинацией по 10000 записей."""

    # Подключение к базе данных
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    # Открытие txt файла для чтения
    with open(file, 'r', encoding='utf-8') as f:
        batch_size = 100  # Размер партии для вставки
        current_batch = []

        for line in f:
            # Убираем лишние пробелы и символы новой строки
            line = line.strip()

            # Пропускаем пустые строки
            if not line:
                continue

            # Добавляем строку как кортеж в текущую партию
            current_batch.append((line,))  # Преобразуем строку в кортеж

            # Если партия достигла размера batch_size, выполняем вставку
            if len(current_batch) >= batch_size:
                try:
                    # Выполняем вставку в базу с помощью execute_values
                    execute_values(cursor, f"INSERT INTO {table_name} (name) VALUES %s", current_batch)
                    conn.commit()  # Фиксируем изменения
                    print(f"Вставлено {len(current_batch)} записей.")
                except Exception as e:
                    print(f"Ошибка при вставке данных: {e}")
                    conn.rollback()  # Откат изменений в случае ошибки
                    print(f"Ошибочные записи: {current_batch}")

                # Очищаем текущую партию
                current_batch = []

        # Вставка оставшихся записей, если они есть
        if current_batch:
            try:
                execute_values(cursor, f"INSERT INTO {table_name} (name) VALUES %s", current_batch)
                conn.commit()
                print(f"Вставлено {len(current_batch)} оставшихся записей.")
            except Exception as e:
                print(f"Ошибка при вставке оставшихся данных: {e}")
                conn.rollback()
                print(f"Ошибочные записи: {current_batch}")

    # Закрытие соединения с базой данных
    cursor.close()
    conn.close()
    print("Все данные успешно вставлены.")


if __name__ == "__main__":
    """
    set_new_brands = set()
    set_old_brands = get_existing_brands(connection_params_destination, table_name='card.brand')

    print(set_old_brands)

    for rows in get_pagination_rows(connection_params_origin, table_name='card_row', page_size=100000):
        process_tags_to_set(rows, set_new_brands, set_old_brands)
    save_tags_to_file(set_new_brands)
    """
    print("Yes")
    insert_brand_to_new_db(connection_params_destination, table_name='card.brand', file='brand_output.txt')