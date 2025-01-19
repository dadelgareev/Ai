import psycopg2
from psycopg2 import sql

# Параметры подключения
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'your_database'
DB_USER = 'your_user'
DB_PASSWORD = 'your_password'

# Параметры пагинации
PAGE_SIZE = 1000  # Количество записей на странице


# Функция для подключения к базе данных
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# Функция для получения списка дублирующихся guid
def get_duplicates(cursor):
    cursor.execute("""
        SELECT guid, COUNT(*) as duplicated_count
        FROM card_row
        GROUP BY guid
        HAVING COUNT(*) > 1
    """)
    return cursor.fetchall()


# Функция для удаления дубликатов с пагинацией
def delete_duplicates(cursor, guid):
    # Получаем ctid дубликатов для данного guid
    cursor.execute("""
        WITH numbered_rows AS (
            SELECT ctid, guid, ROW_NUMBER() OVER (PARTITION BY guid ORDER BY ctid) AS row_num
            FROM card_row
            WHERE guid = %s
        )
        DELETE FROM card_row
        WHERE ctid IN (
            SELECT ctid
            FROM numbered_rows
            WHERE row_num > 1
        )
        RETURNING ctid;
    """, (guid,))
    deleted_rows = cursor.fetchall()
    return len(deleted_rows)


def main():
    # Подключение к базе данных
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Получаем список дубликатов
        duplicates = get_duplicates(cursor)

        total_deleted = 0

        # Для каждого duplicate guid
        for guid, duplicated_count in duplicates:
            print(f"Обработка дубликатов для GUID: {guid} (Количество: {duplicated_count})")

            # Пагинация удаления: удаляем дубликаты партиями по PAGE_SIZE
            deleted_in_batch = 0
            while True:
                cursor.execute("""
                    WITH numbered_rows AS (
                        SELECT ctid, guid, ROW_NUMBER() OVER (PARTITION BY guid ORDER BY ctid) AS row_num
                        FROM card_row
                        WHERE guid = %s
                    )
                    DELETE FROM card_row
                    WHERE ctid IN (
                        SELECT ctid
                        FROM numbered_rows
                        WHERE row_num > 1
                        LIMIT %s
                    )
                    RETURNING ctid;
                """, (guid, PAGE_SIZE))

                deleted_rows = cursor.fetchall()
                if not deleted_rows:
                    break  # Выход из цикла, если больше нет записей для удаления

                deleted_in_batch += len(deleted_rows)
                total_deleted += len(deleted_rows)

                print(f"Удалено {len(deleted_rows)} дубликатов для GUID: {guid} в этой партии.")

                # Подтверждаем изменения после каждой партии
                conn.commit()

            print(f"Завершено удаление для GUID: {guid}, удалено: {deleted_in_batch} дубликатов.")

        print(f"\nИтог: Всего удалено {total_deleted} дубликатов.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
