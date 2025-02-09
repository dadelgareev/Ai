import psycopg2
from psycopg2 import sql


def get_connection(db_params):
    """Создает подключение к базе данных"""
    return psycopg2.connect(**db_params)


def get_tables(cursor):
    """Получает список всех таблиц в схеме public"""
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
    """)
    return [table[0] for table in cursor.fetchall()]


def get_table_structure(cursor, table_name):
    """Получает структуру таблицы (колонки, типы данных, ограничения)"""
    # Получаем информацию о колонках с уточненным запросом для массивов
    cursor.execute("""
        SELECT 
            c.column_name,
            CASE 
                WHEN c.data_type = 'ARRAY' THEN 
                    t.element_type || '[]'
                WHEN c.data_type = 'USER-DEFINED' THEN
                    c.udt_name
                ELSE 
                    c.data_type
            END as data_type,
            c.character_maximum_length,
            c.is_nullable,
            c.column_default
        FROM information_schema.columns c
        LEFT JOIN LATERAL (
            SELECT 
                typelem::regtype as element_type
            FROM pg_catalog.pg_type
            WHERE oid = c.udt_name::regtype
        ) t ON true
        WHERE c.table_schema = 'public' 
        AND c.table_name = %s
        ORDER BY c.ordinal_position
    """, (table_name,))
    columns = cursor.fetchall()

    # Получаем информацию о первичных ключах
    cursor.execute("""
        SELECT DISTINCT c.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
            AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
        WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = %s
    """, (table_name,))
    primary_keys = list(set([pk[0] for pk in cursor.fetchall()]))

    return columns, primary_keys


def generate_create_table_sql(table_name, columns, primary_keys):
    """Генерирует SQL для создания таблицы"""
    column_definitions = []

    for col in columns:
        name, data_type, max_length, nullable, default = col

        # Формируем определение типа данных
        if max_length and not data_type.endswith('[]'):  # Не добавляем длину для массивов
            col_type = f"{data_type}({max_length})"
        else:
            col_type = data_type

        # Добавляем NULL/NOT NULL
        null_constraint = "NULL" if nullable == "YES" else "NOT NULL"

        # Добавляем DEFAULT если есть
        default_value = f"DEFAULT {default}" if default else ""

        column_definitions.append(
            f"{name} {col_type} {null_constraint} {default_value}".strip()
        )

    # Добавляем первичный ключ
    if primary_keys:
        column_definitions.append(
            f"PRIMARY KEY ({', '.join(primary_keys)})"
        )

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {','.join(column_definitions)}
    );
    """

    return create_table_sql


def migrate_schema(source_params, target_params):
    """Выполняет миграцию схемы из исходной БД в целевую"""
    try:
        # Подключаемся к исходной БД
        source_conn = get_connection(source_params)
        source_cur = source_conn.cursor()

        # Подключаемся к целевой БД
        target_conn = get_connection(target_params)
        target_cur = target_conn.cursor()

        # Получаем список всех таблиц
        tables = get_tables(source_cur)

        print(f"Найдено таблиц: {len(tables)}")

        # Для каждой таблицы
        for table_name in tables:
            try:
                print(f"Обработка таблицы: {table_name}")

                # Получаем структуру таблицы
                columns, primary_keys = get_table_structure(source_cur, table_name)

                # Генерируем и выполняем SQL для создания таблицы
                create_sql = generate_create_table_sql(table_name, columns, primary_keys)

                print(f"Выполняется SQL:\n{create_sql}")  # Добавлен вывод SQL для отладки

                target_cur.execute(create_sql)

                print(f"Таблица {table_name} успешно создана")

            except psycopg2.Error as e:
                print(f"Ошибка при создании таблицы {table_name}: {str(e)}")
                target_conn.rollback()  # Откатываем изменения для этой таблицы
                continue  # Продолжаем со следующей таблицей

        # Фиксируем изменения
        target_conn.commit()
        print("Миграция схемы успешно завершена")

    except Exception as e:
        print(f"Ошибка при миграции: {str(e)}")
        if 'target_conn' in locals():
            target_conn.rollback()

    finally:
        # Закрываем соединения
        if 'source_cur' in locals():
            source_cur.close()
        if 'source_conn' in locals():
            source_conn.close()
        if 'target_cur' in locals():
            target_cur.close()
        if 'target_conn' in locals():
            target_conn.close()

# Пример использования
if __name__ == "__main__":
    source_db = {
        "host": "89.111.172.215",
        "database": "postgres",
        "user": "postgres",
        "password": "super",
        "port": 5500
    }

    target_db = {
        "host": "193.232.55.5",
        "database": "postgres",
        "user": "postgres",
        "password": "super",
        "port": 5500
    }

    migrate_schema(source_db, target_db)