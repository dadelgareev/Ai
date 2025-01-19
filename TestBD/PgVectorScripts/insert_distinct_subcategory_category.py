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

def get_names_from_microcategory(connection_params, table_microcategory):
    """
    Извлекает все значения `name` из таблицы card.microcategory2.
    :param connection_params: Параметры подключения к базе данных.
    :param table_microcategory: Название таблицы card.microcategory2.
    :return: Множество значений `name`.
    """
    query = f"SELECT name FROM {table_microcategory};"

    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        cursor.execute(query)
        names = {row[0] for row in cursor.fetchall()}

        cursor.close()
        conn.close()

        return names

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при извлечении данных из {table_microcategory}: {error}")
        return set()


def get_subcategories_from_card_row(connection_params, table_card_row):
    """
    Извлекает все уникальные значения `subcategory` из таблицы card_row.
    :param connection_params: Параметры подключения к базе данных.
    :param table_card_row: Название таблицы card_row.
    :return: Множество уникальных значений `subcategory`.
    """
    query = f"SELECT DISTINCT subcategory FROM {table_card_row} WHERE subcategory IS NOT NULL;"

    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        cursor.execute(query)
        subcategories = {row[0] for row in cursor.fetchall()}

        cursor.close()
        conn.close()

        return subcategories

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при извлечении данных из {table_card_row}: {error}")
        return set()


def find_missing_names(connection_params_microcategory, connection_params_card_row, table_microcategory, table_card_row):
    """
    Находит значения `name` из card.microcategory2, которых нет в `subcategory` таблицы card_row.
    :param connection_params_microcategory: Параметры подключения к базе с card.microcategory2.
    :param connection_params_card_row: Параметры подключения к базе с card_row.
    :param table_microcategory: Название таблицы card.microcategory2.
    :param table_card_row: Название таблицы card_row.
    :return: Список значений `name`, которых нет в `subcategory`.
    """
    # Получаем значения из двух баз
    names = get_names_from_microcategory(connection_params_microcategory, table_microcategory)
    subcategories = get_subcategories_from_card_row(connection_params_card_row, table_card_row)

    # Ищем недостающие значения
    missing_names = subcategories - names

    return missing_names


# Пример использования
if __name__ == "__main__":


    table_microcategory = 'card.microcategory2'
    table_card_row = 'card_row'

    missing_names = find_missing_names(
        connection_params_destination,
        connection_params_origin,
        table_microcategory,
        table_card_row
    )

    if missing_names:
        print(f"Найдены следующие значения name, отсутствующие в subcategory: {missing_names}")
    else:
        print("Все значения name из card.microcategory2 присутствуют в subcategory таблицы card_row.")