import psycopg2
from db.db_config import DB_CONFIG_DEV, DB_CONFIG_ROW


def get_connection(db_config):
    """Подключение к указанной БД."""
    try:
        connection = psycopg2.connect(**db_config)
        return connection
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        raise