import csv
import json

import psycopg2
import uuid
from psycopg2.extensions import adapt
from psycopg2.extras import Json
import os

connection_params_origin = {
    'host': '193.232.55.5',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}

def get_csv_list():
    files = os.listdir(os.getcwd())
    for file in files[:]:
        if not file.endswith('.csv'):
            files.remove(file)
    return files

def get_rows_from_csvs(csv_list, batch_size=1000):
    buffer = []  # Буфер для накопления строк
    for csv_file in csv_list:
        with open(csv_file, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                buffer.append(row)  # Добавляем строку в буфер
                if len(buffer) >= batch_size:
                    yield buffer  # Возвращаем партию строк
                    buffer = []  # Очищаем буфер
    if buffer:  # Если остались строки после итерации
        yield buffer

def insert_data_to_old_bd_from_csv(csv_list, conn_params):

    sql_query_inser = """
    INSERT INTO card_row (guid, source, article,guid_list,embedding ,price_rub, brand, category, subcategory, gender, title, tags)
    VALUES (%s,%s, %s, %s, %s, %s, %s, %s);
    """

    for rows in get_rows_from_csvs(csv_list):
        for row in rows:
            try:
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

if __name__ == '__main__':
    files = get_csv_list()
