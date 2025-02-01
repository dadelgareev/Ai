import json
import uuid
import random
import string

from db.db_connection import DB_CONFIG_DEV, get_connection
from models.DressyAI.UserEmbeddings import insert_user_embedding
from models.Users import insert_user


def load_json(filename):
    """Загружает JSON из файла"""
    with open(filename) as json_file:
        return json.load(json_file)


def random_name():
    """Генерирует случайное имя"""
    names = ["Иван", "Петр", "Алексей", "Максим", "Дмитрий", "Сергей", "Анна", "Мария", "Елена", "Ольга"]
    surnames = ["Иванов", "Петров", "Сидоров", "Козлов", "Смирнов", "Федоров", "Васильев", "Кузнецов"]

    return f"{random.choice(names)} {random.choice(surnames)}"


def random_email():
    """Генерирует случайный email"""
    domains = ["gmail.com", "yahoo.com", "mail.ru", "yandex.ru", "outlook.com"]
    name_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    return f"{name_part}@{random.choice(domains)}"


def random_phone():
    """Генерирует случайный номер телефона"""
    return f"+7{random.randint(9000000000, 9999999999)}"


def generate_users(users_count=100):
    """Генерирует список пользователей"""
    users_data = []

    for _ in range(users_count):
        user_data = {
            "user_id": str(uuid.uuid4()),  # Генерируем уникальный UUID v4
            "user_name": random_name(),
            "user_phone": random_phone(),
            "user_gender": random.choice(["Man", "Woman"]),
            "user_email": random_email(),
            "account_id": str(uuid.uuid4()),  # Генерируем другой UUID для account_id
            "gender": random.choice(["Man", "Woman"])  # Генерируем случайный пол для пользователя
        }
        users_data.append(user_data)

    return users_data


def filter_embeddings_by_gender(gender, average_embeddings_json):
    """Фильтрует эмбеддинги по гендеру"""
    return [emd for emd in average_embeddings_json if emd["Gender"] == gender]


if __name__ == '__main__':
    # Загружаем average embeddings из JSON
    average_embeddings_json = load_json("../output_json/average_embedding.json")

    # Генерируем пользователей
    users_data = generate_users(100)

    # Вставляем пользователей в базу данных
    insert_user(users_data)

    # Список для хранения данных пользователей с эмбеддингами
    users_data_embeddings = []

    for user in users_data:
        # Фильтруем эмбеддинги по гендеру
        filtered_embeddings = filter_embeddings_by_gender(user["gender"], average_embeddings_json)

        # Создаем эмбеддинги для каждого пользователя, если для гендера есть данные
        for emdedding in filtered_embeddings:
            user_embedding = {
                "id" : str(uuid.uuid4()),
                "user_id": user["user_id"],
                "user_embedding": emdedding["AverageEmbedding"],
                "category_id": emdedding["CategoryId"],
            }
            users_data_embeddings.append(user_embedding)

    # Вставляем эмбеддинги пользователей в базу данных
    insert_user_embedding(users_data_embeddings)
