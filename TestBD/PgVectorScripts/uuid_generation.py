# Генерация UUID из URL
import uuid

url_namespace = uuid.NAMESPACE_URL  # Стандартное пространство имен URL
image_link = "https://a.lmcdn.ru/img600x866/R/T/RTLADX170501_25337165_5_v2.jpg"
uuid_id = uuid.uuid5(url_namespace, image_link)  # UUID на основе ссылки
print(uuid_id)