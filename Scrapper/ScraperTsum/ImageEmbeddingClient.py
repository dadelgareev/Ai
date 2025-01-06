# Импорты протобуфов
import grpc
import image_embedding_pb2
import image_embedding_pb2_grpc


class ImageEmbeddingClient:
    def __init__(self, server_address="localhost:50052"):
        # Создаем канал для подключения к серверу
        self.channel = grpc.insecure_channel(server_address)
        self.stub = image_embedding_pb2_grpc.ImageEmbeddingServiceStub(self.channel)

    def get_embedding(self, image_name, image_data):
        # Создаем запрос с изображением и его именем
        request = image_embedding_pb2.EmbeddingRequest(image_name=image_name, image_data=image_data)

        # Отправляем запрос на сервер и получаем ответ
        response = self.stub.ProcessImage(request)

        # Возвращаем эмбеддинг (строку с эмбеддингом)
        return response.embedding


def send_image_and_get_embedding(image_path):
    # Чтение изображения в байты
    with open(image_path, "rb") as image_file:
        image_data = image_file.read()

    # Создание клиента
    client = ImageEmbeddingClient()

    # Получение эмбеддинга изображения
    embedding = client.get_embedding(image_name=image_path, image_data=image_data)

    # Выводим полученный эмбеддинг
    print("Received embedding:", embedding)


if __name__ == "__main__":
    # Путь к изображению, которое нужно отправить на сервер
    image_path = "Test_resources/img.png"  # Замените на путь к изображению

    # Отправка изображения и получение эмбеддинга
    send_image_and_get_embedding(image_path)
