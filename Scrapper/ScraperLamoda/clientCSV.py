import grpc
import os
import fileexchange_pb2
import processingdatabase_pb2
import processingdatabase_pb2_grpc

class CsvUploaderClient:
    def __init__(self, server_address="localhost:50051"):
        """Инициализация gRPC канала и стуба для сервера"""
        self.channel = grpc.insecure_channel(server_address)
        self.stub = processingdatabase_pb2_grpc.ProcessingDatabaseStub(self.channel)

    def upload_csv(self, csv_file_path):
        """Отправляет CSV файл на сервер с использованием потоковой передачи"""
        if not os.path.exists(csv_file_path):
            print(f"Ошибка: Файл {csv_file_path} не существует.")
            return

        # Открытие файла и отправка его чанков
        with open(csv_file_path, "rb") as file:
            file_name = os.path.basename(csv_file_path)
            chunk_size = 1024 * 1024  # Размер чанка в байтах (1 MB)

            # Генерация чанков для передачи
            def generate_chunks():
                while True:
                    chunk = file.read(chunk_size)
                    if not chunk:
                        break
                    yield fileexchange_pb2.FileData(
                        FileName=file_name,
                        ChunkData=chunk
                    )

            # Отправка чанков на сервер с использованием метода ImportCSV
            try:
                response = self.stub.ImportCSV(generate_chunks())
                print(f"Ответ от сервера: {response.message} (код: {response.code})")
            except grpc.RpcError as e:
                print(f"Ошибка при вызове gRPC: {e}")

# Пример использования
if __name__ == "__main__":
    client = CsvUploaderClient(server_address="localhost:50051")
    client.upload_csv("123.csv")  # Замените на путь к вашему CSV файлу
