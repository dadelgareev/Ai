import grpc
from concurrent import futures
import os
import fileexchange_pb2
import fileexchange_pb2_grpc
import processingdatabase_pb2
import processingdatabase_pb2_grpc

class ProcessingDatabaseServer(processingdatabase_pb2_grpc.ProcessingDatabaseServicer):
    def __init__(self, output_directory="./received_files"):
        """Конструктор для инициализации сервера и директории для сохранения файлов"""
        self.output_directory = output_directory
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

    def ImportCSV(self, request_iterator, context):
        """Метод для потоковой загрузки CSV файлов"""
        file_name = None
        file_path = None

        # Процесс получения файла и его записи
        for chunk in request_iterator:
            if not file_name:
                file_name = chunk.FileName
                file_path = os.path.join(self.output_directory, file_name)
                print(f"Получаем файл: {file_name}")

            # Записываем данные чанка в файл
            with open(file_path, 'ab') as file:  # 'ab' для добавления данных
                file.write(chunk.ChunkData)
            print(f"Получено {len(chunk.ChunkData)} байт...")

        print(f"Файл {file_name} успешно получен и сохранен.")
        return fileexchange_pb2.UploadStatus(
            message="File uploaded successfully", code=fileexchange_pb2.UploadStatusCode.OK
        )

def serve():
    """Запуск gRPC сервера"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    processingdatabase_pb2_grpc.add_ProcessingDatabaseServicer_to_server(ProcessingDatabaseServer(), server)
    server.add_insecure_port("[::]:50051")
    print("Server is running on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
