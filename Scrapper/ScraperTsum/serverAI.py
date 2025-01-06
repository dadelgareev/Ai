import grpc
from concurrent import futures
from transformers import AutoImageProcessor, AutoModelForImageClassification
from PIL import Image
import torch
import io
import image_embedding_pb2
import image_embedding_pb2_grpc


class ImageEmbeddingProcessor:
    def __init__(self, model_name="google/vit-base-patch16-224"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.processor = AutoImageProcessor.from_pretrained(model_name, use_fast=True)
        self.model = AutoModelForImageClassification.from_pretrained(model_name).to(self.device)

    def get_embedding(self, img_bytes):
        try:
            img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
            inputs = self.processor(images=img, return_tensors="pt").to(self.device)

            with torch.no_grad():
                outputs = self.model(**inputs)
            embeddings = outputs.logits
            return embeddings.squeeze().tolist()
        except Exception as e:
            print(f"Error processing image: {e}")
            return None


class ImageEmbeddingService(image_embedding_pb2_grpc.ImageEmbeddingServiceServicer):
    def __init__(self):
        self.processor = ImageEmbeddingProcessor()

    def ProcessImage(self, request, context):
        print(f"Received image: {request.image_name}")

        # Извлечение эмбеддингов
        embedding = self.processor.get_embedding(request.image_data)
        if embedding is None:
            return image_embedding_pb2.EmbeddingResponse(
                embedding="Error processing image"
            )

        # Преобразуем список эмбеддингов в строку
        embedding_str = ",".join(map(str, embedding))
        return image_embedding_pb2.EmbeddingResponse(embedding=embedding_str)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    image_embedding_pb2_grpc.add_ImageEmbeddingServiceServicer_to_server(
        ImageEmbeddingService(), server
    )

    server.add_insecure_port("[::]:50052")
    print("Server is running on port 50052...")

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
