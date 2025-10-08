import grpc
from concurrent import futures
import document_uploader_pb2
import document_uploader_pb2_grpc
import os

# Создаем папку для загруженных файлов, если ее нет
UPLOAD_FOLDER = 'uploaded_files'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

class DocumentUploaderServicer(document_uploader_pb2_grpc.DocumentUploaderServicer):
    """
    Реализация сервиса DocumentUploader
    """

    def UploadFile(self, request_iterator, context):
        """
    Реализация Client streaming RPC метода UploadFile.
    Принимает поток сообщений FileChunk от клиента.
    """
        file_data = b''  # Здесь будем собирать бинарное содержимое файла
        file_name = None

        # Читаем поток запросов от клиента
        for chunk in request_iterator:
            if not file_name:
                # Предполагаем, что имя файла приходит в первом чанке
                file_name = chunk.file_name
                print(f"[Сервер] Начата загрузка файла: {file_name}")

            # Добавляем содержимое чанка к общим данным
            file_data += chunk.content
            print(f"[Сервер] Получен чанк размером {len(chunk.content)} байт")

        # После того как все чанки получены, сохраняем файл
        if file_name and file_data:
            file_path = os.path.join(UPLOAD_FOLDER, file_name)
            try:
                with open(file_path, 'wb') as f:
                    f.write(file_data)
                print(f"[Сервер] Файл успешно сохранен: {file_path}")

                # Возвращаем успешный статус
                return document_uploader_pb2.UploadStatus(
                    message="Файл успешно загружен",
                    success=True,
                    file_path=file_path
                )
            except Exception as e:
                error_msg = f"Ошибка сохранения файла: {str(e)}"
                print(f"[Сервер] {error_msg}")
                return document_uploader_pb2.UploadStatus(
                    message=error_msg,
                    success=False,
                    file_path=""
                )
        else:
            error_msg = "Данные файла не получены или имя файла отсутствует."
            print(f"[Сервер] {error_msg}")
            return document_uploader_pb2.UploadStatus(
                message=error_msg,
                success=False,
                file_path=""
            )

def serve():
    # Создаем сервер gRPC с пулом потоков
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # Добавляем наш сервис к серверу
    document_uploader_pb2_grpc.add_DocumentUploaderServicer_to_server(
        DocumentUploaderServicer(), server
    )
    # Слушаем порт 50051
    server.add_insecure_port('[::]:50051')
    # Запускаем сервер
    server.start()
    print("Сервер DocumentUploader запущен на порту 50051...")
    # Ожидаем завершения работы
    server.wait_for_termination()

if __name__ == '__main__':
    serve()