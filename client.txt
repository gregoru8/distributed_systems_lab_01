import grpc
import document_uploader_pb2
import document_uploader_pb2_grpc

# Размер чанка в байтах (например, 1 КБ)
CHUNK_SIZE = 1024

def generate_chunks(file_path, file_name):
    """Генератор, который читает файл и выдает чанки для отправки."""
    # Первый чанк: отправляем имя файла
    yield document_uploader_pb2.FileChunk(file_name=file_name)
    # Последующие чанки: отправляем содержимое файла
    with open(file_path, 'rb') as f:
        while True:
            chunk_data = f.read(CHUNK_SIZE)
            if not chunk_data:
                break  # Файл прочитан полностью
            yield document_uploader_pb2.FileChunk(content=chunk_data, chunk_size=len(chunk_data))

def run():
    # Устанавливаем соединение с сервером
    with grpc.insecure_channel('localhost:50051') as channel:
        # Создаем "заглушку" (stub) для вызова удаленных методов
        stub = document_uploader_pb2_grpc.DocumentUploaderStub(channel)

        # Путь к файлу, который хотим загрузить
        file_to_upload = 'venv/test_file.txt'  # Создайте заранее небольшой текстовый файл для теста
        file_name_on_server = 'uploaded_test_file.txt'

        print(f"[Клиент] Начата загрузка файла '{file_to_upload}' на сервер как '{file_name_on_server}'...")

        # Вызываем удаленный метод UploadFile.
        # Передаем ему генератор чанков.
        try:
            response = stub.UploadFile(generate_chunks(file_to_upload, file_name_on_server))
            if response.success:
                print(f"[Клиент] УСПЕХ: {response.message}")
                print(f"[Клиент] Файл сохранен по пути: {response.file_path}")
            else:
                print(f"[Клиент] ОШИБКА: {response.message}")
        except grpc.RpcError as e:
            print(f"[Клиент] Ошибка RPC: {e.code()} -> {e.details()}")

if __name__ == '__main__':
    run()