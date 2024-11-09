import csv
import json
import os
import time
from collections import defaultdict

import pika

# Словарь для хранения полученных значений
pending_values = defaultdict(dict)

# Путь создания файлов в CSV
os.makedirs("logs", exist_ok=True)

# Имя файла для записи метрик
csv_file = "logs/metric_log.csv"


# Функция для записи метрик в CSV
def write_to_csv(message_id, y_true, y_pred):
    # Вычисляем абсолютную ошибку
    abs_error = abs(y_true - y_pred)

    # Проверяем, существует ли файл
    file_exists = os.path.isfile(csv_file)

    # Открываем файл в режиме добавления
    with open(csv_file, "a", newline="") as csvfile:
        fieldnames = ["message_id", "y_true", "y_pred", "absolute_error"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Записываем заголовки, если файл новый
        if not file_exists:
            writer.writeheader()

        # Записываем данные
        writer.writerow(
            {
                "message_id": message_id,
                "y_true": y_true,
                "y_pred": y_pred,
                "absolute_error": abs_error,
            }
        )


# Создаём функцию callback для обработки данных из очереди
def callback(ch, method, properties, body):
    message = json.loads(body)
    message_id = message["id"]
    value = message["body"]

    print(
        f"Из очереди {method.routing_key} получено сообщение (id: {message_id}, значение: {value})"
    )

    # Сохраняем значение в соответствующий ключ
    if method.routing_key == "y_true":
        pending_values[message_id]["true"] = value
    else:  # y_pred
        pending_values[message_id]["pred"] = value

    # Если для данного id получены оба значения, записываем их в CSV
    if "true" in pending_values[message_id] and "pred" in pending_values[message_id]:
        y_true = pending_values[message_id]["true"]
        y_pred = pending_values[message_id]["pred"]

        # Записываем метрики в CSV
        write_to_csv(message_id, y_true, y_pred)
        print(
            f"Записаны метрики для сообщения {message_id}: true={y_true}, pred={y_pred}, "
            f"error={abs(y_true - y_pred)}"
        )

        # Удаляем обработанные значения из словаря
        del pending_values[message_id]


def main():
    while True:
        try:
            # Создаём подключение к серверу на локальном хосте
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            channel = connection.channel()

            # Объявляем очереди
            channel.queue_declare(queue="y_true")
            channel.queue_declare(queue="y_pred")

            # Извлекаем сообщения из очередей
            channel.basic_consume(
                queue="y_true", on_message_callback=callback, auto_ack=True
            )
            channel.basic_consume(
                queue="y_pred", on_message_callback=callback, auto_ack=True
            )

            print("...Ожидание сообщений, для выхода нажмите CTRL+C")
            channel.start_consuming()

        except KeyboardInterrupt:
            print("Получен сигнал завершения работы")
            if "connection" in locals() and connection.is_open:
                connection.close()
            break

        except Exception as e:
            print(f"Не удалось подключиться к очереди: {str(e)}")
            print("Повторная попытка через 5 секунд...")
            time.sleep(5)
            continue


if __name__ == "__main__":
    main()
