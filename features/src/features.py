import json
import time
from datetime import datetime

import numpy as np
import pika
from sklearn.datasets import load_diabetes


def load_data():
    return load_diabetes(return_X_y=True)


def generate_message(X, y):
    random_row = np.random.randint(0, X.shape[0] - 1)
    message_id = datetime.timestamp(datetime.now())

    message_y_true = {"id": message_id, "body": y[random_row]}
    message_features = {"id": message_id, "body": list(X[random_row])}

    return message_id, message_y_true, message_features


def publish_messages(channel, message_id, message_y_true, message_features):
    # Публикуем сообщение в очередь y_true
    channel.basic_publish(
        exchange="", routing_key="y_true", body=json.dumps(message_y_true)
    )
    print(f"Сообщение с правильным ответом отправлено в очередь (id: {message_id})")

    # Публикуем сообщение в очередь features
    channel.basic_publish(
        exchange="", routing_key="features", body=json.dumps(message_features)
    )
    print(f"Сообщение с вектором признаков отправлено в очередь (id: {message_id})")


def main():
    # Загружаем данные один раз при запуске
    X, y = load_data()

    while True:
        try:
            # Создаём подключение по адресу rabbitmq:
            connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
            channel = connection.channel()

            # Создаём очереди
            channel.queue_declare(queue="y_true")
            channel.queue_declare(queue="features")

            # Генерируем сообщения
            message_id, message_y_true, message_features = generate_message(X, y)

            # Публикуем сообщения
            publish_messages(channel, message_id, message_y_true, message_features)

            # Закрываем подключение
            connection.close()

            # Добавляем задержку в 10 секунд
            time.sleep(10)

        except KeyboardInterrupt:
            print("\nПолучен сигнал завершения работы")
            if "connection" in locals() and connection.is_open:
                connection.close()
            break

        except Exception as e:
            print(f"Не удалось подключиться к очереди: {str(e)}")
            # Добавляем задержку перед повторной попыткой
            time.sleep(5)


if __name__ == "__main__":
    main()
