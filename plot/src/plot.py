import time
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Создаем директорию logs, если она не существует
Path("logs").mkdir(exist_ok=True)

# Имя файла для записи метрик
csv_file = "logs/metric_log.csv"

# Имя файла для сохранения графика
png_file = "logs/error_distribution.png"


def plot_error_distribution():
    try:
        # Читаем данные из CSV файла
        df = pd.read_csv(csv_file)

        # Очищаем предыдущий график
        plt.clf()

        # Создаем фигуру
        plt.figure(figsize=(10, 6))

        # Строим гистограмму ошибок используя только seaborn
        sns.histplot(data=df, x="absolute_error", kde=True, color="skyblue", bins=30)

        plt.title("Distribution of Absolute Errors")
        plt.xlabel("Absolute Error")
        plt.ylabel("Count")
        plt.grid(True, alpha=0.3)

        # Добавляем статистическую информацию
        mean_error = df["absolute_error"].mean()
        median_error = df["absolute_error"].median()
        plt.axvline(mean_error, color="red", linestyle="dashed", linewidth=1)
        plt.axvline(median_error, color="green", linestyle="dashed", linewidth=1)

        # Добавляем легенду со статистикой
        plt.legend([f"Mean: {mean_error:.2f}", f"Median: {median_error:.2f}"])

        # Сохраняем график
        plt.savefig(png_file)
        plt.close()

        print(
            f"График обновлен. Среднее значение ошибки: {mean_error:.2f}, "
            f"Медиана ошибки: {median_error:.2f}"
        )

    except FileNotFoundError:
        print("Файл metric_log.csv пока не создан")
    except pd.errors.EmptyDataError:
        print("Файл metric_log.csv пуст")
    except Exception as e:
        print(f"Ошибка при построении графика: {str(e)}")


def main():
    print("Запуск сервиса построения графиков...")

    while True:
        try:
            plot_error_distribution()

        except KeyboardInterrupt:
            print("\nПолучен сигнал завершения работы")
            break

        except Exception as e:
            print(f"Неожиданная ошибка: {str(e)}")

        # Ждем 10 секунд перед следующим обновлением
        time.sleep(10)


if __name__ == "__main__":
    main()
