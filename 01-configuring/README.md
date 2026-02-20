# Лабораторная работа №1. Установка и настройка ETL-инструмента. Создание конвейеров данных

**Цель работы.** Изучение основных принципов работы с ETL-инструментами на примере Pentaho Data Integration (PDI), настройка среды, создание конвейера обработки данных (фильтрация, очистка, замена значений) и выгрузка результатов в базу данных MySQL.

# Описание данных

# Ход работы

## Подготовка окружения

**Шаг 1. Установка Java и зависимостей**
<img width="961" height="1028" alt="image" src="https://github.com/user-attachments/assets/6cd9cee1-9a0e-4772-aa40-628353266a5f" />

**Шаг 2. Установка драйвера MySQL**
<img width="871" height="809" alt="image" src="https://github.com/user-attachments/assets/9ab4f2a5-23da-4243-8e04-f456ffb3b086" />

<img width="954" height="1032" alt="image" src="https://github.com/user-attachments/assets/1a0698de-fd82-4f0a-91f8-cc6556ab0be3" />

<img width="960" height="1003" alt="image" src="https://github.com/user-attachments/assets/3c100ba2-f473-441b-b15a-699dcc440ed9" />

**Шаг 3. Запуск Pentaho Spoon**
<img width="870" height="625" alt="image" src="https://github.com/user-attachments/assets/e378e801-b0f9-46ef-937a-ad13162b7cd0" />


# Задание на лабораторную работу
### Общая задача
1.  Выбрать вариант задания из таблицы ниже.
2.  Скачать CSV-датасет (если ссылка Kaggle недоступна — использовать VPN, найти зеркало, сгенерировать синтетические данные или использовать анонимизированные рабочие данные).
3.  Скачать шаблоны конвейеров для примера: [GitHub Repository](https://github.com/BosenkoTM/workshop-on-ETL/tree/main/lectures/L_01).
4.  Создать трансформацию (`.ktr`), реализующую:
    *   **CSV File Input.** Чтение данных.
    *   **Filter Rows / Value Mapper / String Operations.** Очистка данных, фильтрация битых записей, замена значений.
    *   **Table Output.** Загрузка очищенных данных в таблицу MySQL в базе `mgpu_ico_etl_XX`.
5.  Проверить результат SQL-запросом через phpMyAdmin.
### Варианты заданий
| 14 | **Биржа:** обработка данных торгов. | [Stock Market Dataset](https://www.kaggle.com/datasets/borismarjanovic/price-

# ВЫполнение лаборатоной работы
## трансформация кучи txt в csv
```python
import os
import pandas as pd
from tqdm import tqdm
import kagglehub
path = kagglehub.dataset_download(
    "borismarjanovic/price-volume-data-for-all-us-stocks-etfs"
)

print("Папка с данными:", path)

data_path = os.path.join(path, "Data")

all_txt_files = []
for root, dirs, files in os.walk(data_path):
    for file in files:
        if file.lower().endswith(".txt"):
            all_txt_files.append(os.path.join(root, file))

print(f"Найдено {len(all_txt_files)} файлов")

if len(all_txt_files) == 0:
    raise ValueError("TXT файлы не найдены.")
output_file = os.path.join(path, "combined_all_stocks.csv")

first_file = True

for file_path in tqdm(all_txt_files, desc="Объединяем файлы"):

    try:
        # ticker: убираем .us.txt
        filename = os.path.basename(file_path)
        ticker = filename.replace(".us.txt", "").upper()

        df = pd.read_csv(file_path)

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        df["ticker"] = ticker

        df.to_csv(
            output_file,
            mode='w' if first_file else 'a',
            header=first_file,
            index=False
        )

        first_file = False

    except Exception as e:
        print(f"Ошибка при обработке {file_path}: {e}")

print("Готово.")
print("Итоговый файл:", output_file)
```
**Пример работы кода:**
<img width="1568" height="938" alt="image" src="https://github.com/user-attachments/assets/2c9a7bd3-104b-410c-87cc-f8db39bab55d" />

## Созданный конвейер в Spoon (общий вид)
ZVjSoVl9	
## Настройки ключевых шагов (Input, Filter, Output)

## SQL-запросы, использованные для проверки загрузки данных, и скриншот результата SELECT из phpMyAdmin

## Файлы:
