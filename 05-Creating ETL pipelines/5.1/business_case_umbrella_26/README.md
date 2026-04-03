# Проектный практикум по разработке ETL-решений: Лабораторная работа №5

## Постановка задачи (Вариант 14)
Разработать контейнеризированное ETL-решение на базе Apache Airflow для автоматизации пайплайна обработки данных со следующими требованиями:
- Получить прогноз погоды в **Варшаве на 5 дней** (используя внешний API).
- Обработать данные: добавить столбец **"день недели"**, отфильтровать по рабочим дням и сагрегировать.
- Сгенерировать данные о продажах за эти же даты и объединить наборы данных.
- Обучить простейшую ML-модель (Линейная регрессия).
- Вывести **таблицу средней температуры по рабочим дням** (в качестве инструмента визуализации добавлен Streamlit).

## Архитектура проекта

```mermaid
graph TD
    subgraph External Sources
        API[Open-Meteo]
    end

    subgraph Docker Infrastructure
        PG[(PostgreSQL Airflow Metadata)]
        
        subgraph Airflow Components
            SCH[Scheduler]
            WEB[Webserver :8080]
            INIT[Init-container]
        end
        
        subgraph Visualization
            STR[Streamlit App :8501]
        end
        
        VOL[Shared Volume /opt/airflow/data]
    end

    subgraph User
        BROWSER((Browser))
    end

    API -->|JSON forecast| SCH
    SCH -->|Saves CSV| VOL
    SCH <-->|Reads/Writes State| PG
    WEB <-->|Reads State| PG
    STR -->|Reads CSV| VOL
    
    BROWSER -->|Monitors DAGs| WEB
    BROWSER -->|Views Dashboard| STR
```

## Технический стек
* **Оркестрация:** Apache Airflow 2.8.1
* **Контейнеризация:** Docker, Docker Compose
* **Язык программирования:** Python 3.11
* **Библиотеки (ETL & ML):** Pandas, Scikit-learn, Joblib, Requests
* **Визуализация:** Streamlit, Matplotlib
* **База данных:** PostgreSQL 12 (для метаданных Airflow)

## Описание DAG (`variant_14_warsaw`)
Пайплайн состоит из следующих задач (Task):
1. **`fetch_weather_forecast`**: Обращается к Open-Meteo, получает прогноз для **Варшавы** на 5 дней, сохраняет в `weather_forecast.csv`.
2. **`clean_weather_data`**: Заполняет пропуски, вычисляет и **добавляет столбец "день недели"** и флаг рабочего дня, сохраняет в `clean_weather.csv`.
3. **`visualize_table`**: Фильтрует рабочие дни, группирует по дню недели (средняя температура), сохраняет и отрисовывает картинку таблицы.
4. **`fetch_sales_data`**: Читает даты из прогноза погоды и генерирует данные о продажах на эти же даты, сохраняет в `sales_data.csv`.
5. **`clean_sales_data`**: Очищает данные продаж.
6. **`join_datasets`**: Объединяет погоду и продажи по дате (Inner Join).
7. **`train_ml_model`**: Обучает линейную регрессию предсказывать продажи по температуре.
8. **`deploy_ml_model`**: Имитирует деплой (загружает сохраненную `.pkl` модель).

---

## Исходный код

Перед началом создайте следующую структуру директорий:
```text
project/
├── dags/
│   └── variant_14.py
├── app/
│   └── app.py
├── data/
├── docker-compose.yml
└── Dockerfile
```

### 1. `Dockerfile`
Добавлен `streamlit` и `matplotlib` для графиков.
```dockerfile
FROM apache/airflow:slim-2.8.1-python3.11

USER airflow

# Устанавливаем необходимые Python-библиотеки
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    joblib \
    requests \
    azure-storage-blob==12.8.1 \
    psycopg2-binary \
    streamlit \
    matplotlib \
    "connexion[swagger-ui]"

USER root

# Создаём директории и назначаем владельца
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs /opt/airflow/app

USER airflow
```

### 2. `docker-compose.yml`
Добавлен сервис `streamlit` для визуализации графиков.
```yaml
x-environment: &airflow_environment
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
  - AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
  - AIRFLOW__CORE__LOAD_EXAMPLES=False
  - AIRFLOW__CORE__STORE_DAG_CODE=True
  - AIRFLOW__CORE__STORE_SERIALIZED_DAGS=True
  - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
  - AIRFLOW__WEBSERVER__RBAC=False
  - AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey123
  - AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
  - AIRFLOW__LOGGING__REMOTE_LOGGING=False
  - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs

x-airflow-image: &airflow_image custom-airflow:slim-2.8.1-python3.11

services:
  postgres:
    image: postgres:12-alpine
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      timeout: 5s
      retries: 5

  init:
    image: *airflow_image
    depends_on:
      postgres:
        condition: service_healthy
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    entrypoint: >
      bash -c "
      airflow db upgrade &&
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.org &&
      echo 'Airflow init completed.'"
    healthcheck:
      test: ["CMD", "airflow", "db", "check"]
      interval: 10s
      retries: 5
      start_period: 10s

  webserver:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8080:8080"
    restart: always
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    command: webserver

  scheduler:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    restart: always
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    command: scheduler

  streamlit:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8501:8501"
    restart: always
    volumes:
      - ./data:/opt/airflow/data
      - ./app:/opt/airflow/app
    command: bash -c "streamlit run /opt/airflow/app/app.py --server.port=8501 --server.address=0.0.0.0"

volumes:
  logs:
  postgres_data:
```

### 3. `dags/variant_14.py`
Скорректирован для Варшавы, фильтрации рабочих дней, вычисления средних значений и генерации корректных дат для join.

```python
import os
import requests
import pandas as pd
import joblib
import matplotlib.pyplot as plt
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sklearn.linear_model import LinearRegression

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id="variant_14_warsaw",
    default_args=default_args,
    description="Variant 14: Warsaw 5 days, average by workdays, visualize table.",
    schedule_interval="@daily",
    catchup=False
)

def fetch_weather_forecast():
    # Warsaw coords: 52.2297, 21.0122 (Variant 14)
    # Using Open-Meteo API as in original template
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=52.2297&longitude=21.0122"
        "&daily=temperature_2m_mean"
        "&timezone=Europe%2FWarsaw"
        "&forecast_days=5"
    )
    
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching weather data: {response.status_code}")
        print(f"Response text: {response.text}")
        raise Exception(f"API request failed with status {response.status_code}")

    data = response.json()
    
    if 'daily' not in data:
        print(f"Unexpected API response structure: {data}")
        raise KeyError("'daily' not found in API response")

    dates = data['daily']['time']
    temperatures = data['daily']['temperature_2m_mean']
    
    df = pd.DataFrame({
        'date': dates,
        'temperature': temperatures
    })
    
    # Use relative path for portability
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    print(f"Using data directory: {data_dir}")
    
    try:
        os.makedirs(data_dir, exist_ok=True)
        print(f"Directory {data_dir} created or already exists.")
    except Exception as e:
        print(f"Error creating directory {data_dir}: {e}")
        raise e

    save_path = os.path.join(data_dir, 'weather_forecast.csv')
    df.to_csv(save_path, index=False)
    print(f"Weather forecast for Warsaw saved to {save_path}.")

def clean_weather_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    
    df['temperature'] = df['temperature'].ffill()
    
    df['date'] = pd.to_datetime(df['date'])
    days_map = {
        0: 'Понедельник', 1: 'Вторник', 2: 'Среда', 
        3: 'Четверг', 4: 'Пятница', 5: 'Суббота', 6: 'Воскресенье'
    }
    df['день недели'] = df['date'].dt.weekday.map(days_map)
    df['is_working_day'] = df['date'].dt.weekday < 5
    
    df.to_csv(os.path.join(data_dir, 'clean_weather.csv'), index=False)
    print("Cleaned weather data saved.")

def visualize_table():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'clean_weather.csv'))
    
    # Filter working days and group by weekday to get average
    working_days = df[df['is_working_day'] == True]
    grouped = working_days.groupby('день недели')['temperature'].mean().reset_index()
    grouped.rename(columns={'день недели': 'День недели', 'temperature': 'Средняя температура'}, inplace=True)
    grouped['Средняя температура'] = grouped['Средняя температура'].round(2)
    
    # Optional: Save grouped data to CSV
    grouped.to_csv(os.path.join(data_dir, 'grouped_weather.csv'), index=False)
    
    # Visualize table using matplotlib
    fig, ax = plt.subplots(figsize=(6, 3))
    ax.axis('tight')
    ax.axis('off')
    table_data = grouped.values.tolist()
    columns = grouped.columns.tolist()
    
    table = ax.table(cellText=table_data, colLabels=columns, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1.2, 1.2)
    
    plt.title('Средняя температура по рабочим дням (Варшава)', pad=20)
    plt.savefig(os.path.join(data_dir, 'workday_avg_temp.png'), bbox_inches='tight')
    plt.close()
    print("Visualization saved securely.")

def fetch_sales_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    weather_df = pd.read_csv(os.path.join(data_dir, 'clean_weather.csv'))
    dates = weather_df['date'].tolist()
    
    sales = [12, 18, 22, 28, 35, 15, 8][:len(dates)]
    
    df = pd.DataFrame({'date': dates, 'sales': sales})
    df.to_csv(os.path.join(data_dir, 'sales_data.csv'), index=False)
    print("Sales data saved.")

def clean_sales_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'sales_data.csv'))
    df['sales'] = df['sales'].ffill()
    df.to_csv(os.path.join(data_dir, 'clean_sales.csv'), index=False)

def join_datasets():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    weather_df = pd.read_csv(os.path.join(data_dir, 'clean_weather.csv'))
    sales_df = pd.read_csv(os.path.join(data_dir, 'clean_sales.csv'))
    
    joined_df = pd.merge(weather_df, sales_df, on='date', how='inner')
    joined_df.to_csv(os.path.join(data_dir, 'joined_data.csv'), index=False)

def train_ml_model():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'joined_data.csv'))
    
    X = df[['temperature']]
    y = df['sales']
    
    model = LinearRegression()
    model.fit(X, y)
    
    joblib.dump(model, os.path.join(data_dir, 'ml_model.pkl'))

def deploy_ml_model():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    model = joblib.load(os.path.join(data_dir, 'ml_model.pkl'))
    print("Model deployed successfully:", model)

t1 = PythonOperator(task_id="fetch_weather_forecast", python_callable=fetch_weather_forecast, dag=dag)
t2 = PythonOperator(task_id="clean_weather_data", python_callable=clean_weather_data, dag=dag)
t_vis = PythonOperator(task_id="visualize_table", python_callable=visualize_table, dag=dag)
t3 = PythonOperator(task_id="fetch_sales_data", python_callable=fetch_sales_data, dag=dag)
t4 = PythonOperator(task_id="clean_sales_data", python_callable=clean_sales_data, dag=dag)
t5 = PythonOperator(task_id="join_datasets", python_callable=join_datasets, dag=dag)
t6 = PythonOperator(task_id="train_ml_model", python_callable=train_ml_model, dag=dag)
t7 = PythonOperator(task_id="deploy_ml_model", python_callable=deploy_ml_model, dag=dag)

t1 >> t2 >> t_vis
t2 >> t3 >> t4
[t2, t4] >> t5
t5 >> t6 >> t7
```

### 4. `app/app.py` (Streamlit Дашборд)
Скрипт выводит таблицу средних температур.
```python
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

st.set_page_config(page_title="Прогноз погоды Варшава", layout="wide")
st.title("Анализ погоды в Варшаве на 5 дней (Вариант 14)")

# Use relative path for portability
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(base_dir, 'data', 'clean_weather.csv')

if os.path.exists(data_path):
    df = pd.read_csv(data_path)
    
    st.write("### Очищенные данные с добавленным столбцом 'день недели'")
    st.dataframe(df)

    st.write("### Визуализация таблицы: Средняя температура по рабочим дням")
    
    # Filter working days and group by weekday to get average
    working_days = df[df['is_working_day'] == True]
    grouped = working_days.groupby('день недели')['temperature'].mean().reset_index()
    grouped.rename(columns={'день недели': 'День недели', 'temperature': 'Средняя температура, °C'}, inplace=True)
    grouped['Средняя температура, °C'] = grouped['Средняя температура, °C'].round(2)
    
    st.table(grouped)
    
    fig, ax = plt.subplots(figsize=(6, 3))
    ax.axis('tight')
    ax.axis('off')
    table_data = grouped.values.tolist()
    columns = grouped.columns.tolist()
    table = ax.table(cellText=table_data, colLabels=columns, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(14)
    table.scale(1.2, 1.5)
    st.pyplot(fig)
else:
    st.warning("Данные еще не сгенерированы. Пожалуйста, запустите DAG в Airflow.")
```

---

## Ход выполнения


В  `docker-compose.yml` папка `data` пробрасывается из локальной системы внутрь контейнера (bind mount):
`- ./data:/opt/airflow/data`

Когда Docker монтирует  локальную папку `./data` в контейнер, она **перезаписывает** те права доступа, которые мы указывали в `Dockerfile` (`chown -R airflow`). 
Локальная папка принадлежит  пользователю компьютера (или root), а Airflow внутри контейнера работает от ограниченного пользователя `airflow` (обычно с UID 50000). Из-за этого у Airflow нет прав создать файл в этой папке.


```bash
sudo chown -R 50000:0 ./data
sudo chown -R dev:dev /home/dev/Downloads/practice/business_case_umbrella_25
```
### Подготовка и сборка кастомного образа
Поскольку в проекте используются дополнительные библиотеки (Pandas, Scikit-learn, Streamlit и др.), перед запуском оркестратора необходимо собрать кастомный Docker-образ из `Dockerfile`. 
Откройте терминал в корневой папке проекта (где лежат `Dockerfile` и `docker-compose.yml`) и выполните:

```bash
docker build -t custom-airflow:slim-2.8.1-python3.11 .
```
<img width="1919" height="1011" alt="image" src="https://github.com/user-attachments/assets/cabbfa71-b5c0-4ffd-bac7-b32138a1f79a" />

### Запуск проекта
После того как образ успешно собран, запустите всю инфраструктуру (PostgreSQL, Airflow Init, Webserver, Scheduler и Streamlit) в фоновом режиме:
```bash
docker compose up -d
```
<img width="1075" height="188" alt="image" src="https://github.com/user-attachments/assets/9869e841-b601-4fec-bf30-78f72085debd" />

### Проверка запущенных контейнеров
Убедитесь, что инфраструктура поднялась без ошибок. Для вывода списка активных контейнеров и их статусов используйте команду:
```bash
docker ps
```
<img width="1919" height="1007" alt="image" src="https://github.com/user-attachments/assets/9976f40d-c049-4bbe-850a-8047c4e78128" />

*(Вы должны увидеть контейнеры с именами, содержащими `postgres`, `webserver`, `scheduler`, `streamlit`. Контейнер `init` завершит работу после настройки БД).*

### Просмотр логов
Чтобы отследить процесс инициализации Airflow или диагностировать работу компонентов, посмотрите логи.
Для просмотра логов всех сервисов в реальном времени:
```bash
docker compose logs -f
```
Для просмотра логов конкретного сервиса (например, чтобы убедиться, что `init` создал пользователя):
```bash
docker compose logs init
```
<img width="1919" height="989" alt="image" src="https://github.com/user-attachments/assets/76691a53-cadd-428b-a18d-478d121445bf" />

*(Для выхода из режима потокового чтения логов нажмите `Ctrl+C`)*.

### Выполнение DAG и получение визуализации
1. **Запуск пайплайна (Airflow):** 
   * Перейдите в браузере по адресу [http://localhost:8080](http://localhost:8080).
   * Авторизуйтесь (логин: `admin`, пароль: `admin`).
   * Найдите ваш DAG в списке, снимите его с паузы (переключатель слева) и запустите вручную, нажав кнопку **Play (▶)** ➜ **Trigger DAG**.
   * Дождитесь успешного выполнения всех задач (статус поменяется на темно-зеленый "Success"). Данные скачаются, обработаются и сохранится модель.
   <img width="1848" height="956" alt="image" src="https://github.com/user-attachments/assets/55d2c1ee-ff1f-4906-83d5-8fb07168fc11" />

2. **Просмотр визуализации (Streamlit):**
   * Перейдите по адресу [http://localhost:8501](http://localhost:8501).
   * На открывшемся дашборде вы увидите очищенную таблицу данных и итоговую таблицу средних температур по рабочим дням.
<img width="1847" height="951" alt="image" src="https://github.com/user-attachments/assets/b4a194d1-f5f0-45e4-8a87-1f25e2cce0a0" />


3. Прогноз продаж
<img width="1749" height="478" alt="image" src="https://github.com/user-attachments/assets/6fbd810f-a757-4536-93f9-2d192aed3e57" />

### Выключение проекта и полная очистка ресурсов
После успешного завершения работы необходимо остановить сервисы, удалить контейнеры, очистить сеть, тома данных (volumes) и собранные образы.

1. Остановка контейнеров, удаление связанной сети и томов:
```bash
docker compose down -v
```
<img width="1916" height="1003" alt="image" src="https://github.com/user-attachments/assets/2a675a27-a2cf-45b7-9a8e-9ad65bccd6f5" />

2. Удаление кастомного Docker-образа Airflow:
```bash
docker rmi custom-airflow:slim-2.8.1-python3.11
```
<img width="1919" height="1010" alt="image" src="https://github.com/user-attachments/assets/a317da29-347e-4e53-b1ce-a7e5027daa9f" />

3. *(Опционально)* Очистка системы от зависших ("dangling") сетей и слоёв кэша сборки:
```bash
docker network prune -f
docker image prune -f
```
4. Если необходимо удалить сгенерированные файлы данных из локальной папки:
```bash
rm -rf data/*
```
<img width="1919" height="1003" alt="image" src="https://github.com/user-attachments/assets/9d269c83-800d-43a3-80b2-87f83463405a" />

