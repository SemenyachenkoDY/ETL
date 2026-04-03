import os
import requests
import pandas as pd
import joblib
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
    dag_id="real_umbrella_moscow",
    default_args=default_args,
    description="Fetch Moscow weather, clean, add weekday, train ML, deploy.",
    schedule_interval="@daily",
    catchup=False
)

def fetch_weather_forecast():
    # Открытый API Open-Meteo (Москва, прогноз на 7 дней, средняя температура)
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=55.75&longitude=37.62"
        "&daily=temperature_2m_mean"
        "&timezone=Europe%2FMoscow"
        "&forecast_days=7"
    )
    
    response = requests.get(url)
    data = response.json()
    
    # Извлекаем списки дат и температур из JSON
    dates = data['daily']['time']
    temperatures = data['daily']['temperature_2m_mean']
    
    # Создаем DataFrame (имена колонок такие же, чтобы остальные функции работали)
    df = pd.DataFrame({
        'date': dates,
        'temperature': temperatures
    })
    
    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)
    df.to_csv(os.path.join(data_dir, 'weather_forecast.csv'), index=False)
    print("Weather forecast for Moscow saved via Open-Meteo (No API Key).")

def clean_weather_data():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    
    # Очистка
    df['temperature'] = df['temperature'].ffill()
    
    # Добавление столбца "день недели"
    df['date'] = pd.to_datetime(df['date'])
    days_map = {
        0: 'Понедельник', 1: 'Вторник', 2: 'Среда', 
        3: 'Четверг', 4: 'Пятница', 5: 'Суббота', 6: 'Воскресенье'
    }
    df['день недели'] = df['date'].dt.weekday.map(days_map)
    
    df.to_csv(os.path.join(data_dir, 'clean_weather.csv'), index=False)
    print("Cleaned weather data with 'день недели' saved.")

def fetch_sales_data():
    data_dir = '/opt/airflow/data'
    # Чтобы join сработал, берём даты из прогноза
    weather_df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    dates = weather_df['date'].tolist()
    
    # Моковые данные продаж
    sales = [10, 15, 20, 25, 30, 10, 5][:len(dates)]
    
    df = pd.DataFrame({'date': dates, 'sales': sales})
    df.to_csv(os.path.join(data_dir, 'sales_data.csv'), index=False)
    print("Sales data saved.")

def clean_sales_data():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'sales_data.csv'))
    df['sales'] = df['sales'].ffill()
    df.to_csv(os.path.join(data_dir, 'clean_sales.csv'), index=False)
    print("Cleaned sales data saved.")

def join_datasets():
    data_dir = '/opt/airflow/data'
    weather_df = pd.read_csv(os.path.join(data_dir, 'clean_weather.csv'))
    sales_df = pd.read_csv(os.path.join(data_dir, 'clean_sales.csv'))
    
    # Объединение
    joined_df = pd.merge(weather_df, sales_df, on='date', how='inner')
    joined_df.to_csv(os.path.join(data_dir, 'joined_data.csv'), index=False)
    print("Joined dataset saved.")

def train_ml_model():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'joined_data.csv'))
    
    X = df[['temperature']]
    y = df['sales']
    
    model = LinearRegression()
    model.fit(X, y)
    
    joblib.dump(model, os.path.join(data_dir, 'ml_model.pkl'))
    print("ML model trained and saved.")

def deploy_ml_model():
    data_dir = '/opt/airflow/data'
    model = joblib.load(os.path.join(data_dir, 'ml_model.pkl'))
    print("Model deployed successfully:", model)

# Инициализация операторов
t1 = PythonOperator(task_id="fetch_weather_forecast", python_callable=fetch_weather_forecast, dag=dag)
t2 = PythonOperator(task_id="clean_weather_data", python_callable=clean_weather_data, dag=dag)
t3 = PythonOperator(task_id="fetch_sales_data", python_callable=fetch_sales_data, dag=dag)
t4 = PythonOperator(task_id="clean_sales_data", python_callable=clean_sales_data, dag=dag)
t5 = PythonOperator(task_id="join_datasets", python_callable=join_datasets, dag=dag)
t6 = PythonOperator(task_id="train_ml_model", python_callable=train_ml_model, dag=dag)
t7 = PythonOperator(task_id="deploy_ml_model", python_callable=deploy_ml_model, dag=dag)

# Настройка зависимостей (граф)
t1 >> t2
t3 >> t4
[t2, t4] >> t5
t5 >> t6 >> t7