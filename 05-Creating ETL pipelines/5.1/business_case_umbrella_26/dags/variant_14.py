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
    data = response.json()
    
    dates = data['daily']['time']
    temperatures = data['daily']['temperature_2m_mean']
    
    df = pd.DataFrame({
        'date': dates,
        'temperature': temperatures
    })
    
    # Use relative path for portability
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    os.makedirs(data_dir, exist_ok=True)
    df.to_csv(os.path.join(data_dir, 'weather_forecast.csv'), index=False)
    print("Weather forecast for Warsaw saved.")

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
