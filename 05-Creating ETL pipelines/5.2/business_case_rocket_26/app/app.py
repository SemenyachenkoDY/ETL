import streamlit as st
import pandas as pd
import json
import os
from PIL import Image

st.set_page_config(page_title="Rocket Launch Analytics", layout="wide")

DATA_DIR = "/opt/airflow/data"
JSON_FILE = f"{DATA_DIR}/launches.json"
PREDICTIONS_FILE = f"{DATA_DIR}/ml_predictions.csv"
IMAGES_DIR = f"{DATA_DIR}/images"

st.title("🚀 Аналитика космических запусков и ML распознавание")

# Секция 1: Анализ расписания (из JSON)
st.header("1. Ближайшие запуски (ETL Data)")
if os.path.exists(JSON_FILE):
    try:
        with open(JSON_FILE, "r") as f:
            launches = json.load(f).get("results", [])
    except (json.JSONDecodeError, ValueError):
        st.error("Ошибка чтения JSON. Файл обновляется или поврежден. Попробуйте обновить страницу через несколько секунд.")
        launches = []
    
    if launches:
        df_launches = pd.DataFrame([{
            "Имя миссии": l.get("name"),
            "Статус": l.get("status", {}).get("name"),
            "Окно старта": l.get("window_start"),
            "Провайдер": l.get("launch_service_provider", {}).get("name", "Unknown")
        } for l in launches])
        st.dataframe(df_launches)
        
        st.subheader("Запуски по провайдерам")
        provider_counts = df_launches["Провайдер"].value_counts()
        st.bar_chart(provider_counts)
else:
    st.warning("Файл launches.json еще не загружен. Запустите DAG в Airflow.")

st.markdown("---")

# Секция 2: Результаты ML
st.header("2. Распознавание типов ракет (ML Data)")
if os.path.exists(PREDICTIONS_FILE) and os.path.getsize(PREDICTIONS_FILE) > 1:
    try:
        df_preds = pd.read_csv(PREDICTIONS_FILE)
    except pd.errors.EmptyDataError:
        df_preds = pd.DataFrame()

    if df_preds.empty:
        st.warning("Файл ml_predictions.csv пуст. Сначала запустите DAG для скачивания изображений, затем выполните ml.ipynb.")
    else:
        st.dataframe(df_preds)
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Статистика по типам ракет")
            rocket_counts = df_preds["predicted_rocket"].value_counts()
            st.bar_chart(rocket_counts)
            
        # Галерея
        st.subheader("Галерея распознанных ракет")
        cols = st.columns(3)
        for idx, row in df_preds.iterrows():
            img_path = os.path.join(IMAGES_DIR, row['image_name'])
            if os.path.exists(img_path):
                with cols[idx % 3]:
                    img = Image.open(img_path)
                    st.image(img, caption=f"{row['predicted_rocket']} ({row['confidence']}%)", use_column_width=True)
else:
    st.info("Результаты ML еще не готовы. Выполните ноутбук ml.ipynb для генерации прогнозов.")