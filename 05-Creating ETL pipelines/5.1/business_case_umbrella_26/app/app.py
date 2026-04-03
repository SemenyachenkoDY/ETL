import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

st.set_page_config(page_title="Прогноз погоды Варшава", layout="wide")
st.title("Анализ погоды в Варшаве на 5 дней (Вариант 14)")

data_path = '/opt/airflow/data/clean_weather.csv'

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