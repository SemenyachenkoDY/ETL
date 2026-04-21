import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
import csv

# --- Конфигурационные переменные ---

DATA_DIR = "/opt/airflow/data"
IMAGES_DIR = f"{DATA_DIR}/images"
# Путь для сохранения JSON в общей директории (чтобы Streamlit видел файл)
TMP_JSON_FILE = f"{DATA_DIR}/launches.json"
STAGES_CSV_FILE = f"{DATA_DIR}/loading_stages.csv"
MAX_IMAGES = 10
API_URL = f"https://lldev.thespacedevs.com/2.3.0/launches/upcoming/?format=json&limit={MAX_IMAGES}"

# --- Настройка Retries (Задание 2 и 3) ---
# Настраиваем параметры retries для всего DAG и отдельных задач для обхода ошибок сети
default_args = {
    "owner": "airflow",
    "retries": 3, # Общее количество попыток при падении задачи
    "retry_delay": timedelta(minutes=1), # Задержка между попытками
}

# --- Определение DAG ---
dag = DAG(
    dag_id="download_rocket_launch_v14",
    description="Cleans dir, downloads JSON and images, handles retries, and generates loading stages table.",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["variant_14"]
)

# --- Определение Задач ---

# 1. ЗАДАЧА ОЧИСТКИ.Удаляем всё содержимое папки
clean_data_directory = BashOperator(
    task_id="clean_data_directory",
    bash_command=f"mkdir -p {DATA_DIR} && rm -rf {DATA_DIR}/*",
    dag=dag,
)

# 2. ЗАДАЧА СКАЧИВАНИЯ JSON: Скачиваем свежий список запусков
# Переопределяем retries для этой задачи (Задание 3)
download_launches = BashOperator(
    task_id="download_launches",
    # Используем .tmp файл и атомарный mv, чтобы другие сервисы (Streamlit) не читали недописанный файл.
    bash_command=(
        f"curl -fSL --connect-timeout 15 --max-time 120 --progress-bar "
        f"-H 'Accept: application/json' -o {TMP_JSON_FILE}.tmp '{API_URL}' && "
        f"mv {TMP_JSON_FILE}.tmp {TMP_JSON_FILE}"
    ),
    retries=5, # Увеличиваем кол-во попыток до 5, т.к. сеть может быть нестабильна
    retry_delay=timedelta(seconds=60),
    dag=dag,
)

# 3. ЗАДАЧА СКАЧИВАНИЯ КАРТИНОК. Обрабатываем JSON и загружаем фото
def _get_pictures():
    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)
    with open(TMP_JSON_FILE, encoding="utf-8") as f:
        try:
            launches = json.load(f)
        except json.JSONDecodeError as e:
            f.seek(0)
            preview = f.read(500)
            raise RuntimeError(f"Launch API returned non-JSON payload. json error: {e}. Payload preview: {preview!r}") from e

        image_urls = []
        for launch in launches.get("results", []):
            image = launch.get("image")
            if isinstance(image, dict):
                image_url = image.get("image_url")
                if image_url:
                    image_urls.append(image_url)
            elif isinstance(image, str) and image:
                image_urls.append(image)

        image_urls = list(dict.fromkeys(image_urls))[:MAX_IMAGES]
        for image_index, image_url in enumerate(image_urls, start=1):
            print(f"[{image_index}/{len(image_urls)}] Downloading: {image_url}")
            try:
                response = requests.get(image_url, timeout=30)
                response.raise_for_status()
                image_filename = image_url.split("/")[-1]
                target_file = f"{IMAGES_DIR}/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.RequestException as e:
                # Явно пробрасываем ошибку для работы Airflow Retries (Задание 2)
                print(f"Connection error while downloading {image_url}: {e}")
                raise e

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

# 4. ЗАДАЧА ФОРМИРОВАНИЯ ТАБЛИЦЫ СТАДИЙ (Задание 1)
def _generate_stages_table(**context):
    stages = [
        {"stage": "1", "name": "clean_data_directory", "description": "Очистка директории data от предыдущих запусков", "status": "Success"},
        {"stage": "2", "name": "download_launches", "description": "Скачивание JSON файла с расписанием запусков ракет через curl", "status": "Success"},
        {"stage": "3", "name": "get_pictures", "description": "Загрузка изображений ракет (используется механизм Retries для обхода ошибок сети)", "status": "Success"},
        {"stage": "4", "name": "generate_stages_table", "description": "Формирование CSV таблицы этапов выполнения ETL процесса (текущая задача)", "status": "In Progress"}
    ]
    with open(STAGES_CSV_FILE, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["stage", "name", "description", "status"])
        writer.writeheader()
        writer.writerows(stages)
    print(f"Stages table successfully written to {STAGES_CSV_FILE}")

generate_stages_table = PythonOperator(
    task_id="generate_stages_table",
    python_callable=_generate_stages_table,
    provide_context=True,
    dag=dag
)

# 5. ЗАДАЧА УВЕДОМЛЕНИЯ
notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "There are now $(ls {IMAGES_DIR}/ | wc -l) images in {IMAGES_DIR} and stages table in {STAGES_CSV_FILE}."',
    dag=dag,
)

# --- Порядок выполнения ---
clean_data_directory >> download_launches >> get_pictures >> generate_stages_table >> notify
