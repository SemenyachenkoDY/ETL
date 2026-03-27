# Лабораторная работа №4. Анализ и обработка больших данных с Dask (ETL-пайплайн)

**Вариант 14**: `Parking_Violations_Issued_-_Fiscal_Year_2015.csv` (2.8 ГБ)

**Цель работы:** изучить инструменты Dask для обработки Big Data, освоить построение ETL-пайплайнов с «ленивыми вычислениями» и визуализировать графы выполнения задач (DAG).

## Шаг 1. Extract (Извлечение данных)

Для работы с файлом объемом 2.8 ГБ (около 11 млн строк) используется `dask.dataframe`. Настраиваем локальный кластер для параллельной обработки.

```python
import dask.dataframe as dd
from dask.distributed import Client
from dask.diagnostics import ProgressBar

# Инициализация клиента Dask (2 воркера, 2 потока на воркер)
client = Client(n_workers=2, threads_per_worker=2, processes=True)

# Чтение данных с указанием типов для оптимизации
dtypes = {
    'Issuer Command': 'object', 'Issuer Squad': 'object',
    'House Number': 'object', 'Time First Observed': 'object',
    'Violation Description': 'object', 'Violation Legal Code': 'object',
    'Violation Post Code': 'object', 'Unregistered Vehicle?': 'float64',
    'Violation Location': 'float64', 'Date First Observed': 'object',
    'Feet From Curb': 'float64', 'Law Section': 'object',
    'Vehicle Year': 'float64', 'Meter Number': 'object',
    'Violation County': 'object',
    'Double Parking Violation': 'object',
    'Hydrant Violation': 'object',
    'No Standing or Stopping Violation': 'object'
}

df = dd.read_csv('Parking_Violations_Issued_-_Fiscal_Year_2015.csv', dtype=dtypes, low_memory=False)
```

## Шаг 2. Transform (Трансформация и очистка данных)

Проведено профилирование качества данных (подсчет пропусков). Столбцы с пропуском более 55% удаляются. Также удаляются технические столбцы, не несущие смысловой нагрузки для анализа.

```python
# Вычисление процента пропусков
missing_values = df.isnull().sum()
mysize = df.index.size
missing_count_percent = ((missing_values / mysize) * 100).compute()

# Удаление разреженных столбцов (>55% пропусков)
columns_to_drop = list(missing_count_percent[missing_count_percent > 55].index)
df_dropped = df.drop(columns=columns_to_drop)

# Удаление избыточных технических столбцов
additional_columns = ['Street Code1', 'Street Code2', 'Street Code3', 'Issuer Code', 'Feet From Curb', 'Violation Post Code']
df_final = df_dropped.drop(columns=[c for c in additional_columns if c in df_dropped.columns])
```

**Результат очистки:**
Удалены столбцы с максимальным количеством пропусков: `NTA`, `BBL`, `BIN`, `Latitude`, `Longitude` и др.

## Шаг 3. Load (Загрузка / Сохранение результатов)

Очищенный датасет сохраняется в формате Parquet, который является стандартом де-факто для больших данных благодаря колоночному хранению и высокой скорости чтения/записи в Dask.

```python
df_final.to_parquet('cleaned_violations_2015.parquet', engine='pyarrow')
```

## Визуализация DAG

### 1. Простой граф (Аналитика марок ТС)

Граф визуализирует процесс подсчета общего числа строк, уникальных марок автомобилей и вычисление среднего значения нарушений на марку.

```python
from dask import delayed

def get_total(dataframe): return len(dataframe)
def get_unique(dataframe): return dataframe['Vehicle Make'].nunique().compute()
def get_avg(total, unique): return round(total / unique, 2)

x = delayed(get_total)(df_final)
y = delayed(get_unique)(df_final)
z = delayed(get_avg)(x, y)

z.visualize(filename='simple_graph.png')
```

### 2. Сложный граф (Анализ по районам NYC)

Построение многоуровневого графа для анализа доли нарушений в часы пик (8:00 - 10:00) в разрезе округов (Violation County).

```python
districts = ['NY', 'K', 'Q', 'BX', 'R']

layer1 = [delayed(load_district_data)(d, df_final) for d in districts] # Фильтрация
layer2 = [delayed(count_violations)(d) for d in layer1]                # Всего в районе
layer3 = [delayed(count_peak_hours)(d) for d in layer1]               # В часы пик
layer4 = [delayed(calculate_p)(t, p) for t, p in zip(layer2, layer3)]   # Процент %

final_results = delayed(list)(layer4)
final_results.visualize(filename='complex_graph.png')
```

## Контрольные вопросы

1. **Главное отличие Dask от Pandas?**
   Dask распределяет данные по партициям и вычисляет их параллельно, позволяя работать с файлами больше объема RAM. Pandas работает в одном потоке и только «в памяти».
2. **Что такое ленивые вычисления и зачем нужен .compute()?**
   Ленивые вычисления — это накопление плана действий без их немедленного выполнения. `.compute()` — это команда на запуск процесса выполнения накопленного графа. Это позволяет оптимизировать запросы перед началом тяжёлых операций.
3. **Что такое DAG и его роль?**
   DAG (Directed Acyclic Graph) — направленный ациклический граф задач. Он описывает зависимости: что должно быть вычислено сначала, а что можно делать параллельно. Это основа для эффективной работы планировщика Dask.

---

**Итоговый результат:** Реализован полный ETL-пайплайн для датасета 2015 года, построены графы аналитики и сохранен очищенный набор данных.
