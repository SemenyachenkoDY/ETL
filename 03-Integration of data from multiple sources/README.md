# Лабораторная работа №3. Интеграция данных из нескольких источников. Обработка и согласование данных из разных источников

**Цель работы.** Разработать комплексное ETL-решение для интеграции данных из локальной СУБД PostgreSQL и файловых источников (CSV/Excel) в целевое хранилище MySQL. Спроектировать верхнеуровневую архитектуру аналитического решения.

## Вариант 14
Комплексный анализ успеваемости. Найти корреляцию между посещаемостью и средним баллом студента.

Образование.
PostgreSQL: Студенты.
Excel: Оценки.
CSV: Посещаемость занятий.

# Ход работы

## Шаг 1.  Архитектура решения
<img width="979" height="714" alt="image" src="https://github.com/user-attachments/assets/2d0e7d71-b328-41e1-b7d8-d961453e80cd" />

## Шаг 2. Создание таблицы и её заполнение в PostgreSQL:

### Создание таблицы: 
<img width="1919" height="1010" alt="image" src="Files/Снимок%20экрана%202026-03-24%20104139.png" />

### Заполнение таблицы
<img width="1919" height="1010" alt="image" src="Files/Снимок%20экрана%202026-03-24%20104220.png" />

### Вид таблицы
<img width="1919" height="1010" alt="image" src="Files/Снимок%20экрана%202026-03-24%20104301.png" />


## Шаг 3. Разработка трансформации в Pentaho (Spoon)

ОБщий вид, трансформации:
<img width="1807" height="787" alt="image" src="https://github.com/user-attachments/assets/75e96d12-3e8b-4e98-9ec9-1b15c960bc44" />

### Настройка основных узлов:

Подключение PostgreSQL:
<img width="1856" height="1041" alt="image" src="https://github.com/user-attachments/assets/c4ad9f28-d25f-4d08-a988-6c4342c5414d" />

Подкючение Excel файла:
<img width="1627" height="893" alt="image" src="https://github.com/user-attachments/assets/6f5269d2-5836-41dc-8c43-a68907b64f20" />

Подключение CSV файла:
<img width="1629" height="902" alt="image" src="https://github.com/user-attachments/assets/63423677-2cb6-40a9-a7b7-00d85e77e034" />

Фильтрация:
<img width="1082" height="722" alt="image" src="https://github.com/user-attachments/assets/5d99b0fa-a696-4364-929b-e9cc17672545" />

<img width="1090" height="718" alt="image" src="https://github.com/user-attachments/assets/b08d5c07-8a18-4111-b213-6b15381e095f" />


Калькулятор:
<img width="1633" height="715" alt="image" src="https://github.com/user-attachments/assets/92901712-ed2b-4a78-bebb-3bcec2cac875" />

Загрузка в MySQL:
<img width="947" height="717" alt="image" src="https://github.com/user-attachments/assets/26ad8fc4-f526-4a36-ad3f-f47d54507d88" />
<img width="1852" height="730" alt="image" src="https://github.com/user-attachments/assets/a74bf5f9-4436-4aa1-a3b0-062e58bf7f57" />

## Шаг 4. Создание витрины данных (MySQL View)
```
CREATE VIEW student_performance_analysis AS
SELECT 
    id,
    name,
    department,
    email,
    math_grade,
    data_science_grade,
    programming_grade,
    lectures_attended,
    total_lectures,
    avg_grade,
    attendance_rate,
    -- Категория успеваемости на основе среднего балла
    CASE
        WHEN avg_grade >= 85 THEN 'Высокий'
        WHEN avg_grade >= 70 THEN 'Средний'
        WHEN avg_grade >= 50 THEN 'Низкий'
        WHEN avg_grade IS NOT NULL THEN 'Критический'
        ELSE 'Нет данных'
    END AS performance_category,
    -- Процент посещаемости
    ROUND(attendance_rate, 1) AS attendance_percent,
    (math_grade IS NOT NULL AND data_science_grade IS NOT NULL AND programming_grade IS NOT NULL) 
        AS has_all_grades
FROM student_performance_stats
-- Исключаем строки, где нет ни одной оценки и посещаемости 
WHERE (math_grade IS NOT NULL OR data_science_grade IS NOT NULL OR programming_grade IS NOT NULL)
   OR attendance_rate IS NOT NULL;
```
<img width="1858" height="1046" alt="image" src="https://github.com/user-attachments/assets/1ae5833a-9c40-4fb9-be86-21baecdaee69" />

# Файлы

[Файл Transformations]()

[Excel файл с данными](Files/grades.xlsx)

[CSV файл с данными](Files/attendance.csv)

