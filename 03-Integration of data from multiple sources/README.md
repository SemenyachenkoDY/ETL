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

## Шаг 2. Разработка трансформации в Pentaho (Spoon)

ОБщий вид, трансформации:
<img width="1919" height="1010" alt="image" src="https://github.com/user-attachments/assets/5f29b0de-ee43-4763-88ea-fd0a45039d36" />

### Настройка основных узлов:

Подключение PostgreSQL:

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

## Шаг 3. Создание витрины данных (MySQL View)

ZVjSoVl9
# Файлы

[Файл Transformations orders](KTR/lab_02_1_csv_orders.ktr)

[Файл создании БД в PostgreSQL]()

[CSV файл с данными]()

[Генератор данных]()
