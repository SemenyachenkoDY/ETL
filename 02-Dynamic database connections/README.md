
# Лабораторная работа №2. Динамические соединения с базами данных

**Цель работы.** Получить практические навыки создания сложного ETL-процесса, включающего динамическую загрузку файлов по HTTP, нормализацию базы данных, обработку дубликатов и настройку обработки ошибок с использованием Pentaho Data Integration (PDI).

## Вариант 14

|№ |Основной фильтр для загрузки в БД	 |Доп. задание 1 (Аналитика)	 |Доп. задание 2 (Аналитика)| 
|-|--------------|------------|----|
|14| Только заказы с возвратами |	Статистика по менеджерам| 	Анализ регионов|

# Ход работы

## Шаг 1. Подготовка базы данных

Перед запуском ETL-процесса необходимо создать структуру таблиц в вашей базе данных (mgpu_ico_etl_14). Выполните следующий SQL-скрипт через phpMyAdmin или DBeaver:
```SQL
-- 1. Таблица заказов (фактов)
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
 row_id INT PRIMARY KEY,
 order_date DATE,
 ship_date DATE,
 ship_mode VARCHAR(50),
 sales DECIMAL(10,2),
 quantity INT,
 discount DECIMAL(4,2),
 profit DECIMAL(10,2),
 returned TINYINT(1) DEFAULT 0 -- 1 = Yes, 0 = No
);
```
Пример Успешного выполнения:
<img width="1919" height="531" alt="image" src="https://github.com/user-attachments/assets/7726cdf2-39d6-4cc4-878a-6eda42aa3053" />

```
-- 2. Таблица клиентов (измерение)
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
 id INT AUTO_INCREMENT PRIMARY KEY,
 customer_id VARCHAR(20) NOT NULL,
 customer_name VARCHAR(100),
 segment VARCHAR(50),
 country VARCHAR(100),
 city VARCHAR(100),
 state VARCHAR(100),
 postal_code VARCHAR(20),
 region VARCHAR(50),
 INDEX idx_customer_id (customer_id),
 INDEX idx_region (region)
);
```
Пример Успешногго выполнения:
<img width="1919" height="574" alt="image" src="https://github.com/user-attachments/assets/04f5b5a0-3b3f-430d-82e4-fb9ee3531781" />

```
-- 3. Таблица продуктов (измерение)
DROP TABLE IF EXISTS products;
CREATE TABLE products (
 id INT AUTO_INCREMENT PRIMARY KEY,
 product_id VARCHAR(20) NOT NULL,
 category VARCHAR(50),
 sub_category VARCHAR(50),
 product_name VARCHAR(255),
 person VARCHAR(100),
 INDEX idx_product_id (product_id),
 INDEX idx_category (category),
 INDEX idx_subcategory (sub_category)
);
```
Пример Успешногго выполнения:
<img width="1919" height="547" alt="image" src="https://github.com/user-attachments/assets/dd116cf2-34f5-48b1-8f50-01d4c2f6d969" />


```
-- 4. Индексы и настройка кодировки
ALTER TABLE orders ADD INDEX idx_order_date (order_date);
ALTER TABLE orders ADD INDEX idx_ship_date (ship_date);

-- ЗАМЕНИТЕ mgpu_ico_etl_14 на имя вашей базы данных!
ALTER DATABASE mgpu_ico_etl_14 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE customers CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE products CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```
Пример Успешногго выполнения:
<img width="1919" height="922" alt="image" src="https://github.com/user-attachments/assets/9a7c7146-df97-4003-980e-0b6cc1c50f28" />

## Шаг 2. Настройка Job (Главного задания)

**Set Variables: Создайте переменную пути к файлу.**
<img width="1833" height="1035" alt="image" src="https://github.com/user-attachments/assets/9187ee10-ffb6-4544-be43-16e19188f2bf" />

**Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.**
<img width="1833" height="1035" alt="image" src="https://github.com/user-attachments/assets/ae602505-c8bd-4723-9f9d-5b567ad18509" />

**HTTP (Download): Загрузка файла, если его нет.**

**Transformation. Последовательный вызов трех трансформаций для загрузки данных.**
<img width="1833" height="1035" alt="image" src="https://github.com/user-attachments/assets/7361647e-1c17-4929-aa1c-3714b850b6c4" />

## Шаг 3. Реализация Трансформаций (Transformations)
### Трансформация 1. Load Orders

**Select Values. Установите типы данных (Date format: dd.MM.yyyy для дат, Integer для ID).**
<img width="1539" height="812" alt="image" src="https://github.com/user-attachments/assets/b23323a7-833f-4701-8d13-0e18af66dcaf" />

**Memory Group By. Используется для дедупликации (группировка по row_id, взятие первых значений по остальным полям).**
<img width="1137" height="764" alt="image" src="https://github.com/user-attachments/assets/3cdf6b45-12f7-4170-a741-2087f545ebde" />

**Filter Rows (Валидация)**
* Условие: order_date IS NOT NULL AND ship_date IS NOT NULL AND reterned = YES.
* TRUE -> Table Output (в таблицу orders).
* FALSE -> Write to Log (логирование ошибок).
<img width="1084" height="715" alt="image" src="https://github.com/user-attachments/assets/86f31bd1-9b66-45d1-b2bf-de031eca6c1c" />

**Value Mapper. Преобразование поля Returned: Yes -> 1, No -> 0, Empty -> 0.**
<img width="845" height="361" alt="image" src="https://github.com/user-attachments/assets/dbe21791-545d-4e5e-9283-032fd68b204b" />

### Трансформация 2. Load Customers

**Select Values. Оставьте только поля, относящиеся к клиенту (customer_id, name, city и т.д.).**
<img width="1844" height="1053" alt="image" src="https://github.com/user-attachments/assets/8ba418c6-c85a-44cf-a047-8fea208c5b81" />

**Memory Group By. Группировка по customer_id (устранение дублей клиентов).**
<img width="1844" height="1053" alt="image" src="https://github.com/user-attachments/assets/074f44c5-3311-442a-83e1-46f6ee3075ad" />

**Table Output. Загрузка в таблицу customers.**
<img width="1844" height="1053" alt="image" src="https://github.com/user-attachments/assets/3b147de1-5ee1-4e5e-be52-a1d5ae2918f2" />

### Трансформация 3. Load Products

**Select Values. Оставьте поля продукта (product_id, category, name и т.д.).**
<img width="1844" height="1053" alt="image" src="https://github.com/user-attachments/assets/a6c7b226-4b88-4103-aba5-c2b7060e5933" />


**Memory Group By. Группировка по product_id.**
<img width="1844" height="1053" alt="image" src="https://github.com/user-attachments/assets/1060761b-ce53-440a-b809-b4159697d769" />


**Table Output. Загрузка в таблицу products.**
<img width="1844" height="1053" alt="image" src="https://github.com/user-attachments/assets/214b96c3-97f7-441c-a9a3-6b7be15bd8d0" />

## Шаг 4 Выполнение доп заданий

### Настройка инпута 1
<img width="1833" height="1035" alt="image" src="https://github.com/user-attachments/assets/401be8ec-297f-4e30-b9f9-1e3031bcccf3" />
```
SELECT 
    p.person AS manager,
    COUNT(DISTINCT o.row_id) AS total_orders,
    SUM(o.sales) AS total_sales,
    SUM(o.profit) AS total_profit,
    AVG(o.discount) AS avg_discount,
    SUM(o.quantity) AS total_quantity
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.person
ORDER BY total_sales DESC;
```
**Результат работы:**
<img width="1833" height="1035" alt="image" src="https://github.com/user-attachments/assets/c66ffb19-d2b2-4495-aa65-9110e070e521" />

### Настройка инпута 2 
<img width="1833" height="1035" alt="image" src="https://github.com/user-attachments/assets/f5bb5f44-0a7e-491a-aadb-071c8133042e" />

```
SELECT 
    c.region,
    COUNT(DISTINCT o.row_id) AS order_count,
    SUM(o.sales) AS total_sales,
    SUM(o.profit) AS total_profit,
    AVG(o.discount) AS avg_discount,
    SUM(o.quantity) AS total_quantity,
    COUNT(DISTINCT c.customer_id) AS customer_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region
ORDER BY total_sales DESC;
```
**Результат работы:**
<img width="1833" height="1035" alt="image" src="https://github.com/user-attachments/assets/c1600a94-6c1d-4580-90a8-05755b908ca9" />

[Файл Job](/KTR/Job%20CSV_to_MYsql.kjb)

[Файл Transformations orders](KTR/lab_02_1_csv_orders.ktr)

[Файл Transformations products](/KTR/lab_02_2_csv_to_Customers.ktr)

[Файл Transformations customers](KTR/lab_02_3_csv_to_products.ktr)

[Файл трансформации для Статистика по менеджерам](KTR/zadanie_1.ktr)

[[Файл трансформации для Анализ регионов](KTR/zadanie_2.ktr)
