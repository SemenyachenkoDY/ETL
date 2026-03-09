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

```
<?xml version="1.0" encoding="UTF-8"?>
<transformation>
  <info>
    <name>student_performance_etl_final</name>
    <description>Final ETL process with complex joins and cleaning</description>
    <trans_type>Normal</trans_type>
    <size_rowset>10000</size_rowset>
  </info>
  <order>
    <!-- Main Flow -->
    <hop><from>Get Students (Postgres)</from><to>Lookup Grades (Excel)</to><enabled>Y</enabled></hop>
    <hop><from>Lookup Grades (Excel)</from><to>Filter NULL Grades</to><enabled>Y</enabled></hop>
    <hop><from>Filter NULL Grades</from><to>Lookup Attendance</to><enabled>Y</enabled></hop>
    <hop><from>Lookup Attendance</from><to>Lookup Bonuses (Excel)</to><enabled>Y</enabled></hop>
    <hop><from>Lookup Bonuses (Excel)</from><to>Lookup Groups (CSV)</to><enabled>Y</enabled></hop>
    <hop><from>Lookup Groups (CSV)</from><to>Validate Attendance Range</to><enabled>Y</enabled></hop>
    <hop><from>Validate Attendance Range</from><to>Calculate Indicators</to><enabled>Y</enabled></hop>
    <hop><from>Calculate Indicators</from><to>Write to MySQL</to><enabled>Y</enabled></hop>
    
    <!-- Cleaning Attendance -->
    <hop><from>Read CSV Attendance</from><to>Sort Attendance</to><enabled>Y</enabled></hop>
    <hop><from>Sort Attendance</from><to>Unique Attendance</to><enabled>Y</enabled></hop>
    <hop><from>Unique Attendance</from><to>Lookup Attendance</to><enabled>Y</enabled></hop>
  </order>

  <!-- Step Definitions (Simplified for XML structure representation) -->
  <step>
    <name>Lookup Bonuses (Excel)</name>
    <type>StreamLookup</type>
    <from>Read Excel Bonuses</from>
    <lookup>
      <key><name>student_id</name><field>student_id</field></key>
      <value><name>bonus_points</name><rename>bonus_points</rename></value>
    </lookup>
    <GUI><xloc>500</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Read Excel Bonuses</name>
    <type>ExcelInput</type>
    <file><name>c:\Users\Даня и Маша\Downloads\Проекты\Lab_ETL\files\bonuses.xlsx</name></file>
    <fields><field><name>student_id</name><type>Integer</type></field><field><name>bonus_points</name><type>Integer</type></field></fields>
    <spreadsheet_type>SAX_POI</spreadsheet_type>
    <GUI><xloc>500</xloc><yloc>150</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Lookup Groups (CSV)</name>
    <type>StreamLookup</type>
    <from>Read CSV Group Info</from>
    <lookup>
      <key><name>student_id</name><field>student_id</field></key>
      <value><name>group_code</name><rename>group_code</rename></value>
      <value><name>is_active</name><rename>is_active</rename></value>
    </lookup>
    <GUI><xloc>650</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Read CSV Group Info</name>
    <type>CsvInput</type>
    <filename>c:\Users\Даня и Маша\Downloads\Проекты\Lab_ETL\files\group_info.csv</filename>
    <header>Y</header>
    <fields><field><name>student_id</name><type>Integer</type></field><field><name>group_code</name><type>String</type></field></fields>
    <GUI><xloc>650</xloc><yloc>150</yloc><draw>Y</draw></GUI>
  </step>

  <!-- Other steps remain analogous to dirty_data version but with more hops -->
  <step><name>Get Students (Postgres)</name><type>TableInput</type><GUI><xloc>50</xloc><yloc>50</yloc><draw>Y</draw></GUI></step>
  <step><name>Calculate Indicators</name><type>Calculator</type><GUI><xloc>800</xloc><yloc>50</yloc><draw>Y</draw></GUI></step>
  <step><name>Write to MySQL</name><type>TableOutput</type><GUI><xloc>950</xloc><yloc>50</yloc><draw>Y</draw></GUI></step>
</transformation>

```

## Шаг 3. Создание витрины данных (MySQL View)
# Файлы

[Файл Transformations orders](KTR/lab_02_1_csv_orders.ktr)

[Файл создании БД в PostgreSQL]()

[CSV файл с данными]()

[Генератор данных]()
