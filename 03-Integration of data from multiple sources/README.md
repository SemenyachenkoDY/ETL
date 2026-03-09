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
    <description/>
    <extended_description/>
    <trans_version/>
    <trans_type>Normal</trans_type>
    <trans_status>0</trans_status>
    <directory>/</directory>
    <parameters>
    </parameters>
    <log>
      <trans-log-table>
        <connection/>
        <schema/>
        <table/>
        <size_limit_lines/>
        <interval/>
        <timeout_days/>
        <field><id>ID_BATCH</id><enabled>Y</enabled><name>ID_BATCH</name></field>
        <field><id>CHANNEL_ID</id><enabled>Y</enabled><name>CHANNEL_ID</name></field>
        <field><id>TRANSNAME</id><enabled>Y</enabled><name>TRANSNAME</name></field>
        <field><id>STATUS</id><enabled>Y</enabled><name>STATUS</name></field>
        <field><id>LINES_READ</id><enabled>Y</enabled><name>LINES_READ</name><subject/></field>
        <field><id>LINES_WRITTEN</id><enabled>Y</enabled><name>LINES_WRITTEN</name><subject/></field>
        <field><id>LINES_UPDATED</id><enabled>Y</enabled><name>LINES_UPDATED</name><subject/></field>
        <field><id>LINES_INPUT</id><enabled>Y</enabled><name>LINES_INPUT</name><subject/></field>
        <field><id>LINES_OUTPUT</id><enabled>Y</enabled><name>LINES_OUTPUT</name><subject/></field>
        <field><id>LINES_REJECTED</id><enabled>Y</enabled><name>LINES_REJECTED</name><subject/></field>
        <field><id>ERRORS</id><enabled>Y</enabled><name>ERRORS</name></field>
        <field><id>STARTDATE</id><enabled>Y</enabled><name>STARTDATE</name></field>
        <field><id>ENDDATE</id><enabled>Y</enabled><name>ENDDATE</name></field>
        <field><id>LOGDATE</id><enabled>Y</enabled><name>LOGDATE</name></field>
        <field><id>DEPDATE</id><enabled>Y</enabled><name>DEPDATE</name></field>
        <field><id>REPLAYDATE</id><enabled>Y</enabled><name>REPLAYDATE</name></field>
        <field><id>LOG_FIELD</id><enabled>Y</enabled><name>LOG_FIELD</name></field>
        <field><id>EXECUTING_SERVER</id><enabled>N</enabled><name>EXECUTING_SERVER</name></field>
        <field><id>EXECUTING_USER</id><enabled>N</enabled><name>EXECUTING_USER</name></field>
        <field><id>CLIENT</id><enabled>N</enabled><name>CLIENT</name></field>
      </trans-log-table>
    </log>
    <maxdate>
      <connection/>
      <table/>
      <field/>
      <offset>0.0</offset>
      <maxdiff>0.0</maxdiff>
    </maxdate>
    <size_rowset>10000</size_rowset>
    <sleep_time_empty>50</sleep_time_empty>
    <sleep_time_full>50</sleep_time_full>
    <unique_connections>N</unique_connections>
    <feedback_shown>Y</feedback_shown>
    <feedback_size>50000</feedback_size>
    <using_thread_priorities>Y</using_thread_priorities>
    <shared_objects_file/>
    <capture_step_performance>N</capture_step_performance>
    <step_performance_capturing_delay>1000</step_performance_capturing_delay>
    <step_performance_capturing_size_limit>100</step_performance_capturing_size_limit>
    <dependencies>
    </dependencies>
    <partitionschemes>
    </partitionschemes>
    <slaveservers>
    </slaveservers>
    <clusterschemes>
    </clusterschemes>
    <created_user>-</created_user>
    <created_date>2023/10/26 14:00:00.000</created_date>
    <modified_user>-</modified_user>
    <modified_date>2023/10/26 14:00:00.000</modified_date>
    <key_for_session_key/>
    <is_key_private>N</is_key_private>
  </info>
  <notepads>
  </notepads>
  <connection>
    <name>PostgreSQL_Source</name>
    <server>localhost</server>
    <type>POSTGRESQL</type>
    <access>Native</access>
    <database>st_200</database>
    <port>5432</port>
    <username>admin</username>
    <password>admin</password>
    <attributes>
      <attribute><code>FORCE_IDENTIFIERS_TO_LOWERCASE</code><attribute>N</attribute></attribute>
      <attribute><code>FORCE_IDENTIFIERS_TO_UPPERCASE</code><attribute>N</attribute></attribute>
      <attribute><code>IS_CLUSTERED</code><attribute>N</attribute></attribute>
      <attribute><code>PORT_NUMBER</code><attribute>5432</attribute></attribute>
      <attribute><code>PRESERVE_RESERVED_WORD_CASE</code><attribute>Y</attribute></attribute>
      <attribute><code>QUOTE_ALL_FIELDS</code><attribute>N</attribute></attribute>
      <attribute><code>SUPPORTS_BOOLEAN_DATA_TYPE</code><attribute>Y</attribute></attribute>
      <attribute><code>SUPPORTS_TIMESTAMP_DATA_TYPE</code><attribute>Y</attribute></attribute>
      <attribute><code>USE_CURSOR</code><attribute>N</attribute></attribute>
    </attributes>
  </connection>
  <connection>
    <name>MySQL_Target</name>
    <server>95.131.149.21</server>
    <type>MYSQL</type>
    <access>Native</access>
    <database>mgpu_ico_etl_XX</database>
    <port>3306</port>
    <username>your_username</username>
    <password>your_password</password>
    <attributes>
      <attribute><code>FORCE_IDENTIFIERS_TO_LOWERCASE</code><attribute>N</attribute></attribute>
      <attribute><code>FORCE_IDENTIFIERS_TO_UPPERCASE</code><attribute>N</attribute></attribute>
      <attribute><code>IS_CLUSTERED</code><attribute>N</attribute></attribute>
      <attribute><code>PORT_NUMBER</code><attribute>3306</attribute></attribute>
      <attribute><code>PRESERVE_RESERVED_WORD_CASE</code><attribute>Y</attribute></attribute>
      <attribute><code>QUOTE_ALL_FIELDS</code><attribute>N</attribute></attribute>
      <attribute><code>STREAM_RESULTS</code><attribute>Y</attribute></attribute>
      <attribute><code>SUPPORTS_BOOLEAN_DATA_TYPE</code><attribute>Y</attribute></attribute>
      <attribute><code>SUPPORTS_TIMESTAMP_DATA_TYPE</code><attribute>Y</attribute></attribute>
      <attribute><code>USE_CURSOR</code><attribute>Y</attribute></attribute>
    </attributes>
  </connection>
  <order>
    <hop><from>Get Students (Postgres)</from><to>Lookup Grades (Excel)</to><enabled>Y</enabled></hop>
    <hop><from>Lookup Grades (Excel)</from><to>Filter NULL Grades</to><enabled>Y</enabled></hop>
    <hop><from>Filter NULL Grades</from><to>Lookup Attendance</to><enabled>Y</enabled></hop>
    <hop><from>Lookup Attendance</from><to>Validate Attendance Range</to><enabled>Y</enabled></hop>
    <hop><from>Validate Attendance Range</from><to>Calculate Indicators</to><enabled>Y</enabled></hop>
    <hop><from>Calculate Indicators</from><to>Write to MySQL</to><enabled>Y</enabled></hop>
    <hop><from>Read CSV Attendance</from><to>Sort Attendance</to><enabled>Y</enabled></hop>
    <hop><from>Sort Attendance</from><to>Unique Attendance</to><enabled>Y</enabled></hop>
    <hop><from>Unique Attendance</from><to>Lookup Attendance</to><enabled>Y</enabled></hop>
  </order>

  <step>
    <name>Get Students (Postgres)</name>
    <type>TableInput</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <connection>PostgreSQL_Source</connection>
    <sql>SELECT student_id, full_name, major FROM students</sql>
    <limit>0</limit>
    <lookup/>
    <execute_each_row>N</execute_each_row>
    <variables_active>N</variables_active>
    <lazy_conversion_active>N</lazy_conversion_active>
    <cached_row_meta_active>N</cached_row_meta_active>
    <row-meta/>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>50</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Read Excel Grades</name>
    <type>ExcelInput</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <header>Y</header>
    <noempty>Y</noempty>
    <stoponempty>N</stoponempty>
    <filefield/>
    <sheetfield/>
    <sheet_rownum_field/>
    <rownum_field/>
    <sheetname_field/>
    <file_aggregation_field/>
    <dynamic_filenames_field/>
    <file_lookup_field/>
    <file>
      <name>c:\Users\Даня и Маша\Downloads\Проекты\Lab_ETL\files\grades.xlsx</name>
      <filemask/>
      <exclude_filemask/>
      <file_required>N</file_required>
      <include_subfolders>N</include_subfolders>
    </file>
    <fields>
      <field><name>student_id</name><type>Integer</type><length>-1</length><precision>-1</precision><trim_type>none</trim_type><repeat>N</repeat><format/><currency/><decimal/><group/></field>
      <field><name>math_grade</name><type>Integer</type><length>-1</length><precision>-1</precision><trim_type>none</trim_type><repeat>N</repeat><format/><currency/><decimal/><group/></field>
      <field><name>data_science_grade</name><type>Integer</type><length>-1</length><precision>-1</precision><trim_type>none</repeat>N</repeat><format/><currency/><decimal/><group/></field>
      <field><name>programming_grade</name><type>Integer</type><length>-1</length><precision>-1</precision><trim_type>none</trim_type><repeat>N</repeat><format/><currency/><decimal/><group/></field>
    </fields>
    <sheets><sheet><name>Sheet1</name><startrow>0</startrow><startcol>0</startcol></sheet></sheets>
    <strict_types>N</strict_types>
    <spreadsheet_type>SAX_POI</spreadsheet_type>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>200</xloc><yloc>150</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Lookup Grades (Excel)</name>
    <type>StreamLookup</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <from>Read Excel Grades</from>
    <input_sorted>N</input_sorted>
    <preserve_memory>Y</preserve_memory>
    <sorted_list>N</sorted_list>
    <integer_pair>N</integer_pair>
    <lookup>
      <key><name>student_id</name><field>student_id</field></key>
      <value><name>math_grade</name><rename>math_grade</rename><default/><type>Integer</type></value>
      <value><name>data_science_grade</name><rename>data_science_grade</rename><default/><type>Integer</type></value>
      <value><name>programming_grade</name><rename>programming_grade</rename><default/><type>Integer</type></value>
    </lookup>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>200</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Filter NULL Grades</name>
    <type>FilterRows</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <compare>
      <condition>
        <negated>N</negated>
        <conditions>
          <condition>
            <negated>N</negated>
            <leftvalue>math_grade</leftvalue>
            <function>IS NOT NULL</function>
            <rightvalue/>
          </condition>
          <condition>
            <negated>N</negated>
            <operator>AND</operator>
            <leftvalue>math_grade</leftvalue>
            <function>&gt;=</function>
            <rightvalue/>
            <value><name>constant</name><type>Integer</type><text>0</text><length>-1</length><precision>0</precision><isnull>N</isnull><mask>#</mask></value>
          </condition>
        </conditions>
      </condition>
    </compare>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>350</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Read CSV Attendance</name>
    <type>CsvInput</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <filename>c:\Users\Даня и Маша\Downloads\Проекты\Lab_ETL\files\attendance.csv</filename>
    <header>Y</header>
    <separator>,</separator>
    <fields>
      <field><name>student_id</name><type>Integer</type></field>
      <field><name>lectures_attended</name><type>Integer</type></field>
      <field><name>total_lectures</name><type>Integer</type></field>
    </fields>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>350</xloc><yloc>250</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Sort Attendance</name>
    <type>SortRows</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <directory>%%java.io.tmpdir%%</directory>
    <prefix>out</prefix>
    <sort_size>1000000</sort_size>
    <free_memory/>
    <compress>N</compress>
    <compress_variable/>
    <unique_rows>N</unique_rows>
    <fields>
      <field><name>student_id</name><ascending>Y</ascending><case_sensitive>N</case_sensitive><collator_enabled>N</collator_enabled><collator_strength>0</collator_strength><presorted>N</presorted></field>
    </fields>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>350</xloc><yloc>150</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Unique Attendance</name>
    <type>Unique</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <fields><field><name>student_id</name><case_sensitive>N</case_sensitive></field></fields>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>500</xloc><yloc>150</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Lookup Attendance</name>
    <type>StreamLookup</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <from>Unique Attendance</from>
    <input_sorted>N</input_sorted>
    <preserve_memory>Y</preserve_memory>
    <sorted_list>N</sorted_list>
    <integer_pair>N</integer_pair>
    <lookup>
      <key><name>student_id</name><field>student_id</field></key>
      <value><name>lectures_attended</name><rename>lectures_attended</rename><default/><type>Integer</type></value>
      <value><name>total_lectures</name><rename>total_lectures</rename><default/><type>Integer</type></value>
    </lookup>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>500</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Validate Attendance Range</name>
    <type>FilterRows</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <compare>
      <condition>
        <negated>N</negated>
        <conditions>
          <condition>
            <negated>N</negated>
            <leftvalue>lectures_attended</leftvalue>
            <function>&lt;=</function>
            <rightvalue>total_lectures</rightvalue>
          </condition>
          <condition>
            <negated>N</negated>
            <operator>AND</operator>
            <leftvalue>lectures_attended</leftvalue>
            <function>&gt;=</function>
            <rightvalue/>
            <value><name>constant</name><type>Integer</type><text>0</text><length>-1</length><precision>0</precision><isnull>N</isnull><mask>#</mask></value>
          </condition>
        </conditions>
      </condition>
    </compare>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>650</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Calculate Indicators</name>
    <type>Calculator</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <calculation>
      <field_name>sum_grades</field_name>
      <calc_type>ADD3</calc_type>
      <field_a>math_grade</field_a>
      <field_b>data_science_grade</field_b>
      <field_c>programming_grade</field_c>
      <value_type>Integer</value_type>
      <remove>Y</remove>
    </calculation>
    <calculation>
      <field_name>const_3</field_name>
      <calc_type>CONSTANT</calc_type>
      <field_a>3</field_a>
      <value_type>Integer</value_type>
      <remove>Y</remove>
    </calculation>
    <calculation>
      <field_name>avg_grade</field_name>
      <calc_type>DIVIDE</calc_type>
      <field_a>sum_grades</field_a>
      <field_b>const_3</field_b>
      <value_type>Number</value_type>
      <value_precision>2</value_precision>
    </calculation>
    <calculation>
      <field_name>attendance_rate</field_name>
      <calc_type>PERCENT_1</calc_type>
      <field_a>lectures_attended</field_a>
      <field_b>total_lectures</field_b>
      <value_type>Number</value_type>
      <value_precision>2</value_precision>
    </calculation>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>800</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step>
    <name>Write to MySQL</name>
    <type>TableOutput</type>
    <description/>
    <distribute>Y</distribute>
    <custom_distribution/>
    <copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <connection>MySQL_Target</connection>
    <schema/>
    <table>student_performance_stats</table>
    <commit>1000</commit>
    <truncate>Y</truncate>
    <ignore_errors>N</ignore_errors>
    <use_batch>Y</use_batch>
    <specify_fields>Y</specify_fields>
    <partitioning_enabled>N</partitioning_enabled>
    <fields>
      <field><column_name>student_id</column_name><stream_name>student_id</stream_name></field>
      <field><column_name>full_name</column_name><stream_name>full_name</stream_name></field>
      <field><column_name>major</column_name><stream_name>major</stream_name></field>
      <field><column_name>avg_grade</column_name><stream_name>avg_grade</stream_name></field>
      <field><column_name>attendance_rate</column_name><stream_name>attendance_rate</stream_name></field>
    </fields>
    <attributes/>
    <cluster_schema/>
    <remotesteps><input></input><output></output></remotesteps>
    <GUI><xloc>950</xloc><yloc>50</yloc><draw>Y</draw></GUI>
  </step>

  <step_error_handling></step_error_handling>
  <slave-step-copy-partition-distribution></slave-step-copy-partition-distribution>
  <slave_transformation>N</slave_transformation>
  <attributes/>
</transformation>

```

## Шаг 3. Создание витрины данных (MySQL View)


# Файлы

[Файл Transformations orders](KTR/lab_02_1_csv_orders.ktr)

[Файл создании БД в PostgreSQL]()

[CSV файл с данными]()

[Генератор данных]()
