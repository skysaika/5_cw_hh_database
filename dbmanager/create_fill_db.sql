--- Код для создания базы данных "db_employers" в PGAdmin

---Проверка, что таблица существует:
SELECT EXISTS
(
    SELECT 1
    FROM   information_schema.tables
    WHERE  table_name=%s
);

---Создание базы данных "db_employers"
CREATE DATABASE employers_db;

---Очистка таблиц перед заполнением
TRUNCATE TABLE {table_name} CASCADE;

---Создание  таблиц
CREATE TABLE {table_name}
(employer_id int PRIMARY KEY,
employer_name varchar(100),
employer_url varchar(200));

INSERT INTO {table_name} VALUES (%s, %s, %s)',
(employer['id'],
 employer['name'],
 employer['alternate_url']);