--- Код для создания таблиц в PGAdmin


---Проверка, что таблица существует:
SELECT EXISTS
(
    SELECT 1
    FROM   information_schema.tables
    WHERE  table_name=%s
);

CREATE TABLE employers  ---в коде {table_name}
(
	employer_id int PRIMARY KEY,
	employer_name varchar(100),
	eployer_url varchar(200)
);

CREATE TABLE vacancies  ---в коде {table_name}
(
	vacancy_id int PRIMARY KEY,
	vacancy_name varchar(100),
	vacancy_url varchar(200),
	vacancy_salary_from int,
	vacancy_salary_to int,
	employer_id int REFERENCES employers(employer_id)
);
