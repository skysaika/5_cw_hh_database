---список всех компаний и количество вакансий у каждой компании
SELECT employer_name, COUNT(vacancies) AS vacancy_count #
FROM employers
INNER JOIN vacancies
USING(employer_id)
GROUP BY employer_name
ORDER BY vacancy_count DESC;


---список всех вакансий с указанием названия компании,
---названия вакансии и зарплаты и ссылки на вакансию
SELECT employer_name, vacancy_name, vacancy_salary_from, vacancy_salary_to, vacancy_url
FROM employers
INNER JOIN vacancies
USING(employer_id)
WHERE vacancy_salary_from IS NOT null
ORDER BY vacancy_salary_from DESC;


---Получает среднюю зарплату от, по вакансиям (среди тех где она указана).
SELECT ROUND(AVG(vacancy_salary_from)) FROM vacancies;


---Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
SELECT vacancy_name, vacancy_salary_from
FROM vacancies
WHERE vacancy_salary_from > (SELECT AVG(vacancy_salary_from) FROM vacancies)
ORDER BY vacancy_salary_from DESC;



---список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”
SELECT vacancy_name, vacancy_url, vacancy_salary_from, vacancy_salary_to
FROM vacancies
WHERE LOWER(vacancy_name) LIKE LOWER('%Python%');
