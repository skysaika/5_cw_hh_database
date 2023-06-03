from cw_hh_db.db_manager.connect_db import ConnectDB


class DBManager:
    """Класс для работы с БД посредством SQL запросов запросам"""

    @staticmethod
    def get_companies_and_vacancies_count(table_name_emp: str, table_name_vac:str):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        conn = ConnectDB.connect_to_db()
        employers_vac_list = []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"""SELECT employer_name, COUNT({table_name_vac}) AS vacancy_count 
                                    FROM {table_name_emp} 
                                    INNER JOIN {table_name_vac} 
                                    USING(employer_id)
                                    GROUP BY employer_name 
                                    ORDER BY vacancy_count DESC""")
                    emp_vac_all = cur.fetchall()
                    for emp_vac in emp_vac_all:
                        emp, vac_count = emp_vac
                        employers_vac_list.append(f"Работодатель {emp}: число вакансий {vac_count}")
        finally:
            conn.close()
        return employers_vac_list

    @staticmethod
    def get_all_vacancies(table_name_emp: str, table_name_vac: str):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        vacancies_data_list = []
        conn = ConnectDB.connect_to_db()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""SELECT employer_name, vacancy_name, vacancy_salary_from, vacancy_salary_to, vacancy_url
                            FROM {table_name_emp} 
                            INNER JOIN {table_name_vac} 
                            USING(employer_id)
                            WHERE vacancy_salary_from IS NOT null
                            ORDER BY vacancy_salary_from DESC"""
                    )
                    all_info = cur.fetchall()
                    for vac_info in all_info:
                        emp_name, vac_name, vac_sal_from, vac_sal_to, vac_url = vac_info
                        if vac_sal_from is not None and vac_sal_to is not None:
                            form_sal_from, form_sal_to = vac_sal_from, vac_sal_to
                        elif vac_sal_from is None and vac_sal_to is not None:
                            form_sal_from, form_sal_to = vac_sal_to, vac_sal_to
                        elif vac_sal_from is not None and vac_sal_to is None:
                            form_sal_from, form_sal_to = vac_sal_from, vac_sal_from
                        else:
                            form_sal_from, form_sal_to = 'не указано', 'не указано'
                        vacancies_data_list.append(
                            f"""Работодатель: {emp_name}, вакансия: {vac_name}, 
зарплата от: {form_sal_from}, до: {form_sal_to}
url: {vac_url}\n""")
        finally:
            conn.close()
        return vacancies_data_list

    @staticmethod
    def get_avg_salary(table_name_vac: str):
        """Получает среднюю зарплату от, по вакансиям (среди тех где она указана)."""
        conn = ConnectDB.connect_to_db()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT ROUND(AVG(vacancy_salary_from)) FROM {table_name_vac}")
                    avg_salary = cur.fetchone()[0]
        finally:
            conn.close()
        return f"Средняя заработная плата от {avg_salary} рублей"

    @staticmethod
    def get_vacancies_with_higher_salary(table_name_vac: str):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        salaries_top_list = []
        conn = ConnectDB.connect_to_db()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"""SELECT vacancy_name, vacancy_salary_from
                                    FROM {table_name_vac}
                                    WHERE vacancy_salary_from > (SELECT AVG(vacancy_salary_from) FROM {table_name_vac}) 
                                    ORDER BY vacancy_salary_from DESC""")
                    salaries_top = cur.fetchall()
                    for salary_top in salaries_top:
                        name, sal_top = salary_top
                        salaries_top_list.append(f"Вакансия: {name}, заработная плата: {sal_top}")
        finally:
            conn.close()
        return salaries_top_list

    @staticmethod
    def get_vacancies_with_keyword(table_name_vac: str, keyword: str):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”"""
        vacancies_list = []
        conn = ConnectDB.connect_to_db()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""SELECT vacancy_name, vacancy_url, vacancy_salary_from, vacancy_salary_to 
                            FROM {table_name_vac}
                            WHERE LOWER(vacancy_name) LIKE LOWER('%{keyword}%')""")
                    vacancies_keyword = cur.fetchall()
                    for vacancy in vacancies_keyword:
                        name, url, sal_from, sal_to = vacancy
                        if sal_from is not None and sal_to is not None:
                            form_sal_from, form_sal_to = sal_from, sal_to
                        elif sal_from is None and sal_to is not None:
                            form_sal_from, form_sal_to = sal_to, sal_to
                        elif sal_from is not None and sal_to is None:
                            form_sal_from, form_sal_to = sal_from, sal_from
                        else:
                            form_sal_from, form_sal_to = 'не указано', 'не указано'
                        vacancies_list.append(
                            f"Вакансия: {name}, url: {url}, зарплата от: {form_sal_from}, до: {form_sal_to}")
        finally:
            conn.close()
        return vacancies_list
