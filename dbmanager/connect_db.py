import psycopg2
import psycopg2.errors
import os
from dotenv import load_dotenv

from cw_hh_db.parser.request_hh import HH

def table_exists(cursor, table_name):
    """Функция проверки существования таблицы"""
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables
            WHERE  table_name = %s
        );
    """, (table_name,))
    return cursor.fetchone()[0]


class ConnectDB:
    """Класс для подключения к Базе данных"""

    @staticmethod
    def connect_to_db():
        load_dotenv()
        postgres_key = os.getenv('POSTGRESSQL_KEY')
        conn = psycopg2.connect(host='localhost', user='postgres', password=postgres_key, dbname='employers_db')
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute('CREATE DATABASE employers_db')
            except psycopg2.errors.lookup('42P04'):
                pass  # если база данных уже существует, то пропускаем создание
        conn.autocommit = False
        return conn

class FillDB(HH):
    """Класс для заполнения базы данных, наследуется от HH т.к. обращается к его методам при получении
    необходимой информации"""
    __employers_names = []

    def __init__(self, employers_list: list):
        """Инициализируется списком передаваемых работодателей"""
        self.employers_list = employers_list
        for employer in self.employers_list:
            super().__init__(employer)
            self.__employers_names.append(self.employer)

    @classmethod
    def __get_employers_all(cls):
        """Метод для получения информации из класса-родителя по работодателям"""
        for employer in cls.__employers_names:
            employer_info = HH(employer)
            employer_info.get_employer()
        return super().employers_data

    @classmethod
    def __get_vacancies_all(cls):
        """Метод для получения информации из класса-родителя по вакансиям"""
        vacancies_all = []
        for employer in cls.employers_data:
            emp = HH(employer['name'])
            vacancies_emp = emp.get_vacancies(employer['id'])
            for vacancy in vacancies_emp:
                vacancies_all.append(vacancy)
        return vacancies_all

    def fill_db_employers(self, table_name: str):
        """Метод заполняет таблицу данными о работодателях, если возникает ошибка
        (что ID такого работодателя уже существует) идет дальше"""
        conn = ConnectDB.connect_to_db()
        employers = self.__get_employers_all()
        try:
            with conn:
                with conn.cursor() as cur:
                    if table_exists(cur, table_name):
                        cur.execute(f'TRUNCATE TABLE {table_name} CASCADE')
                    else:
                        cur.execute(f"""CREATE TABLE {table_name}
                                        (employer_id int PRIMARY KEY, 
                                        employer_name varchar(100), 
                                        employer_url varchar(200))""")
                    for employer in employers:
                        try:
                            with conn.cursor() as cur:
                                cur.execute(f'INSERT INTO {table_name} VALUES (%s, %s, %s)',
                                            (employer['id'],
                                             employer['name'],
                                             employer['alternate_url']))
                        except psycopg2.errors.UniqueViolation:
                            continue
        except psycopg2.errors.UndefinedTable:
            print(f"Таблица {table_name} не найдена")
        finally:
            conn.close()

    def fill_db_vacancies(self, table_name: str):
        """Метод заполняет таблицу данными о вакансиях, если возникает ошибка
         (что ID такой работодателя уже существует) идет дальше"""
        conn = ConnectDB.connect_to_db()
        vacancies = self.__get_vacancies_all()
        try:
            with conn:
                with conn.cursor() as cur:
                    if table_exists(cur, table_name):
                        cur.execute(f'TRUNCATE TABLE {table_name} CASCADE')
                    else:
                        cur.execute(
                            f"""CREATE TABLE {table_name} 
                            (vacancy_id int PRIMARY KEY, 
                            vacancy_name varchar(100), 
                            vacancy_url varchar(200), 
                            vacancy_salary_from int, 
                            vacancy_salary_to int, 
                            employer_id int 
                            REFERENCES employers(employer_id));""")
            for vacancy in vacancies:
                try:
                    with conn:
                        with conn.cursor() as cur:
                            cur.execute(f'INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s)',
                                        (vacancy['id'], vacancy['vacancy'], vacancy['url'], vacancy['salary_from'],
                                         vacancy['salary_to'], vacancy['employer_id']))
                except psycopg2.errors.UniqueViolation:
                    continue
        except psycopg2.errors.UndefinedTable:
            print(f"Таблица {table_name} не найдена")
        finally:
            conn.close()
