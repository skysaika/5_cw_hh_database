import json
import requests


class HH:
    """Класс для доступа к API HeadHunter"""
    employer_dict = {}
    employers_data = []
    vacancies_emp = []

    def __init__(self, employer):
        self.employer = employer

    def get_employer(self):
        """Метод для получения информации по работодателю"""
        url = 'https://api.hh.ru/employers'
        params = {'text': {self.employer}, "areas": 113, 'per_page': 20}
        response = requests.get(url, params=params)
        employer = response.json()
        if employer is None:
            return "Данные не получены"
        elif 'items' not in employer:
            return "Нет указанных работодателей"
        else:
            self.employer_dict = {'id': employer['items'][0]['id'], 'name': employer['items'][0]['name'],
                                  'alternate_url': employer['items'][0]['alternate_url']}
            self.employers_data.append(self.employer_dict)
            return self.employer_dict

    def __get_page_vacancies(self, employer_id, page):
        """Метод для получения всех вакансий исходя из id работодателя"""
        self.employer_id = employer_id
        params = {
            'employer_id': employer_id,
            'area': 113,
            'per_page': 100,
            'page': page
        }
        response = requests.get('https://api.hh.ru/vacancies', params)
        data = response.content.decode()
        response.close()
        return data

    def get_vacancies(self, employer_id):
        """Метод для обработки полученной информации по вакансиям"""
        vacancies_emp_dicts = []
        for page in range(10):
            vacancies_data = json.loads(self.__get_page_vacancies(employer_id, page))
            if 'errors' in vacancies_data:
                return vacancies_emp_dicts  # возвращаем пустой список
            items = vacancies_data.get('items')
            for item in items:
                salary = item.get('salary')
                if salary is not None:
                    if salary.get('to') is None:
                        salary['to'] = salary.get('from')
                    elif salary.get('from') is None:
                        salary['from'] = salary.get('to')
                    vacancy_dict = {'id': item['id'], 'vacancy': item['name'],
                                    'url': item['apply_alternate_url'],
                                    'salary_from': salary.get('from'),
                                    'salary_to': salary.get('to'),
                                    'employer_id': item['employer']['id']}
                    if vacancy_dict['salary_to'] is None:
                        vacancy_dict['salary_to'] = vacancy_dict['salary_from']
                    vacancies_emp_dicts.append(vacancy_dict)
        return vacancies_emp_dicts
