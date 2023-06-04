from dbmanager.connect_db import ConnectDB, FillDB
from dbmanager.create_tables import TablesCreator
from dbmanager.dbmanager import DBManager


def main():
    employers = [
        'skyeng', 'skillbox', 'Kaspersky lab', 'Яндекс Практикум', 'Вконтакте',
        'LG Electronics Inc.', 'SberTech', 'Google', 'Газпромбанк', 'GeekBrains'
    ]
    user_input_emp = "employers"
    user_input_vac = "vacancies"

    tables_creator = TablesCreator()
    tables_creator.create_employers(user_input_emp)
    tables_creator.create_vacancies(user_input_vac)
    print(f"""Программа позволяет ознакомиться с информацией по наличию вакансий у следующих
    работодателей {', '.join(employers)}""")
    print(f"Идет процесс заполнения таблиц {user_input_emp} и {user_input_vac} данными с сайта hh.ru.")
    fill_db = FillDB(employers)
    fill_db.fill_db_employers(user_input_emp)
    try:
        fill_db.fill_db_vacancies(user_input_vac)
    except TypeError:
        print("Данные не получены")
    db_manager = DBManager
    data = db_manager.get_companies_and_vacancies_count(user_input_emp, user_input_vac)
    for d in data:
        print(d)
    all_info = db_manager.get_all_vacancies(user_input_emp, user_input_vac)
    for info in all_info:
        print(info)
    salary = db_manager.get_avg_salary(user_input_vac)
    print(salary)
    top_salary = db_manager.get_vacancies_with_higher_salary(user_input_vac)
    for top in top_salary:
        print(top)
    vacancies = db_manager.get_vacancies_with_keyword(user_input_vac, "Python")
    for vac in vacancies:
        print(vac)

    while True:
        available_commands = "\nТеперь вам доступны следующие запросы для взаимодействия с базой данных:\n" \
                             "1. Список всех компаний и количество вакансий у каждой компании.\n" \
                             "2. Список всех вакансий с указанием названия компании, названия вакансии, " \
                             "зарплаты и ссылки на вакансию.\n" \
                             "3. Информация о средней зарплате по вакансиям.\n" \
                             "4. Список вакансий, у которых зарплата выше средней относительно всех вакансий.\n" \
                             "5. Поиск вакансий, содержащих ключевое слово в наименовании.\n" \
                             "6. Завершение работы программы.\n" \
                             "*  Помощь. Повторный вывод всех доступных к использованию команд.\n"
        print(available_commands)

        user_command = input('Для выбора команды, введите ее номер: ').lower().strip()
        print()

        if user_command == '1':
            data = db_manager.get_companies_and_vacancies_count(user_input_emp, user_input_vac)
            for d in data:
                print(d)
        elif user_command == '2':
            all_info = db_manager.get_all_vacancies(user_input_emp, user_input_vac)
            for info in all_info:
                print(info)
        elif user_command == '3':
            salary = db_manager.get_avg_salary(user_input_vac)
            print(salary)
        elif user_command == '4':
            top_salary = db_manager.get_vacancies_with_higher_salary(user_input_vac)
            for top in top_salary:
                print(top)
        elif user_command == '5':
            keyword = input("Введите ключевое слово для поиска вакансии: ").strip().lower()
            vacancies = db_manager.get_vacancies_with_keyword(user_input_vac, keyword)
            if not vacancies:
                print(f"Вакансии с ключевым словом '{keyword}' не найдено.")
            else:
                print("Результаты поиска:")
                for vac in vacancies:
                    print(vac)
        elif user_command == '6':
            print("Работа программы завершена.")
            break
        elif user_command == '*помощь':
            continue
        else:
            print("Введена неверная команда. Попробуйте еще раз.")


if __name__ == "__main__":
    main()
