import requests
import re
import sqlite3

def get_data(resource: str, offset: int, limit: int = 100) -> dict:
    """
    Функция для получения данных с OpenData.
    Принимает адресс и условия смещения и ограничения

    :param limit: ограничение на количество записей
    :param resource: источник
    :param offset: смещение
    :return: dict значенией
    """
    req = requests.get(f"{resource}limit={limit}&offset={offset}")
    return req.json()['results']['vacancies']


def transform_data(data: list) -> dict:
    """
    Преобразуем входящий массив данных для дальнейшей работы

    :param data: список значений вернувшихся по API
    :return: возвращает словарь вида
        {"table_name":
            "on_conflict": первичный_ключ_таблицы_при_наличии,
            "column_name": наименование колонок в SQLite,
            "values": список значений
        }
    """
    company_data = {
        'on_conflict': 'inn',
        'column_name': ['inn', 'name', 'kpp', 'ogrn', 'site', 'email'],
        'values': [
            (
                element['vacancy']['company'].get('inn'),  # используем get, что бы отловить ошибку в случае отсутсвия ключа
                element['vacancy']['company'].get('name'),
                element['vacancy']['company'].get('kpp'),
                element['vacancy']['company'].get('ogrn'),
                element['vacancy']['company'].get('site', element['vacancy']['company'].get('url')),
                element['vacancy']['company'].get('email')
            )
            for element in data
        ]
    }

    vacancy_data = {
        'on_conflict': 'id',
        'column_name': ['id', 'source', 'inn_company', 'creation_date', "salary", "salary_min",
                        'salary_max', 'job_name', 'vac_url', 'employment', 'schedule', 'duty',
                        'category', 'education', 'experience', 'work_places', 'address', 'lng', 'lat'
                        ],
        'values': [
            (
                element['vacancy'].get('id'),
                element['vacancy'].get('source'),
                element['vacancy']['company'].get('inn'),
                element['vacancy'].get('creation-date'),
                re.sub(r'[^0-9]*(\d+)[^0-9]*', '\\1', element['vacancy'].get('salary')) if element['vacancy'].get('salary') is not None else None,  # Если элемент не None ищем число и удаляем всё остальное
                element['vacancy'].get('salary_min'),
                element['vacancy'].get('salary_max'),
                element['vacancy'].get('job-name'),
                element['vacancy'].get('vac_url'),
                element['vacancy'].get('employment', 1),
                element['vacancy'].get('schedule'),
                re.sub(r'<[^>]+>|\\r|\\n|\\t|&nbsp;', ' ', element['vacancy'].get('duty')) if 'duty' in element['vacancy'] else None,  # аналогично, но убираем html-теги в принципе можно убрать проверку подсунув пустую строку в get
                element['vacancy']['category'].get('specialisation'),
                element['vacancy']['requirement'].get('education'),
                element['vacancy']['requirement'].get('experience'),
                element['vacancy'].get('work_places'),
                element['vacancy']['addresses']['address'][0].get('location'),  # нужно дописать парсер адресса, но не сейчас
                element['vacancy']['addresses']['address'][0].get('lng'),
                element['vacancy']['addresses']['address'][0].get('lat'),
            )
            for element in data
        ]
    }
    return {'company': company_data, 'vacancy': vacancy_data}


def create_tables(conn: sqlite3.Connection) -> None:
    """
    Создаем таблицы в SQLite, если они не существуют.

    :param conn: соединение с SQLite
    :return: None
    """
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company (
        inn TEXT PRIMARY KEY,
        name TEXT,
        kpp TEXT,
        ogrn TEXT,
        site TEXT,
        email TEXT
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS vacancy (
        id TEXT PRIMARY KEY,
        source TEXT,
        inn_company TEXT,
        creation_date TEXT,
        salary TEXT,
        salary_min REAL,
        salary_max REAL,
        job_name TEXT,
        vac_url TEXT,
        employment INTEGER,
        schedule TEXT,
        duty TEXT,
        category TEXT,
        education TEXT,
        experience TEXT,
        work_places TEXT,
        address TEXT,
        lng REAL,
        lat REAL,
        FOREIGN KEY (inn_company) REFERENCES company (inn)
    )
    ''')
    conn.commit()


def load_data(data: dict, conn: sqlite3.Connection) -> None:
    """
    Загружаем данные в SQLite.

    :param data: dict словарь с таблицами
    :param conn: соединение с SQLite
    :return: None
    """
    cursor = conn.cursor()
    for table_name, table_data in data.items():
        for row in table_data['values']:
            placeholders = ', '.join(['?'] * len(row))
            if table_data["on_conflict"]:
                sql = f'INSERT OR IGNORE INTO {table_name} ({",".join(table_data["column_name"])}) VALUES ({placeholders})'
            else:
                sql = f'INSERT INTO {table_name} ({",".join(table_data["column_name"])}) VALUES ({placeholders})'
            cursor.execute(sql, row)
    conn.commit()


def loader():
    resource = 'http://opendata.trudvsem.ru/api/v1/vacancies/region/7300000000000?'

    try:
        number_of_sheets = requests.get(f"{resource}limit=1").json()['meta']['total']
        number_of_sheets = number_of_sheets // 100 + 1
    except Exception as e:
        print(e)
        return

    conn = sqlite3.connect('database.db')
    create_tables(conn)
    for offset in range(number_of_sheets):
        data = get_data(offset=offset, resource=resource)
        transformed_data = transform_data(data)
        load_data(transformed_data, conn)
    conn.close()


if __name__ == "__main__":
    loader()