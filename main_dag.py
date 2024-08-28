from pickle import FALSE

import requests
import re

from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook
from airflow.utils.dates import days_ago
import  datetime

def get_data(resource:str, offset:int, limit:int = 100)->dict:
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


def transform_data(data:list)->dict:
    """
    Преобразуем входящий массив данных для дальнейшей работы

    :param data: список значений вернувшихся по API
    :return: возвращает словарь вида
        {"table_name":
            "on_conflict": первичный_ключ_таблицы_при_наличии,
            "column_name": наименование колонок в PostgresSQL,
            "values": список значений
        }
    """

    today = datetime.datetime.today()
    company_data = {
        'on_conflict': 'inn',
        'column_name': ['inn', 'name', 'kpp', 'ogrn', 'site', 'email'],
        'values': [
            (
                element['vacancy']['company'].get('inn'), # используем get, что бы отловить ошибку в случае отсутсвия ключа
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
                        'category', 'education', 'experience', 'work_places', 'address', 'lng', 'lat', 'today', 'status'
                        ],
        'values': [
            (
                element['vacancy'].get('id'),
                element['vacancy'].get('source'),
                element['vacancy']['company'].get('inn'),
                element['vacancy'].get('creation-date'),
                re.sub(r'[^0-9]*(\d+)[^0-9]*', '\\1', element['vacancy'].get('salary')) if element['vacancy'].get('salary') is not None else None, #Если элемент не None ищем число и удаляем всё остальное
                element['vacancy'].get('salary_min'),
                element['vacancy'].get('salary_max'),
                element['vacancy'].get('job-name'),
                element['vacancy'].get('vac_url'),
                element['vacancy'].get('employment', 1),
                element['vacancy'].get('schedule'),
                re.sub(r'<[^>]+>|\\r|\\n|\\t|&nbsp;', ' ', element['vacancy'].get('duty')) if 'duty' in element['vacancy'] else None, #аналогично, но убираем html-теги в принципе можно убрать проверку подсунув пустую строку в get
                element['vacancy']['category'].get('specialisation'),
                element['vacancy']['requirement'].get('education'),
                element['vacancy']['requirement'].get('experience'),
                element['vacancy'].get('work_places'),
                element['vacancy']['addresses']['address'][0].get('location'),#нужно дописать парсер адресса, но не сейчас
                element['vacancy']['addresses']['address'][0].get('lng'),
                element['vacancy']['addresses']['address'][0].get('lat'),
                today,
                True
            )
            for element in data
        ]
    }
    return {'company': company_data, 'vacancy': vacancy_data}


def load_data(data:dict)->None:
    """
    Загружаем данные в PostgreSQL.
    В идеале хотел использовать pg_hook.insert_row, но с ним не работает ON CONFLICT

    :param data: dict словарь с таблицами
    :return: None
    """
    today = datetime.datetime.today()
    three_days_ago = today - datetime.timedelta(days=3)
    pg_hook = PostgresHook(postgres_conn_id='ul_db')
    for table_name, table_data in data.items():
        for row in table_data['values']:
            if table_data["on_conflict"]:
                sql = f'INSERT INTO "DC".{table_name} ({",".join(table_data["column_name"])}) VALUES ({",".join(["%s"] * len(row))}) ON CONFLICT ({table_data["on_conflict"]}) DO UPDATE SET "date"={today}'
            else:
                sql = f'INSERT INTO "DC".{table_name} ({",".join(table_data["column_name"])}) VALUES ({",".join(["%s"] * len(row))})'
            pg_hook.run(sql, parameters=row)
    sql_status_updater = f'UPDATE "DC".{table_name} SET status=%s WHERE date > %s'
    pg_hook.run(sql_status_updater, parameters=[False, three_days_ago])


def loader(**kwargs):
    resource = 'http://opendata.trudvsem.ru/api/v1/vacancies/region/7300000000000?'

    try:
        number_of_sheets = requests.get(f"{resource}limit=1").json()['meta']['total']
        number_of_sheets = number_of_sheets // 100 + 1
    except Exception as e:
        print(e)
        return

    for offset in range(number_of_sheets):
        data = get_data(offset=offset, resource=resource)
        transformed_data = transform_data(data)
        load_data(transformed_data)


with DAG(
        'rabotaru',
        schedule_interval='10 3 * * *',
        start_date=days_ago(1),
        catchup=False
) as dag:
    get_data_task = PythonOperator(
        task_id='loader',
        python_callable=loader,
        provide_context=True,
        do_xcom_push=True
    )