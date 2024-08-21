import requests
from urllib.parse import urljoin
import re
from pprint import pprint

from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook
from airflow.utils.dates import days_ago


def get_data(**kwargs):
    offset = kwargs['offset']
    resource = kwargs['resource']
    req = requests.get(f"{resource}limit=100&offset={offset}")
    return req.json()['results']['vacancies']


def transform_data(data):
    company_data = {
        'on_conflict': 'inn',
        'column_name': ['inn', 'name', 'kpp', 'ogrn', 'site', 'email'],
        'values': [
            (
                element['vacancy']['company'].get('inn'),
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
                re.sub(r'[^0-9]*(\d+)[^0-9]*', '\\1', element['vacancy'].get('salary')) if element['vacancy'].get(
                    'salary') is not None else None,
                element['vacancy'].get('salary_min'),
                element['vacancy'].get('salary_max'),
                element['vacancy'].get('job-name'),
                element['vacancy'].get('vac_url'),
                element['vacancy'].get('employment', 1),
                element['vacancy'].get('schedule'),
                re.sub(r'<[^>]+>|\\r|\\n|\\t|&nbsp;', ' ', element['vacancy'].get('duty')) if 'duty' in element[
                    'vacancy'] else None,
                element['vacancy']['category'].get('specialisation'),
                element['vacancy']['requirement'].get('education'),
                element['vacancy']['requirement'].get('experience'),
                element['vacancy'].get('work_places'),
                element['vacancy']['addresses']['address'][0].get('location'),
                element['vacancy']['addresses']['address'][0].get('lng'),
                element['vacancy']['addresses']['address'][0].get('lat'),
            )
            for element in data
        ]
    }
    return {'company': company_data, 'vacancy': vacancy_data}


def load_data(data):
    pg_hook = PostgresHook(postgres_conn_id='ul_db')
    for table_name, table_data in data.items():
        for row in table_data['values']:
            if table_data["on_conflict"]:
                sql = f'INSERT INTO "DC".{table_name} ({",".join(table_data["column_name"])}) VALUES ({",".join(["%s"] * len(row))}) ON CONFLICT ({table_data["on_conflict"]}) DO NOTHING'
            else:
                sql = f'INSERT INTO "DC".{table_name} ({",".join(table_data["column_name"])}) VALUES ({",".join(["%s"] * len(row))})'
            pg_hook.run(sql, parameters=row)


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