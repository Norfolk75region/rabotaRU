import re

import yaml
import requests

from bs4 import BeautifulSoup
from pprint import pprint

def get_data(**kwargs):
    req = requests.get(resource + f'limit=100&offset={offset}')
    data = req.json()['results']['vacancies']
    with (open('data.yaml', 'a',encoding='utf-8')) as file:
        yaml.safe_dump(data, file, allow_unicode=True, default_flow_style=False)
        # json.dump(data,file,ensure_ascii=False)
        print("done get_data")
    return data

def transform_data(**kwargs):
    pass

def load_data(**kwargs):
    data = kwargs.get('data', [])  # Assuming 'data' is passed as a keyword argument

    for element in data:
        # element['vacancy']['duty'] = re.sub(r'<.*?>', '', element['vacancy']['duty'])
        company = {
            "inn": element['vacancy']['company'].get('inn'),
            "name": element['vacancy']['company'].get('name'),
            "kpp": element['vacancy']['company'].get('kpp'),
            "ogrn": element['vacancy']['company'].get('ogrn'),
            "site": element['vacancy']['company'].get('site', element['vacancy']['company'].get('url')),
            "email": element['vacancy']['company'].get('email')
        }

        company_insert = f'''INSERT INTO "DC"."company" 
        ("{'","'.join(company.keys())}") 
        VALUES ({', '.join(['%s'] * len(company))})'''

        # company_values = list()
        # for key in company.keys():
        #     print(company[key], type(company[key]), isinstance(company[key],str))
        #     if not company[key]:
        #         company_values.append(None)
        #     else:
        #         if  company[key].isdecimal():
        #             company_values.append(company[key])
        #         else:
        #             company_values.append(f"'{company[key]}'")
        #
        # print(company_values)
        company_values = tuple(company[key] if isinstance(company[key],str) and company[key].isdecimal() else f"'{company[key]}'" for key in company.keys())

        vacancy = {
            "id": element['vacancy'].get('id'),
            "source": element['vacancy'].get('source'),
            "inn_company": element['vacancy']['company'].get('inn'),
            "creation_date": element['vacancy'].get('creation-date'),
            "salary": element['vacancy'].get('salary'),
            "salary_min": element['vacancy'].get('salary_min'),
            "salary_max": element['vacancy'].get('salary_max'),
            "job_name": element['vacancy'].get('job-name'),
            "vac_url": element['vacancy'].get('vac_url'),
            "employment": element['vacancy'].get('employment', 1),
            "schedule": element['vacancy'].get('schedule'),
            "duty": re.sub(r'<.*?>', '', element['vacancy'].get('duty')) if 'duty' in element['vacancy'] else None,
            "category": element['vacancy']['category'].get('specialisation'),
            "education": element['vacancy']['requirement'].get('education'),
            "experience": element['vacancy']['requirement'].get('experience'),
            "work_places": element['vacancy'].get('work_places'),
            "address": element['vacancy']['addresses']['address'][0].get('location'),
            "lng": element['vacancy']['addresses']['address'][0].get('lng'),
            "lat": element['vacancy']['addresses']['address'][0].get('lat'),
        }
        vacancy_insert = f'''INSERT INTO "DC"."company" 
        ("{'","'.join(vacancy.keys())}") 
        VALUES ({', '.join(['%s'] * len(vacancy))})'''
        vacancy_values = tuple(vacancy[key] if isinstance(vacancy[key],str) and vacancy[key].isdecimal() else f"'{vacancy[key]}'" for key in vacancy.keys())

        pprint(company_insert,)
        pprint(vacancy_insert,)


if __name__== '__main__':
    resource = 'http://opendata.trudvsem.ru/api/v1/vacancies/region/7300000000000?'
    data = list()

    try:
        number_of_sheets = requests.get(resource + 'limit=1').json()['meta']['total']
        number_of_sheets = number_of_sheets//100 + 1
    except Exception as e:
        print(e)

    for offset in range(number_of_sheets):
        data = get_data(offset=offset)
        # print(data[0]['vacancy']['id'])
        transform_data()
        load_data(data=data)