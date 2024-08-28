import time
from math import trunc
from turtledemo.clock import datum

import requests
import json
from pprint import pprint
import yaml
from yaml import safe_load


def get_area(area='Ульяновская область'):
    '''
    Получаем id территории

    :param area: название территории
    :return:
    '''
    url = 'https://api.hh.ru/areas'
    response = requests.get(url)
    response.raise_for_status()  # Проверка на ошибки HTTP
    data = response.json()[0]
    data = next((item for item in data['areas'] if item['name'] == area), None)
    if data is None:
        raise ValueError(f"Area '{area}' not found")
    return data

def get_data(url: str, **params):
    response = requests.get(url, params=params)
    response.raise_for_status()  # Проверка на ошибки HTTP
    return response.json()

def transform_data(data:dict)->dict:
    """

    :param data:
    :return:
    """
    vacancy = {}
    company = {}
    return

professional_role = get_data('https://api.hh.ru/professional_roles')
area = get_area('Ульяновская область')
vacancys = 0
sleep = 0
for category in professional_role['categories']:
    for role in category['roles']:
        # print(role['id'])
        page = 0
        pages = 1
        try:
            while page!=pages:
                data = get_data('https://api.hh.ru/vacancies', area=area['id'],
                                page=page, per_page=100, professional_role=role['id'])
                with open(f'HH/HH_{category['name']}_{role['name'].split("/")[0]}_{page}.yaml', 'w', encoding='utf-8') as file:
                    yaml.dump(data, file, allow_unicode=True)
                page += 1
                sleep += 1
                vacancys += data['found'] if page == 1 else 0
                pages = data['pages']
                print(f'страница {page - 1} из {pages} вакансий {vacancys}')
                if sleep % 10 == 0:
                    print('sleep:', sleep)
                    time.sleep(3)#для избегания капчи

        except requests.RequestException as e:
            print(f"An error occurred: {e}")
print(vacancys)
