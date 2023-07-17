import json
import time

from bs4 import BeautifulSoup
import fake_useragent
from lxml import etree
import pandas as pd
import requests
from sqlalchemy import create_engine, MetaData


URL = 'https://hh.ru/search/vacancy'
DB_FILE = '/Users/ilya/Documents/GitHub/python-education/data/hw2.db'
DB_TABLE = 'vacancies'
VACANCIES = 100
TEXT = "middle python"
SEARCH_FIELD = "name"
PER_PAGE = 50


def main():
    url_params = {
        "text": TEXT,
        "search_field": SEARCH_FIELD,
        "per_page": PER_PAGE,
    }

    engine = create_engine(f'sqlite:///{DB_FILE}')
    meta = MetaData()
    meta.create_all(engine)

    page = 1
    while page * url_params['per_page'] <= VACANCIES:
        url_params['page'] = page

        result = requests.get(URL, headers={'User-agent': fake_useragent.UserAgent().random}, params=url_params)

        if(result.status_code == 200):
            table = {
                'id': [],
                'company_name': [],
                'position': [],
                'job_description': [],
                'key_skills': [],
            }

            soup = BeautifulSoup(result.content.decode(), 'lxml')
            data = json.loads(soup.find('template', attrs={'id': 'HH-Lux-InitialState'}).text)
            for vacancy in data['vacancySearchResult']['vacancies']:
                time.sleep(1)
                url = vacancy.get('links').get('desktop')
                print(url)
                id = url.split('/')[-1]
                result = requests.get(url, headers={'User-agent': fake_useragent.UserAgent().random})
                soup = BeautifulSoup(result.content.decode(), "html.parser")
                dom = etree.HTML(str(soup))
                table['id'].append(id)
                table['company_name'].append(dom.xpath('//span[@class="vacancy-company-name"]/a/span')[0].text)
                table['position'].append(soup.find('h1', attrs={'data-qa': 'vacancy-title'}).text)   
                table['job_description'].append(soup.find('div', attrs={'data-qa': 'vacancy-description'}).text)
                skills = ', '.join([tag.text for tag in soup.findAll('span', attrs={'data-qa': 'bloko-tag__text'})])
                table['key_skills'].append(skills)
            
            pd.DataFrame(table).to_sql(
                DB_TABLE, 
                con=engine, 
                index=False, 
                if_exists='append'
            )

            page += 1


if __name__ == '__main__':
    main()
