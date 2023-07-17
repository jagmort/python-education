import asyncio

from aiohttp import ClientSession
import pandas as pd
import requests
from sqlalchemy import create_engine, MetaData


API_URL = 'https://api.hh.ru'
DB_FILE = '/Users/ilya/Documents/GitHub/python-education/data/hw2.db'
DB_TABLE = 'vacancies'
VACANCIES = 100
TEXT = "middle python"
SEARCH_FIELD = "name"
PER_PAGE = 20


async def get_vacancy(id, session):
    url = f'/vacancies/{id}'
    async with session.get(url=url) as response:
        vacancy_json = await response.json()
        return vacancy_json

async def main():
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

        result = requests.get(f"{API_URL}/vacancies", params=url_params)

        if(result.status_code == 200):
            table = {
                'id': [],
                'company_name': [],
                'position': [],
                'job_description': [],
                'key_skills': [],
            }

            async with ClientSession(API_URL) as session:
                tasks = []

                for item in result.json()['items']:
                    id = item['id']
                    tasks.append(asyncio.create_task(get_vacancy(id, session)))
                    results = await asyncio.gather(*tasks)

            for data in results:
                table['id'].append(data['id'])
                table['company_name'].append(data['employer']['name'])
                table['position'].append(data['name'])
                table['job_description'].append(data['description'])
                table['key_skills'].append(', '.join([d['name'] for d in data['key_skills']]))
                
            pd.DataFrame(table).to_sql(
                DB_TABLE, 
                con=engine, 
                index=False, 
                if_exists='append'
            )

        page += 1


if __name__ == '__main__':
    asyncio.run(main())
