import pandas as pd
import requests
from sqlalchemy import create_engine, MetaData


API_URL = 'https://api.hh.ru/vacancies'
DB_FILE = '/Users/ilya/Documents/GitHub/python-education/data/hw2.db'
DB_TABLE = 'vacancies'
VACANCIES = 100
TEXT = "middle python"
SEARCH_FIELD = "name"
PER_PAGE = 20


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

        result = requests.get(API_URL, params=url_params)

        if(result.status_code == 200):
            table = {
                'id': [],
                'company_name': [],
                'position': [],
                'job_description': [],
                'key_skills': [],
            }

            for item in result.json()['items']:
                url = item['url']
                result = requests.get(url)
                data = result.json()
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
    main()
