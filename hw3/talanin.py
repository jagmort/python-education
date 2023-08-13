import asyncio
from collections import Counter
from datetime import datetime, timedelta
import itertools
import logging
import requests
import zipfile

from aiohttp import ClientSession
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd


default_args = {
    'owner': 'Talanin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_companies() -> None:
    def filter_fn(row):
        try:
            okved = row["data"]["СвОКВЭД"]["СвОКВЭДОсн"]["КодОКВЭД"]
        except (KeyError, TypeError):
            return False
        else:
            return (okved == "61" or okved.startswith("61."))

    logger = logging.getLogger(__name__)

    logger.info('Connecting to DB')
    try:
        sqlite_hook = SqliteHook(sqlite_conn_id='talanin_sqlite')
        engine = sqlite_hook.get_conn()    
    except Exception as e:
        logger.fatal(e, exc_info=True)
        quit()
    else:
        logger.info('Connection established')

    # EGRUL archive
    path_to_file = '/home/rtstudent/egrul.json.zip'

    logger.info('File reading')
    with zipfile.ZipFile(path_to_file, 'r') as zip_object:
        name_list = zip_object.namelist()
        count = len(name_list)
        for name in name_list:
            logger.info(name)
            with zip_object.open(name) as file:
                # json_data = file.read()
                companies = pd.read_json(file)

                # filter only 61 OKVED
                m = companies.apply(filter_fn, axis=1)
                filtered = companies[m]

                if not filtered.empty:
                    # create OKVED column
                    data = filtered["data"].apply(pd.Series)
                    svokved = data["СвОКВЭД"].apply(pd.Series)
                    svokvedosn = svokved["СвОКВЭДОсн"].apply(pd.Series)
                    okved = svokvedosn["КодОКВЭД"].apply(pd.Series)

                    # append and rename columns
                    final = pd.concat([filtered.drop(['data', 'data_version'], axis=1), okved], axis=1)
                    final.columns = ["ogrn", "inn", "kpp", "name", "full_name", "okved"]

                    logging.info(f'Insert to table {count}')
                    count -= 1
                    try:
                        final.to_sql(
                            'telecom_companies', 
                            con=engine, 
                            index=False, 
                            if_exists='append'
                        )
                    except Exception as e:
                        logging.fatal(e, exc_info=True)

def get_vacancies() -> None:

    async def get_vacancy(id, session):
        url = f'/vacancies/{id}'
        async with session.get(url=url) as response:
            vacancy_json = await response.json()
            return vacancy_json

    async def vacancies() -> None:

        url_params = {
            "text": "middle python",
            "search_field": "name",
            "per_page": 20,
        }

        logger = logging.getLogger(__name__)

        logger.info('Connecting to DB')
        try:
            sqlite_hook = SqliteHook(sqlite_conn_id='talanin_sqlite')
            engine = sqlite_hook.get_conn()    
        except Exception as e:
            logger.fatal(e, exc_info=True)
            quit()
        else:
            logger.info('Connection established')

        page = 1
        while page * url_params['per_page'] <= 100:
            url_params['page'] = page

            result = requests.get(f"https://api.hh.ru/vacancies", params=url_params)

            if(result.status_code == 200):
                table = {
                    'id': [],
                    'company_name': [],
                    'position': [],
                    'job_description': [],
                    'key_skills': [],
                }

                async with ClientSession('https://api.hh.ru') as session:
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
                    
                logging.info(f'Insert to table {page}')
                try:
                    pd.DataFrame(table).to_sql(
                        'vacancies', 
                        con=engine, 
                        index=False, 
                        if_exists='append'
                    )
                except Exception as e:
                    logging.fatal(e, exc_info=True)

            page += 1

    asyncio.run(vacancies())

def get_top_skills() -> None:
    sqlite_hook = SqliteHook(sqlite_conn_id='talanin_sqlite')
    engine = sqlite_hook.get_conn()
    df = pd.read_sql("SELECT key_skills FROM vacancies, telecom_companies WHERE telecom_companies.name LIKE '%' || vacancies.company_name || '%'", engine)
    skills = df['key_skills'].apply(lambda x: x.split(', '))

    # remove the empty skill and convert the df serie to a list
    skills_flat_list = filter(lambda x: x != '', list(itertools.chain(*skills)))

    counted = Counter(skills_flat_list).most_common(10)
    for i, d in enumerate(counted):
        logging.info(f'No {i + 1} skill {d[0]} occurs {d[1]} times')

# Flow
with DAG(
        dag_id='Talanin_test',
        default_args=default_args,
        description='Talanin DAG',
        start_date=datetime(2023, 8, 12),
        schedule_interval='@once'
    ) as dag:

    create_table_companies = SqliteOperator(
        task_id='create_table_companies',
        sqlite_conn_id='talanin_sqlite',
        sql='CREATE TABLE IF NOT EXISTS telecom_companies (ogrn varchar, inn varchar, kpp varchar, name varchar, full_name varchar, okved varchar)'
    )

    create_table_vacancies = SqliteOperator(
        task_id='create_table_vacancies',
        sqlite_conn_id='talanin_sqlite',
        sql='CREATE TABLE IF NOT EXISTS vacancies (id varchar, company_name varchar, position varchar, job_description varchar, key_skills varchar)'
    )

    get_companies_data = PythonOperator(
        task_id='get_companies',
        python_callable=get_companies,
    )

    get_vacancies_data = PythonOperator(
        task_id='get_vacancies',
        python_callable=get_vacancies,
    )

    get_top_skills_rating = PythonOperator(
        task_id='get_top_skills',
        python_callable=get_top_skills,
    )

    [create_table_companies >> get_companies_data, create_table_vacancies >> get_vacancies_data] >> get_top_skills_rating
