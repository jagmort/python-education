from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String

from config import config


def filter_fn(row):
    try:
        okved = row["data"]["СвОКВЭД"]["СвОКВЭДОсн"]["КодОКВЭД"]
    except (KeyError, TypeError):
        return False
    else:
        return okved.startswith("61")

def main():
    engine = create_engine(f'sqlite:///{config["db_file"]}')
    meta = MetaData()
    table = Table(
        config["telecom_table"], meta,
        Column('ogrn', String, primary_key = True), 
        Column('inn', String),
        Column('kpp', String), 
        Column('name', String), 
        Column('full_name', String), 
        Column('okved', String), 
    )
    meta.create_all(engine)

    entries = Path(config["data_dir"])

    for i, file in enumerate(entries.glob('*.json')):
        print(i)
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

            final.to_sql(
                config["telecom_table"], 
                con=engine, 
                index=False, 
                if_exists='append'
            )


if __name__ == '__main__':
    main()
