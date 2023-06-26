from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, insert, MetaData, Table, Column, String

from config import config


def main():
    engine = create_engine(f'sqlite:///{config["db_file"]}', echo=True)
    meta = MetaData()
    table = Table(
        config["telecom_table"], meta,
        Column('ogrn', String, primary_key = True), 
        Column('inn', String),
        Column('kpp', String), 
        Column('name', String), 
        Column('okved', String), 
    )
    meta.create_all(engine)

    entries = Path(config["data_dir"])
    for i, file in enumerate(entries.glob('*.json')):
        print(i)
        companies = pd.read_json(file)

        for row in companies.iterrows():
            ogrn, inn, kpp, name  = row[1]["ogrn"], row[1]["inn"], row[1]["kpp"], row[1]["name"] 
            try:
                okved = row[1]["data"]["СвОКВЭД"]["СвОКВЭДДоп"]["КодОКВЭД"]
            except (KeyError, TypeError):
                okved = ""
            else:
                if okved.startswith("61"):
                    stmt = insert(table).values(ogrn=ogrn, inn=inn, kpp=kpp, name=name, okved=okved).prefix_with('OR IGNORE')
                    with engine.connect() as conn:
                        conn.execute(stmt)
                        conn.commit()


if __name__ == '__main__':
    main()
