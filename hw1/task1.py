import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String

from config import config


def main():
    engine = create_engine(f'sqlite:///{config["db_file"]}')
    meta = MetaData()
    table = Table(
        config["db_table"], meta,
        Column('code', String, primary_key = True), 
        Column('parent_code', String),
        Column('section', String), 
        Column('name', String), 
        Column('comment', String), 
    )
    meta.create_all(engine)

    names = pd.read_json(config["json_file"], encoding="utf-8")
    names[['code', 'parent_code', 'section', 'name', 'comment']].to_sql(
        config["db_table"], 
        con=engine, 
        index=False, 
        if_exists='append'
    )


if __name__ == '__main__':
    main()
