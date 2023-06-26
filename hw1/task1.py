import pandas as pd
from sqlalchemy import create_engine, select, func, MetaData, Table, Column, String

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
    print(names)

    names[['code', 'parent_code', 'section', 'name', 'comment']].to_sql(
        config["db_table"], 
        con=engine, 
        index=False, 
        if_exists='append'
    )

    stmt = select(func.count()).select_from(table)
    with engine.connect() as conn:
        results = conn.execute(stmt)
        print(f'Table rows: {next(results)[0]}')


if __name__ == '__main__':
    main()
