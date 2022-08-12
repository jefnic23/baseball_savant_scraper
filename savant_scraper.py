from tqdm import tqdm
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import requests
import random
import time
import math
import os
import io


def createEngine():
    load_dotenv()
    USER = os.getenv('USER')
    PSWD = os.getenv('PSWD')
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')
    NAME = os.getenv('NAME')
    return create_engine(f'postgresql://{USER}:{PSWD}@{HOST}:{PORT}/{NAME}')


def main():
    # connect to database
    engine = createEngine()

    # determine start date based on whether previous table exists
    # if table exists, set start date to most recent date in table
    if not sqlalchemy.inspect(engine).has_table("baseball_savant"):
        start_year = 2015
        num_days = -1
        update = False
    else:
        with engine.connect() as conn:
            res = conn.execute('SELECT game_date FROM baseball_savant ORDER BY game_date DESC').first()[0]
            update_start_date = datetime.strptime(res, '%Y-%m-%d').date()
            start_year = update_start_date.year
            num_days = (date.today() - update_start_date).days
            update = True

    # build table from scratch if it doesn't exist
    # if table exists, scrape data starting from most recent date in the database
    if num_days > 0:
        for y in tqdm(range(start_year, date.today().year+1), position=0, desc="Overall"):
            start_date = date(y, 7, 23) if y == 2020 else update_start_date if update else date(y, 3, 20)
            periods = 16 if start_date.year == 2020 else math.ceil(num_days/7) if update else 32

            # savant limits queries to 40,000 rows, so get 6 days of data at a time
            for d in tqdm(pd.date_range(start=start_date, periods=periods, freq="7D"), position=1, desc="Season", leave=False):
                delta = timedelta(days=6)
                url = f"https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea={str(y)}%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={d.strftime('%Y-%m-%d')}&game_date_lt={(d.date() + delta).strftime('%Y-%m-%d')}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details&"
                req = requests.get(url).content
                pd.read_csv(io.StringIO(req.decode('utf-8'))).to_sql("baseball_savant", engine, if_exists="append")
                time.sleep(random.randrange(15,30))
    else:
        print("Database is up to date.")


if __name__ == '__main__':
    main()
