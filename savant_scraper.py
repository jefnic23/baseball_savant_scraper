import concurrent.futures
import io
import math
import os
import time
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import requests
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import create_engine
from tqdm import tqdm

from col_dtypes import dtypes


def db_engine():
    load_dotenv()
    USER = os.getenv('USER')
    PSWD = os.getenv('PSWD')
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')
    NAME = os.getenv('NAME')
    return create_engine(f'postgresql://{USER}:{PSWD}@{HOST}:{PORT}/{NAME}')


def spray_angle(coord_x, coord_y, hand):
    '''
    derives spray angle given x and y hit coordinates.
    if batter is left-handed, invert the spray angle.
    '''
    try:
        spray_angle = np.arctan((coord_x - 125.42)/(198.27 - coord_y)) * 180 / np.pi * 0.75
        if hand == "L":
            return 0 - spray_angle
        else:
            return spray_angle
    except ZeroDivisionError:
        return np.nan


def get_statcast_data(_year, _date, _delta):
    url = f"https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea={str(_year)}%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={_date.strftime('%Y-%m-%d')}&game_date_lt={(_date.date() + _delta).strftime('%Y-%m-%d')}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details&"
    res = requests.get(url, timeout=None).content
    return pd.read_csv(io.StringIO(res.decode('utf-8')))


def main():
    # connect to database
    engine = db_engine()

    # determine start date based on whether previous table exists
    # if table exists, set start date to most recent date in table
    if not sqlalchemy.inspect(engine).has_table("baseball_savant"):
        start_year = 2015
        num_days   = 1
        update     = False
    else:
        with engine.connect() as conn:
            res               = conn.execute('SELECT game_date FROM baseball_savant ORDER BY game_date DESC').first()[0]
            update_start_date = datetime.strptime(res, '%Y-%m-%d').date()
            start_year        = update_start_date.year
            num_days          = (date.today() - update_start_date).days
            update            = True

    # build table from scratch if it doesn't exist
    # if table exists, scrape data starting from most recent date in the database
    if num_days > 0:
        for _year in tqdm(range(start_year, date.today().year+1), position=0, desc="Overall"):
            start_date = date(_year, 7, 23) if _year == 2020 else update_start_date if update else date(_year, 3, 20)
            periods    = 16 if start_date.year == 2020 else math.ceil(num_days/7) if update else 32
            _delta     = timedelta(days=6)

            # savant limits queries to 40,000 rows, so get 6 days of data at a time
            with tqdm(total=periods, position=1, desc="Season", leave=False) as progress:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = {executor.submit(get_statcast_data, _year, _date, _delta) for _date in pd.date_range(start=start_date, periods=periods, freq="7D")}
                    for future in concurrent.futures.as_completed(futures):
                        df = future.result()
                        df['spray_angle'] = np.arctan((df['hc_x'] - 125.42) / (198.27 - df['hc_y'])) * 180 / np.pi * 0.75
                        df['spray_angle'] = np.where(df['stand'] == 'L', 0 - df['spray_angle'], df['spray_angle'])
                        df.to_sql(
                            "baseball_savant", 
                            engine, 
                            if_exists="append",
                            index=False,
                            dtype=dtypes
                        )
                        progress.update(1)
                    time.sleep(30)
    else:
        print("Database is up to date.")


if __name__ == '__main__':
    main()
        