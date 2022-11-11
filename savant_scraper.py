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


def get_statcast_data(_year, _date, _delta):
    url = f"https://baseballsavant.mlb.com/statcast_search/csv?hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea={str(_year)}%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={_date.strftime('%Y-%m-%d')}&game_date_lt={(_date.date() + _delta).strftime('%Y-%m-%d')}&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&type=details&all=true"
    res = requests.get(url, timeout=None).content
    return pd.read_csv(io.StringIO(res.decode('utf-8')), on_bad_lines='skip')


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
            update_start_date = datetime.strptime(res, '%Y-%m-%d').date() if type(res) == 'str' else res
            start_year        = update_start_date.year
            num_days          = (date.today() - update_start_date).days
            update            = True

    # build table from scratch if it doesn't exist
    # if table exists, scrape data starting from most recent date in the database
    if num_days > 0:
        for _year in tqdm(range(start_year, date.today().year+1), position=0, desc="Overall"):
            start_date = date(_year, 7, 23) if _year == 2020 else update_start_date if update else date(_year, 3, 20)
            periods    = 16 if start_date.year == 2020 else math.ceil(num_days/4) if update else 32
            _delta     = timedelta(days=3)

            with tqdm(total=periods, position=1, desc="Season", leave=False) as progress:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = {executor.submit(get_statcast_data, _year, _date, _delta) for _date in pd.date_range(start=start_date, periods=periods, freq="4D")}
                    for future in concurrent.futures.as_completed(futures):
                        df = future.result()
                        if df is not None and not df.empty:
                            print(df.head())
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
                        time.sleep(60)
    else:
        print("Database is up to date.")


if __name__ == '__main__':
    main()
        