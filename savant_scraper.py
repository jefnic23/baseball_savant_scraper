from tqdm import tqdm
from datetime import date, timedelta
import pandas as pd
import io
import requests
import time
import os


if __name__ == '__main__':
    # make directory to house savant data
    if not os.path.exists('data'):
        os.mkdir('data')

    # loop through all savant years
    for year in tqdm(range(2015, date.today().year+1)):
        df = []
        # 2020 is shortened season, so start date is pushed up
        if year == 2020:
            start_date = date(year, 7, 23)
        else:
            start_date = date(year, 3, 20)
        end_date = date(year, 11, 30)
        # savant limits queries to 40,000 rows, so get 6 days' worth of data at a time
        for d in tqdm(pd.date_range(start=start_date, end=end_date, freq="7D")):
            delta = timedelta(days=6)
            url = f"https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea={str(year)}%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={d.strftime('%Y-%m-%d')}&game_date_lt={(d.date() + delta).strftime('%Y-%m-%d')}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details&"
            req = requests.get(url).content
            df.append(pd.read_csv(io.StringIO(req.decode('utf-8'))))
            time.sleep(30)
        df = pd.concat(df)
        df.to_csv(f"data/savant_{year}.csv")
