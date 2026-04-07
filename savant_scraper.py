import concurrent.futures
import io
import os
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import requests
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text
from tqdm import tqdm

from col_dtypes import dtypes, sqlalchemy_to_pandas_dtypes
from rate_limiter import RateLimiter

TABLE_NAME = "baseball_savant"

READ_DTYPES, DATE_COLS = sqlalchemy_to_pandas_dtypes(dtypes)

RATE_LIMITER = RateLimiter(3.0)

SESSION = requests.Session()
SESSION.headers.update({"Accept-Encoding": "gzip, deflate", "User-Agent": "savant_scraper"})

SWING_EVENTS = [
    "foul",
    "foul_pitchout",
    "foul_tip",
    "hit_into_play",
    "swinging_pitchout",
    "swinging_strike",
    "swinging_strike_blocked",
]

TAKE_EVENTS = [
    "automatic_ball",
    "automatic_strike",
    "ball",
    "blocked_ball",
    "called_strike",
]

CONTACT_EVENTS = [
    "foul",
    "foul_pitchout",
    "hit_into_play",
]


def db_engine() -> Engine:
    load_dotenv()
    CONNECTION_STRING = os.getenv("CONNECTION_STRING")
    return create_engine(CONNECTION_STRING)


def get_statcast_data(
    year: int,
    game_date: datetime,
    delta: timedelta,
    dtypes: dict[str, str] = READ_DTYPES,
    parse_dates: list[str] = DATE_COLS,
    session: requests.Session = SESSION,
    rate_limiter: RateLimiter = RATE_LIMITER,
) -> pd.DataFrame:
    rate_limiter.wait()

    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv?"
        f"hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC="
        f"&hfSea={year}%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws="
        f"&batter_stands=&hfSA=&game_date_gt={game_date.strftime('%Y-%m-%d')}"
        f"&game_date_lt={(game_date.date() + delta).strftime('%Y-%m-%d')}"
        "&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn="
        "&hfBBT=&hfFlag=&metric_1=&group_by=name&min_pitches=0&min_results=0"
        "&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed"
        "&sort_order=desc&type=details&all=true"
    )

    response = session.get(url, timeout=None)

    if response.status_code != 200:
        response.raise_for_status()

    content = response.content

    return pd.read_csv(
        io.StringIO(content.decode("utf-8")),
        on_bad_lines="skip",
        dtype=dtypes,
        parse_dates=parse_dates,
        na_values=["null", "NULL", "", "None", "NA"],
    )


def main():
    # connect to database
    engine = db_engine()

    # determine start date based on whether previous table exists
    # if table exists, set start date to most recent date in table
    if not sqlalchemy.inspect(engine).has_table(TABLE_NAME):
        start_year = 2015
        num_days = 1
        update = False
    else:
        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT game_date FROM {TABLE_NAME} ORDER BY game_date DESC")).first()[0]
            update_start_date = datetime.strptime(res, "%Y-%m-%d").date() if isinstance(res, str) else res
            start_year = update_start_date.year
            num_days = (date.today() - update_start_date).days
            update = True

    # build table from scratch if it doesn't exist
    # if table exists, scrape data starting from most recent date in the database
    if num_days > 0:
        for year in tqdm(range(start_year, date.today().year + 1), position=0, desc="Overall"):
            if update and year == update_start_date.year:
                start_date = update_start_date
            else:
                start_date = date(year, 7, 23) if year == 2020 else date(year, 3, 1)

            end_date = min(date(year, 10, 31), date.today())

            if start_date > end_date:
                continue

            chunk_starts = list(pd.date_range(start=start_date, end=end_date, freq="4D"))
            delta = timedelta(days=3)

            with tqdm(total=len(chunk_starts), position=1, desc="Season", leave=False) as progress:
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                    futures = {executor.submit(get_statcast_data, year, game_date, delta) for game_date in chunk_starts}
                    for future in concurrent.futures.as_completed(futures):
                        df = future.result()
                        if df is not None and not df.empty:
                            df["player_name"] = (
                                df["player_name"]
                                .str.normalize("NFKD")
                                .str.encode("ascii", errors="ignore")
                                .str.decode("utf-8")
                            )
                            is_left = df["stand"].eq("L").fillna(False)
                            in_play = df["bb_type"].notna()
                            swing_desc = df["description"].isin(SWING_EVENTS).fillna(False)
                            take_desc = df["description"].isin(TAKE_EVENTS).fillna(False)
                            zone = pd.to_numeric(df["zone"], errors="coerce")

                            df["horizontal_launch_speed"] = np.where(
                                in_play,
                                df["launch_speed"] * np.cos(np.radians(df["launch_angle"] - 25)),
                                0,
                            )

                            angle = np.arctan((df["hc_x"] - 125.42) / (198.27 - df["hc_y"])) * 180 / np.pi * 0.75
                            df["spray_angle"] = np.where(is_left, -angle, angle)

                            df["o_swing"] = np.where((zone > 10).fillna(False) & swing_desc, 1, 0)
                            df["z_swing"] = np.where((zone < 10).fillna(False) & swing_desc, 1, 0)
                            df["o_take"] = np.where((zone > 10).fillna(False) & take_desc, 1, 0)
                            df["z_take"] = np.where((zone < 10).fillna(False) & take_desc, 1, 0)
                            df["swing"] = np.where(df["description"].isin(SWING_EVENTS), 1, 0)
                            df["contact"] = np.where(df["description"].isin(CONTACT_EVENTS), 1, 0)

                            df.to_sql(
                                TABLE_NAME,
                                engine,
                                if_exists="append",
                                index=False,
                                dtype=dtypes,
                            )
                        progress.update(1)
    else:
        print("Database is up to date.")


if __name__ == "__main__":
    main()
