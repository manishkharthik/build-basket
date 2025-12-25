import time
import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

SEASONS = [
    "2021-22",
    "2022-23",
    "2023-24",
    "2024-25",
    "2025-26"
]

all_seasons = []

for season in SEASONS:
    print(f"Fetching {season}...")

    stats = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        per_mode_detailed="Totals",   # IMPORTANT
        measure_type_detailed_defense="Base",
        timeout=100
    )

    df = stats.get_data_frames()[0]

    df = df[[
        "PLAYER_ID",
        "PLAYER_NAME",
        "TEAM_ABBREVIATION",
        "AGE",
        "GP",
        "MIN",
        "W_PCT",
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TOV",
        "PF"
    ]]

    # keep TOT row if multi-team
    df = (
        df.sort_values("TEAM_ABBREVIATION")
          .drop_duplicates(subset=["PLAYER_ID"], keep="last")
    )

    df["SEASON"] = season
    all_seasons.append(df)

    time.sleep(1)

basic_df = pd.concat(all_seasons, ignore_index=True)

# standardize column names
basic_df = basic_df.rename(columns={
    "PLAYER_ID": "player_id",
    "PLAYER_NAME": "player_name",
    "TEAM_ABBREVIATION": "team_abbr",
    "AGE": "age",
    "GP": "gp",
    "MIN": "minutes",
    "W_PCT": "w_pct",
    "PTS": "pts",
    "REB": "reb",
    "AST": "ast",
    "STL": "stl",
    "BLK": "blk",
    "TOV": "tov",
    "PF": "pf",
    "SEASON": "season"
})

print("Total rows fetched:", len(basic_df))

# enforce player_id authority
player_map = pd.read_sql(
    "SELECT DISTINCT player_id FROM player_stats",
    engine
)

basic_df = basic_df.merge(
    player_map,
    on="player_id",
    how="inner"
)

print("Rows after enforcing player_id authority:", len(basic_df))
print("Unique players:", basic_df["player_id"].nunique())

basic_df.to_sql(
    "player_basic_stats",
    engine,
    if_exists="replace",
    index=False
)

print("✅ player_basic_stats table created")
