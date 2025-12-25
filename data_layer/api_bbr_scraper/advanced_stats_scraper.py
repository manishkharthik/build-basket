from sqlalchemy import create_engine
import pandas as pd
import cloudscraper
from io import StringIO
import time
import unicodedata
import re

# ------------------------
# CONFIG
# ------------------------

SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
TABLE_NAME = "player_advanced_stats"

engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "darwin", "mobile": False}
)

# ------------------------
# CANONICAL NAME CLEANER
# ------------------------

def clean_name(name):
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("utf-8")
    return re.sub(r"\s+(jr|sr|ii|iii|iv)\.?$", "", name.lower().strip())

# ------------------------
# GET TARGET PLAYERS FROM DB
# ------------------------

QUERY = """
SELECT DISTINCT player_name, player_name_clean
FROM player_basic_stats
WHERE player_name_clean NOT IN (
  SELECT player_name_clean FROM player_advanced_stats
)
"""

targets = pd.read_sql(QUERY, engine)

print(f"Backfilling {len(targets)} players")

# ------------------------
# FIND PLAYER PAGE URL
# ------------------------

def find_player_url(player_name):
    clean = clean_name(player_name)
    last_initial = clean.split()[-1][0]

    index_url = f"https://www.basketball-reference.com/players/{last_initial}/"
    html = scraper.get(index_url).text
    tables = pd.read_html(StringIO(html))
    df = tables[0]

    df["clean"] = df["Player"].apply(clean_name)

    match = df[df["clean"] == clean]
    if match.empty:
        return None

    href = match.iloc[0]["Player"]
    link = match.iloc[0]["Player"]
    return f"https://www.basketball-reference.com{match.index[0]}"

# ------------------------
# SCRAPE PLAYER ADVANCED TABLE
# ------------------------

def scrape_player_advanced(player_name):
    clean = clean_name(player_name)
    last_initial = clean.split()[-1][0]

    index_url = f"https://www.basketball-reference.com/players/{last_initial}/"
    index_html = scraper.get(index_url).text
    index_df = pd.read_html(StringIO(index_html))[0]
    index_df["clean"] = index_df["Player"].apply(clean_name)

    row = index_df[index_df["clean"] == clean]
    if row.empty:
        return pd.DataFrame()

    player_id = row.iloc[0]["Player"]
    player_url = f"https://www.basketball-reference.com{row.index[0]}"

    print(f"Scraping {player_name}")

    html = scraper.get(player_url).text
    tables = pd.read_html(StringIO(html))

    adv = None
    for t in tables:
        if "PER" in t.columns and "TS%" in t.columns:
            adv = t
            break

    if adv is None:
        return pd.DataFrame()

    adv = adv[adv["Season"].isin(SEASONS)]
    adv["player_name"] = player_name
    adv["player_name_clean"] = clean

    adv.columns = (
        adv.columns.str.replace("%", "_pct", regex=False)
        .str.replace("/", "_", regex=False)
        .str.lower()
        .str.strip()
    )

    keep = [
        "player_name",
        "player_name_clean",
        "season",
        "age",
        "g",
        "mp",
        "ws_48",
        "ts_pct",
        "per",
        "ast_pct",
        "trb_pct",
        "bpm",
        "usg_pct",
        "fg3ar",
    ]

    return adv[keep]

# ------------------------
# RUN BACKFILL
# ------------------------

frames = []

for _, row in targets.iterrows():
    df = scrape_player_advanced(row["player_name"])
    if not df.empty:
        frames.append(df)
    time.sleep(1.5)

if not frames:
    print("No player advanced stats found.")
    exit(0)

adv_df = pd.concat(frames, ignore_index=True)
adv_df = adv_df.drop_duplicates(["player_name_clean", "season"])

adv_df.to_sql(
    TABLE_NAME,
    engine,
    if_exists="append",
    index=False
)

print("✅ Player-page advanced stats backfilled successfully.")
