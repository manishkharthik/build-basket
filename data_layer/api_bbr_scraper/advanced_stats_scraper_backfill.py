from sqlalchemy import create_engine
import pandas as pd
import cloudscraper
from io import StringIO
import time
import unicodedata
import re

# ------------------------
# 1. CONFIG
# ------------------------

TABLE_NAME = "player_advanced_stats"

engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "darwin", "mobile": False}
)

# ------------------------
# 2. PLAYER → URL MAPPING
# ------------------------

PLAYER_URLS = {
    "aj green": "https://www.basketball-reference.com/players/g/greenaj01.html",
    "alperen sengun": "https://www.basketball-reference.com/players/s/sengual01.html",
    "bogdan bogdanovic": "https://www.basketball-reference.com/players/b/bogdabo01.html",
    "chris manon": "https://www.basketball-reference.com/players/m/manonch01.html",
    "dario saric": "https://www.basketball-reference.com/players/s/saricda01.html",
    "david jones garcia": "https://www.basketball-reference.com/players/j/jonesda06.html",
    "dennis schroder": "https://www.basketball-reference.com/players/s/schrode01.html",
    "egor demin": "https://www.basketball-reference.com/players/d/demineg01.html",
    "jonas valanciunas": "https://www.basketball-reference.com/players/v/valanjo01.html",
    "jusuf nurkic": "https://www.basketball-reference.com/players/n/nurkiju01.html",
    "karlo matkovic": "https://www.basketball-reference.com/players/m/matkoka01.html",
    "kasparas jakucionis": "https://www.basketball-reference.com/players/j/jakucka01.html",
    "kristaps porzingis": "https://www.basketball-reference.com/players/p/porzikr01.html",
    "luka doncic": "https://www.basketball-reference.com/players/d/doncilu01.html",
    "moussa diabate": "https://www.basketball-reference.com/players/d/diabamo01.html",
    "nikola jokic": "https://www.basketball-reference.com/players/j/jokicni01.html",
    "nikola jovic": "https://www.basketball-reference.com/players/j/jovicni01.html",
    "nikola vucevic": "https://www.basketball-reference.com/players/v/vucevni01.html",
    "nolan traore": "https://www.basketball-reference.com/players/t/traorno01.html",
    "ronald holland": "https://www.basketball-reference.com/players/h/hollaro01.html",
    "tidjane salaun": "https://www.basketball-reference.com/players/s/salauti01.html",
}

# ------------------------
# 3. NAME CLEANER
# ------------------------

def clean_player_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("utf-8")
    name = name.lower().strip()
    name = re.sub(r"\s+(jr|sr|ii|iii|iv)\.?$", "", name)
    return name

# ------------------------
# 4. SCRAPE PLAYER ADVANCED TABLE
# ------------------------

def scrape_player_advanced(player_name_clean: str, url: str) -> pd.DataFrame:
    print(f"Scraping {player_name_clean} → {url}")

    html = scraper.get(url).text
    tables = pd.read_html(StringIO(html))

    adv = None
    for t in tables:
        if "PER" in t.columns and "TS%" in t.columns:
            adv = t
            break

    if adv is None:
        print(f"⚠️ No advanced table found for {player_name_clean}")
        return pd.DataFrame()

    # Keep NBA rows only
    adv = adv[adv["Lg"] == "NBA"].copy()

    # Normalize column names
    adv.columns = (
        adv.columns
        .str.replace("%", "_pct", regex=False)
        .str.replace("/", "_", regex=False)
        .str.lower()
        .str.strip()
    )

    adv["player_name"] = player_name_clean
    adv["player_name_clean"] = player_name_clean

    keep_cols = [
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

    return adv[adv.columns.intersection(keep_cols)]

# ------------------------
# 5. RUN BACKFILL
# ------------------------

frames = []

for name_clean, url in PLAYER_URLS.items():
    df = scrape_player_advanced(name_clean, url)
    if not df.empty:
        frames.append(df)
    time.sleep(1.5)

if not frames:
    print("ℹ️ No advanced stats found for any players.")
    exit(0)

adv_df = pd.concat(frames, ignore_index=True)

# Deduplicate (safe to re-run)
adv_df = adv_df.drop_duplicates(
    subset=["player_name_clean", "season"]
)

# Append to Supabase
adv_df.to_sql(
    TABLE_NAME,
    engine,
    if_exists="append",
    index=False
)

print("✅ Player-page advanced stats backfilled successfully.")
