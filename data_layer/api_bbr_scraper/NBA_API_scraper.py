import time
import re
import pandas as pd
from sqlalchemy import create_engine, text
from nba_api.stats.endpoints import leaguedashplayerstats

# =========================
# CONFIG
# =========================
engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

START_SEASON = 2005
END_SEASON = 2025
SEASON_TYPE = "Regular Season"

# =========================
# HELPERS
# =========================
def season_str(year):
    return f"{year}-{str(year + 1)[-2:]}"

def clean_name(name: str) -> str:
    return re.sub(r"[^a-z]", "", name.lower())

# =========================
# MAIN INGEST
# =========================
rows = []

for year in range(START_SEASON, END_SEASON):
    season = season_str(year)
    print(f"Fetching {season}")

    df = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        season_type_all_star=SEASON_TYPE,
        per_mode_detailed="Totals"
    ).get_data_frames()[0]

    for _, r in df.iterrows():
        gp = int(r["GP"])
        minutes = int(r["MIN"])

        rows.append({
            "player_id": int(r["PLAYER_ID"]),
            "player_name_clean": clean_name(r["PLAYER_NAME"]),
            "season": season,
            "age": int(r["AGE"]) if not pd.isna(r["AGE"]) else None,
            "gp": gp,
            "mpg": round(minutes / gp, 2) if gp > 0 else None,
            "minutes": minutes,

            # shooting (raw only)       
            "fg_pct": r["FG_PCT"],
            "ft_pct": r["FT_PCT"],
            "fg3_pct": r["FG3_PCT"],

            # volume
            "pts": r["PTS"],
            "fgm": r["FGM"],
            "ftm": r["FTM"],
            "fg3m": r["FG3M"],

            # rebounding
            "reb": r["REB"],
            "oreb": r["OREB"],
            "dreb": r["DREB"],

            # defense
            "blk": r["BLK"],
            "stl": r["STL"],

            # playmaking
            "ast": r["AST"],
            "tov": r["TOV"],
        })

    time.sleep(1.2)  # NBA API throttle protection

df = pd.DataFrame(rows)

# =========================
# UPSERT INTO POSTGRES
# =========================
cols = list(df.columns)
insert_cols = ", ".join(cols)
update_cols = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in ("player_id", "season")])

sql = f"""
INSERT INTO player_season_stats ({insert_cols})
VALUES ({", ".join([f":{c}" for c in cols])})
ON CONFLICT (player_id, season)
DO UPDATE SET
{update_cols};
"""

with engine.begin() as conn:
    conn.execute(text(sql), df.to_dict(orient="records"))

print("✅ NBA API ingestion complete (raw stats only)")
