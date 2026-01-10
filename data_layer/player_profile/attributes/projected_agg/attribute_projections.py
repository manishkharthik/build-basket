import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os

ATTRS = [
    "Shooting", "Playmaking",
    "Perimeter_Defense", "Interior_Defense",
    "Rebounding", "Scoring",
    "Efficiency", "Impact",
]

# ---------------------------------------------------------
# 0. Load data
# ---------------------------------------------------------
engine = create_engine(
    "postgresql://postgres.fawypkcmahfkgnqhbmbb:MANFISHY0630@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres",
    pool_pre_ping=True
)

# Latest known attribute levels (baseline)
base_df = pd.read_sql("""
    SELECT *
    FROM player_attributes_base
    ORDER BY player_name_clean, season
""", engine)

latest_attrs = (
    base_df
    .sort_values("season")
    .groupby("player_name_clean", as_index=False)
    .tail(1)[["player_name_clean"] + ATTRS]
)

# Load % delta projections
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
delta_df = pd.read_csv(
    os.path.join(BASE_DIR, "player_final_projection.csv")
)

# ---------------------------------------------------------
# 1. Merge baseline + deltas
# ---------------------------------------------------------
df = delta_df.merge(
    latest_attrs,
    on="player_name_clean",
    how="left",
    suffixes=("_delta", "_base")
)

# ---------------------------------------------------------
# 2. Apply compounded % changes
# ---------------------------------------------------------
rows = []

for player, g in df.groupby("player_name_clean"):
    g = g.sort_values("year_ahead")

    # Start from current attributes
    levels = {
        attr: float(g.iloc[0][f"{attr}_base"])
        for attr in ATTRS
    }

    for _, r in g.iterrows():
        out = {
            "player_name_clean": player,
            "year_ahead": int(r["year_ahead"]),
        }

        for attr in ATTRS:
            pct_change = float(r[f"{attr}_delta"])
            levels[attr] *= (1 + pct_change)
            levels[attr] = np.clip(levels[attr], 0, 100)

            out[attr] = round(levels[attr], 6)

        rows.append(out)

# ---------------------------------------------------------
# 3. Final output
# ---------------------------------------------------------
projection_df = pd.DataFrame(rows)
print(projection_df.head())

# ---------------------------------------------------------
# 4. Upload to DB
# ---------------------------------------------------------
TABLE_NAME = "player_attribute_projections_5y"

projection_df.to_sql(
    TABLE_NAME,
    engine,
    if_exists="replace",
    index=False,
    method="multi",
    chunksize=1000,
)

print(f"Table `{TABLE_NAME}` written successfully.")