import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os

# Load data and define attributes
engine = create_engine(
    "postgresql://postgres.fawypkcmahfkgnqhbmbb:MANFISHY0630@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres",
    pool_pre_ping=True
)

ATTRS = [
    "Efficiency",
    "Impact",
    "Interior_Defense",
    "Perimeter_Defense",
    "Playmaking",
    "Rebounding",
    "Scoring",
    "Shooting",
]

current_df = pd.read_sql("""
    SELECT *
    FROM player_attributes_current
    ORDER BY player_name_clean
""", engine)

projected_df = pd.read_sql("""
    SELECT *
    FROM player_attributes_projections
    ORDER BY player_name_clean, year_ahead
""", engine)

current_df["year_ahead"] = 0
combined_df = pd.concat(
    [current_df, projected_df],
    ignore_index=True
)

rows = []

for year in sorted(combined_df["year_ahead"].unique()):
    year_df = combined_df[combined_df["year_ahead"] == year]

    for attr in ATTRS:
        # Compute percentile ranks (0–1)
        pct = year_df[attr].rank(pct=True)

        for i, (_, r) in enumerate(year_df.iterrows()):
            rows.append({
                "player_name_clean": r["player_name_clean"],
                "year_ahead": year,
                "attribute": attr,
                "value": float(r[attr]),
                "percentile": float(pct.iloc[i]),
            })

# clean data and export to CSV
percentile_df = pd.DataFrame(rows)
percentile_df["percentile"] = (percentile_df["percentile"] * 100).round(1)

percentile_df = percentile_df.sort_values(by=['player_name_clean', 'year_ahead'])
percentile_df.to_csv("player_attribute_percentiles.csv", index=False)

