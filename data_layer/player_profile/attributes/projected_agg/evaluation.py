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

# Calculate delta
current = current_df.set_index("player_name_clean")
year5 = projected_df[projected_df["year_ahead"] == 4].set_index("player_name_clean")
delta = year5[ATTRS] - current[ATTRS]

# Split into buckets
def assign_bucket(row):
    age = row["age"]
    impact = row["Impact"]

    if impact >= 70:
        if age <= 24:
            return "young_elite"
        elif age <= 29:
            return "prime_elite"
        elif age <= 35:
            return "aging_elite"
        else:
            return "old_elite"

    elif impact >= 60:
        if age <= 24:
            return "young_good"
        elif age <= 29:
            return "prime_good"
        else:
            return "old_good"

    else:
        if age <= 24:
            return "young_weak"
        elif age <= 29:
            return "prime_weak"
        else:
            return "old_weak"

current_df["bucket"] = current_df.apply(assign_bucket, axis=1)

# Calculate average delta values for each bucket
delta_with_bucket = (
    delta
    .merge(
        current_df[["player_name_clean", "bucket"]],
        left_index=True,
        right_on="player_name_clean",
        how="inner"
    )
    .set_index("player_name_clean")
)

avg_delta_by_bucket = (
    delta_with_bucket
    .groupby("bucket")[ATTRS]
    .mean()
    .round(2)
)

# Presenting output
bucket_order = [
    "young_elite", "prime_elite", "aging_elite", "old_elite",
    "young_good", "prime_good", "old_good",
    "young_weak", "prime_weak", "old_weak",
]

avg_delta_by_bucket = avg_delta_by_bucket.reindex(bucket_order)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
print(avg_delta_by_bucket)
