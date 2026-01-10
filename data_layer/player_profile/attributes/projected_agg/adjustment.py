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

# Bucket labels
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

# Assign buckets based on player age and current impact
current_df["bucket"] = current_df.apply(assign_bucket, axis=1)

# Bring bucket into projections
projected_df = projected_df.merge(
    current_df[["player_name_clean", "bucket"]],
    on="player_name_clean",
    how="left"
)

# Projection rules for each bucket
BUCKET_RULES = {
    "young_elite": {
        "scoring_threshold": 20,
        "scoring_divisor": 2,
        "decline_threshold": 20,
        "decline_divisor": 4,
    },
    "prime_elite": {
        "scoring_threshold": 15,
        "scoring_divisor": 2,
        "decline_threshold": 20,
        "decline_divisor": 4,
    },
    "aging_elite": {
        "change_threshold": 15,
        "change_divisor": 2,
    },
    "old_elite": {
        "change_threshold": 15,
        "change_divisor": 3,
    },
    "young_good": {
        "change_threshold": 20,
        "change_divisor": 2,
    },
    "prime_good": {
        "change_threshold": 15,
        "change_divisor": 2,
    },
    "old_good": {
        "change_threshold": 10,
        "change_divisor": 2,
    },
    "young_weak": {
        "change_threshold": 15,
        "change_divisor": 2,
    },
    "prime_weak": {
        "change_threshold": 10,
        "change_divisor": 2,
    },
    "old_weak": {
        "change_threshold": 10,
        "change_divisor": 2,
    },
}

BASELINE_CURRENT = current_df.set_index("player_name_clean")[ATTRS].copy()

def dampen_large_changes(player_proj: pd.DataFrame, baseline_current: pd.Series):
    bucket = player_proj["bucket"].iloc[0]
    rules = BUCKET_RULES.get(bucket, {})

    y5 = player_proj[player_proj["year_ahead"] == 5]
    if y5.empty:
        return player_proj

    for attr in ATTRS:
        current_val = baseline_current[attr]
        y5_val = y5[attr].iloc[0]
        total_change = y5_val - current_val

        scale = 1.0

        # Scoring rule (young_elite / prime_elite)
        if attr == "Scoring" and "scoring_threshold" in rules:
            if abs(total_change) > rules["scoring_threshold"]:
                scale *= 1 / rules["scoring_divisor"]

        # Decline rule
        if "decline_threshold" in rules:
            if total_change < -rules["decline_threshold"]:
                scale *= 1 / rules["decline_divisor"]

        # Generic change rule
        if "change_threshold" in rules:
            if abs(total_change) > rules["change_threshold"]:
                scale *= 1 / rules["change_divisor"]

        if scale == 1.0:
            continue

        for idx in player_proj.index:
            year_val = player_proj.at[idx, attr]
            delta = year_val - current_val
            player_proj.at[idx, attr] = current_val + delta * scale

    return player_proj

adjusted = []

for player, player_proj in projected_df.groupby("player_name_clean"):
    if player not in BASELINE_CURRENT.index:
        adjusted.append(player_proj)
        continue

    player_proj = player_proj.copy()
    baseline = BASELINE_CURRENT.loc[player]

    player_proj = dampen_large_changes(player_proj, baseline)
    adjusted.append(player_proj)

projected_df = pd.concat(adjusted, ignore_index=True)
projected_df[ATTRS] = projected_df[ATTRS].clip(0, 100)

# Write to DB
projected_df.to_sql(
    "player_attribute_projections_refined",
    engine,
    if_exists="replace",
    index=False
)