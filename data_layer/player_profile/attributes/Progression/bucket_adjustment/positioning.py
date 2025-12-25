import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from scipy.stats import rankdata
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

bucket_df = pd.read_sql("""
    SELECT *
    FROM player_attributes_base
    ORDER BY player_name, season
""", engine)

bucket_curve_df = pd.read_sql("""
    SELECT *
    FROM player_bucket_attribute_statistics
""", engine)

# Load data from CSV
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
delta_df = pd.read_csv(
    os.path.join(BASE_DIR, "player_delta_projections.csv")
)

# Obtains age of player as of 25/26
latest_player_age = (
    bucket_df.sort_values("season")
      .groupby("player_name_clean", as_index=False)
      .tail(1)
)

# Grouping function
def assign_bucket(age, impact):
    if impact >= 70:
        if age > 35: return "elite_old"
        if age >= 30: return "elite_aging"
        if age >= 25: return "elite_prime"
        return "elite_young"
    elif impact >= 60:
        if age > 30: return "solid_old"
        if age >= 25: return "solid_prime"
        return "solid_young"
    else:
        if age > 30: return "fringe_old"
        if age >= 25: return "fringe_prime"
        return "fringe_young"

latest_player_age["player_bucket"] = latest_player_age.apply(
    lambda r: assign_bucket(r["age"], r["Impact"]),
    axis=1
)

# ---------------------------------------------------------
# 1. Attach buckets to player deltas
# ---------------------------------------------------------
latest_player_age = latest_player_age.rename(
    columns={"Impact": "base_Impact"}
)

delta_df = delta_df.merge(
    latest_player_age[["player_name_clean", "player_bucket", "base_Impact"]],
    on="player_name_clean",
    how="left"
)

# explode attributes into long format
long_df = delta_df.melt(
    id_vars=["player_name_clean", "player_bucket", "year_ahead", "base_Impact"],
    value_vars=ATTRS,
    var_name="attribute",
    value_name="raw_delta"
)

# ---------------------------------------------------------
# 2. Compute percentile ranks within bucket/attr/year
# ---------------------------------------------------------
def add_percentiles(df):
    df = df.copy()
    df["percentile"] = rankdata(df["raw_delta"], method="average") / len(df)
    return df

long_df = (
    long_df
    .groupby(["player_bucket", "attribute", "year_ahead"], group_keys=False)
    .apply(add_percentiles)
)

# ---------------------------------------------------------
# 3. Merge bucket curves
# ---------------------------------------------------------
long_df = long_df.merge(
    bucket_curve_df,
    on=["player_bucket", "attribute", "year_ahead"],
    how="left"
)

# ---------------------------------------------------------
# 4. Percentile → curve interpolation
# ---------------------------------------------------------
def interpolate_curve(row):
    q = row["percentile"]
    p10, p50, p90 = row["p10"], row["p50"], row["p90"]

    if pd.isna(p10):
        return row["raw_delta"]

    if q <= 0.10:
        # below p10 → clamp
        return p10

    elif q <= 0.50:
        # p10 → p50
        t = (q - 0.10) / 0.40
        return p10 + t * (p50 - p10)

    elif q <= 0.90:
        # p50 → p90
        t = (q - 0.50) / 0.40
        return p50 + t * (p90 - p50)

    else:
        # slight extrapolation above p90
        return p90 + (q - 0.90) * (p90 - p50)

long_df["final_delta"] = long_df.apply(interpolate_curve, axis=1)

# =========================================================
# BUCKET × ATTRIBUTE ADJUSTMENT RULES
# =========================================================

BUCKET_ADJUSTMENTS = {
    "elite_old": {
        "ALL": 0.02,
    },

    "elite_aging": {
        "Interior_Defense": 0.03,
        "Perimeter_Defense": 0.03,
        "Rebounding": 0.03,
        "Playmaking": 0.02,
        "Impact": 0.02,
    },

    "elite_prime": {
        "Impact": 0.03,
        "Perimeter_Defense": 0.03,
        "Playmaking": 0.02,
        "Interior_Defense": 0.02,
        "Scoring": 0.01,
        "Shooting": 0.01,
    },

    "elite_young": {
        "Perimeter_Defense": 0.03,
        "Interior_Defense": 0.03,
        "Impact": 0.03,
        "Playmaking": 0.03,
        "Rebounding": 0.03,
        "Scoring": 0.02,
        "Shooting": 0.02,
    },

    "solid_old": {
        "Perimeter_Defense": 0.03,
        "Scoring": 0.01,
        "Shooting": 0.01,
    },

    "solid_prime": {
        "ALL": 0.02,
    },

    "solid_young": {
        "Impact": 0.03,
        "Perimeter_Defense": 0.03,
        "Interior_Defense": 0.03,
        "Playmaking": 0.02,
        "Scoring": 0.02,
        "Shooting": 0.01,
    },
}

def apply_bucket_adjustment(delta, bucket, attr, impact_level=None):
    """
    delta: raw projected % change
    bucket: player bucket
    attr: attribute name
    impact_level: current impact (needed for solid_young condition)
    """

    rules = BUCKET_ADJUSTMENTS.get(bucket)
    if not rules:
        return delta

    # Special rule for solid_young impact floor
    if bucket == "solid_young" and attr == "Impact":
        if impact_level is not None and impact_level < 65.7:
            return delta  # no adjustment

    if "ALL" in rules:
        return delta + rules["ALL"]

    return delta + rules.get(attr, 0.0)

# Updated final values
long_df["final_delta"] = long_df.apply(
    lambda r: apply_bucket_adjustment(
        delta=r["final_delta"],
        bucket=r["player_bucket"],
        attr=r["attribute"],
        impact_level=r["base_Impact"],
    ),
    axis=1
)

ID_NEGATIVE = {"fringe_old", "solid_old", "solid_prime"}
ID_POSITIVE = {"fringe_prime", "fringe_young", "solid_prime"}

def apply_interior_defense_multiplier(row):
    if row["attribute"] != "Interior_Defense":
        return row["final_delta"]

    bucket = row["player_bucket"]

    if bucket in ID_NEGATIVE:
        return row["final_delta"] * -0.1

    if bucket in ID_POSITIVE:
        return row["final_delta"] * 0.1

    return row["final_delta"]

# Final adjustment
long_df["final_delta"] = long_df.apply(
    apply_interior_defense_multiplier,
    axis=1
)

# ---------------------------------------------------------
# 5. Pivot back to wide format
# ---------------------------------------------------------
final_df = long_df.pivot_table(
    index=["player_name_clean", "year_ahead"],
    columns="attribute",
    values="final_delta"
).reset_index()

final_df = final_df.round(6)

# ---------------------------------------------------------
# 6. Save
# ---------------------------------------------------------
final_df.to_csv("player_final_projection.csv", index=False)
print("Action completed")
