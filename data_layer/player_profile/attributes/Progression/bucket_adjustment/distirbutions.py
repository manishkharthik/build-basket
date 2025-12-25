from sqlalchemy import create_engine
import pandas as pd
import os

# Load data from DB
engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

bucket_df = pd.read_sql("""
    SELECT *
    FROM player_attributes_base
    ORDER BY player_name, season
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

delta_df = delta_df.merge(
    latest_player_age[["player_name_clean", "player_bucket"]],
    on="player_name_clean",
    how="left"
)

# No. of players in each grouping
for player_bucket, val in delta_df["player_bucket"].value_counts().items():
    print(f'{player_bucket}: {val // 5}')

ATTRS = [
    "Shooting", "Playmaking",
    "Perimeter_Defense", "Interior_Defense",
    "Rebounding", "Scoring",
    "Efficiency", "Impact",
]

metrics = []

# Compute mean, sd, percentiles (10, 50, 90)
for attr in ATTRS:
    grouped = (
        delta_df
        .groupby(["player_bucket", "year_ahead"])[attr]
        .agg(
            mean_change="mean",
            std_dev="std",
            p10=lambda x: x.quantile(0.10),
            p50=lambda x: x.quantile(0.50),
            p90=lambda x: x.quantile(0.90),
            count="count",
        )
        .reset_index()
    )

    grouped["attribute"] = attr
    metrics.append(grouped)

metrics_df = pd.concat(metrics, ignore_index=True)

# Compute slope (change in mean from prev year)
metrics_df = metrics_df.sort_values(
    ["player_bucket", "attribute", "year_ahead"]
)
metrics_df["slope"] = (
    metrics_df
    .groupby(["player_bucket", "attribute"])["mean_change"]
    .diff()
)

# Compute stability
EPS = 1e-6
metrics_df["stability"] = (
    metrics_df["std_dev"] /
    (metrics_df["mean_change"].abs() + EPS)
)

# Consolidate and push to DB
final_metrics_df = metrics_df[
    [
        "player_bucket",
        "attribute",
        "year_ahead",
        "mean_change",
        "std_dev",
        "p10",
        "p50",
        "p90",
        "slope",
        "stability",
    ]
]

TABLE_NAME = "player_bucket_attribute_statistics"
final_metrics_df.to_sql(
    TABLE_NAME,
    engine,
    if_exists="replace", 
    index=False,
)

print(final_metrics_df.groupby("player_bucket").size())