import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from scipy.stats import rankdata

# =========================
# 0. DB CONNECTION
# =========================
engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

# =========================
# 1. LOAD DATA
# =========================
basic_df = pd.read_sql("""
    SELECT *
    FROM player_basic_stats
""", engine)

advanced_df = pd.read_sql("""
    SELECT *
    FROM player_advanced_stats
""", engine)

# =========================
# 2. MERGE BASIC + ADVANCED
# =========================
df = (
    basic_df
    .merge(
        advanced_df,
        on=["player_name_clean", "season"],
        how="inner"
    )
    .sort_values(["player_name_clean", "season"])
    .reset_index(drop=True)
)

# =========================
# 3. SHRINKAGE SETUP
# =========================
SHRINK_K = {
    # efficiency / impact (strong shrinkage)
    "ts_pct": 1200,
    "bpm": 1800,
    "ws_48": 1800,
    "per": 1500,
    "usg_pct": 800,
    "ast_pct": 900,
    "trb_pct": 900,
    "fg3ar": 700,
    "w_pct": 1000,

    # counting stats (lighter shrinkage)
    "pts": 400,
    "reb": 400,
    "ast": 400,
    "stl": 300,
    "blk": 300,
    "tov": 300,
}

PARTIAL_SEASON = "2025-26"
PARTIAL_K_MULT = 0.35   # reduce shrinkage strength to 35%

STATS_TO_SHRINK = list(SHRINK_K.keys())

def shrink_stat(x, mu, minutes, k):
    return (minutes / (minutes + k)) * x + (k / (minutes + k)) * mu

# =========================
# 4. LEAGUE AVERAGES (PER SEASON)
# =========================
league_means = (
    df.groupby("season")[STATS_TO_SHRINK]
      .mean()
      .reset_index()
      .rename(columns={c: f"{c}_lg" for c in STATS_TO_SHRINK})
)

df = df.merge(league_means, on="season", how="left")

# =========================
# 5. APPLY SHRINKAGE (WITH PARTIAL-SEASON ADJUSTMENT)
# =========================
for stat, k in SHRINK_K.items():
    # default shrinkage (all seasons)
    df[f"{stat}_shrunk"] = shrink_stat(
        df[stat],
        df[f"{stat}_lg"],
        df["minutes"],
        k
    )

    # reduced shrinkage for partial season only
    mask = df["season"] == PARTIAL_SEASON
    k_partial = k * PARTIAL_K_MULT

    df.loc[mask, f"{stat}_shrunk"] = shrink_stat(
        df.loc[mask, stat],
        df.loc[mask, f"{stat}_lg"],
        df.loc[mask, "minutes"],
        k_partial
    )

# =========================
# 6. ROLLING MEANS (ON SHRUNK STATS)
# =========================
ROLL = 5

ROLL_COLS = [
    "pts_shrunk", "reb_shrunk", "ast_shrunk",
    "stl_shrunk", "blk_shrunk", "tov_shrunk",
    "ts_pct_shrunk", "ast_pct_shrunk", "trb_pct_shrunk",
    "bpm_shrunk", "usg_pct_shrunk", "ws_48_shrunk",
    "fg3ar_shrunk", "w_pct_shrunk", "per_shrunk",
    "minutes"   # NOTE: minutes is NOT shrunk
]

for col in ROLL_COLS:
    df[f"{col}_r"] = (
        df
        .groupby("player_name_clean", group_keys=False)[col]
        .rolling(ROLL, min_periods=1)
        .mean()
        .reset_index(drop=True)
    )

# =========================
# 7. ATTRIBUTE CONSTRUCTION
# =========================
df["Shooting"] = (
    0.45 * df["ts_pct_shrunk_r"] +
    0.25 * df["fg3ar_shrunk_r"] +
    0.20 * df["pts_shrunk_r"] +
    0.10 * df["usg_pct_shrunk_r"]
)

df["Playmaking"] = (
    0.40 * df["ast_pct_shrunk_r"] +
    0.25 * df["ast_shrunk_r"] +
    0.20 * df["usg_pct_shrunk_r"] -
    0.15 * df["tov_shrunk_r"]
)

df["Perimeter_Defense"] = (
    0.40 * df["stl_shrunk_r"] +
    0.30 * df["bpm_shrunk_r"] +
    0.30 * df["w_pct_shrunk_r"]
)

df["Interior_Defense"] = (
    0.45 * df["blk_shrunk_r"] +
    0.30 * df["bpm_shrunk_r"] +
    0.25 * df["w_pct_shrunk_r"]
)

df["Rebounding"] = (
    0.50 * df["trb_pct_shrunk_r"] +
    0.30 * df["reb_shrunk_r"] +
    0.20 * df["bpm_shrunk_r"]
)

df["Scoring"] = (
    0.45 * df["pts_shrunk_r"] +
    0.35 * df["usg_pct_shrunk_r"] +
    0.20 * df["bpm_shrunk_r"]
)

df["Efficiency"] = (
    0.50 * df["ts_pct_shrunk_r"] +
    0.30 * df["per_shrunk_r"] +
    0.20 * df["ws_48_shrunk_r"]
)

df["Impact"] = (
    0.40 * df["bpm_shrunk_r"] +
    0.30 * df["ws_48_shrunk_r"] +
    0.20 * df["w_pct_shrunk_r"] +
    0.10 * df["minutes_r"]
)

# =========================
# 8. RELATIVE DELTAS (% CHANGE OVER PREV SEASON)
# =========================
ATTRS = [
    "Shooting", "Playmaking",
    "Perimeter_Defense", "Interior_Defense",
    "Rebounding", "Scoring",
    "Efficiency", "Impact"
]

EPS = 1e-6  # numerical stability

for attr in ATTRS:
    prev = df.groupby("player_name_clean")[attr].shift(1)
    df[f"{attr}_pct_delta"] = (df[attr] - prev) / (prev.abs() + EPS)

# =========================
# 9.1 PARTIAL-SEASON DAMPING (2025–26 ONLY)
# =========================
mask = df["season"] == PARTIAL_SEASON
MIN_TRUST = 600

trust_weight = df["minutes"] / (df["minutes"] + MIN_TRUST)

for attr in ATTRS:
    df[f"{attr}_pct_delta_adj"] = df[f"{attr}_pct_delta"]
    df.loc[mask, f"{attr}_pct_delta_adj"] = (
        trust_weight.loc[mask] * df.loc[mask, f"{attr}_pct_delta"]
    )

# =========================
# 9.2 DISTRIBUTION ANCHORING (PARTIAL SEASON ONLY)
# =========================
REFERENCE_SEASON = "2024-25"
TARGET_SEASON = "2025-26"

def quantile_map(values, ref_values):
    """
    Percentile → quantile mapping, safe for NaNs and small samples.
    """
    values = np.asarray(values)

    # mask valid values
    valid_mask = ~np.isnan(values)
    valid_values = values[valid_mask]

    n = len(valid_values)

    # nothing to map
    if n == 0:
        return values

    # single value → map to reference median
    if n == 1:
        out = values.copy()
        out[valid_mask] = np.median(ref_values)
        return out

    ranks = (rankdata(valid_values, method="average") - 1) / (n - 1)
    ranks = np.clip(ranks, 0.0, 1.0)

    mapped = np.quantile(ref_values, ranks)

    out = values.copy()
    out[valid_mask] = mapped
    return out

for attr in ATTRS:
    col = f"{attr}_pct_delta_adj"

    ref_vals = (
        df.loc[df["season"] == REFERENCE_SEASON, col]
        .dropna()
        .values
    )

    mask = df["season"] == TARGET_SEASON
    target_vals = df.loc[mask, col].values

    # Safety checks
    if len(ref_vals) < 30:
        continue

    df.loc[mask, col] = quantile_map(target_vals, ref_vals)

# =========================
# 10. ATTRIBUTE PROJECTIONS
# =========================
def apply_pct_progression(current, pct_delta):
    return current * (1 + pct_delta)

# example: use most recent season deltas
latest = (
    df[df["player_name_clean"] == "aj green"]
    .sort_values("season")
    .iloc[-1]
)

#load and prepare current attributes
attribute_df = pd.read_sql("""
    SELECT * 
    FROM player_attributes_base
""", engine)

current_attrs = {attr: attribute_df[attr].iloc[0] for attr in ATTRS}

projected = {}
for attr in ATTRS:
    projected[attr] = apply_pct_progression(
        current_attrs[attr],
        latest[f"{attr}_pct_delta_adj"]
    )

print(
    df[df["season"] == "2021-22"][
        [f"{a}_pct_delta_adj" for a in ATTRS]
    ].mean()
)

print(
    df[df["season"] == "2022-23"][
        [f"{a}_pct_delta_adj" for a in ATTRS]
    ].mean()
)

print(
    df[df["season"] == "2023-24"][
        [f"{a}_pct_delta_adj" for a in ATTRS]
    ].mean()
)

print(
    df[df["season"] == "2024-25"][
        [f"{a}_pct_delta_adj" for a in ATTRS]
    ].mean()
)

print(
    df[df["season"] == "2025-26"][
        [f"{a}_pct_delta_adj" for a in ATTRS]
    ].mean()
)



# =========================
# 11. EXPORT RESULTS
# =========================
EXPORT_COLS = [
    "player_name_clean",
    "season",
    
    # progression signals
    "Shooting_pct_delta_adj",
    "Playmaking_pct_delta_adj",
    "Perimeter_Defense_pct_delta_adj",
    "Interior_Defense_pct_delta_adj",
    "Rebounding_pct_delta_adj",
    "Scoring_pct_delta_adj",
    "Efficiency_pct_delta_adj",
    "Impact_pct_delta_adj",
]

df_export = (
    df[EXPORT_COLS]
    .sort_values(["player_name_clean", "season"])
    .reset_index(drop=True)
)

OUTPUT_PATH = "player_raw_delta.csv"

df_export.to_csv(OUTPUT_PATH, index=False)
