import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# ─────────────────────────────────────────────────────────────
# 1. Load Data
# ─────────────────────────────────────────────────────────────
engine = create_engine(
    "postgresql://postgres.fawypkcmahfkgnqhbmbb:MANFISHY0630@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres",
    pool_pre_ping=True,
)

df = pd.read_sql("""
    SELECT
        player_name,
        season,
        fg3_pct, fg3_per_48, fg3m,
        ts_pct,
        ast_pct, ast_per_48, ast_tov_ratio, ast, tov,
        blk_pct, blk_per_48, blk,
        stl_pct, stl_per_48, stl,
        oreb_per_48, dreb_per_48, reb,
        pts_per_48, obpm, pts, fgm, ftm,
        per, fg_pct, ft_pct,
        dbpm, ws_per_48, ws, vorp, minutes
    FROM player_season_stats
""", engine)

# ─────────────────────────────────────────────────────────────
# 2. Split between training and test data
# ─────────────────────────────────────────────────────────────
df["season_end"] = df["season"].str[-2:].astype(int) + 2000

train_df = df[df["season_end"] <= 2020].copy()
test_df  = df[df["season_end"] >= 2021].copy()

# ─────────────────────────────────────────────────────────────
# 3. Apply scaling
# ─────────────────────────────────────────────────────────────
def fit_minmax(col):
    return col.min(), col.max()

scalers = {}
numeric_cols = train_df.columns.drop(["player_name", "season", "season_end"])

for c in numeric_cols:
    scalers[c] = fit_minmax(train_df[c])

def apply_scale(col, min_val, max_val):
    if max_val > min_val:
        return 100 * (col - min_val) / (max_val - min_val)
    return col * 0

for c in numeric_cols:
    lo, hi = scalers[c]
    train_df[c + "_n"] = apply_scale(train_df[c], lo, hi)
    test_df[c + "_n"]  = apply_scale(test_df[c], lo, hi)

# ─────────────────────────────────────────────────────────────
# 4. Compute attributes
# ─────────────────────────────────────────────────────────────
def build_attributes(df):
    df["Shooting"] = (
        0.30 * df["fg3_pct_n"] +
        0.20 * df["ts_pct_n"] +
        0.30 * df["fg3_per_48_n"] +
        0.20 * df["fg3m_n"] + 28
    )

    df["Playmaking"] = (
        0.25 * df["ast_pct_n"] +
        0.20 * df["ast_tov_ratio_n"] +
        0.25 * df["ast_per_48_n"] +
        0.20 * df["ast_n"] -
        0.10 * df["tov_n"] + 45
    )

    df["Perimeter_Defense"] = (
        0.20 * df["stl_pct_n"] +
        0.20 * df["stl_n"] +
        0.30 * df["stl_per_48_n"] +
        0.30 * df["dbpm_n"] + 40
    )

    df["Interior_Defense"] = (
        0.20 * df["blk_pct_n"] +
        0.20 * df["blk_n"] +
        0.30 * df["blk_per_48_n"] +
        0.30 * df["dbpm_n"] + 40
    )

    df["Rebounding"] = (
        0.35 * df["oreb_per_48_n"] +
        0.35 * df["dreb_per_48_n"] +
        0.30 * df["reb_n"] + 20
    )

    df["Scoring"] = (
        0.25 * df["pts_per_48_n"] +
        0.25 * df["obpm_n"] +
        0.25 * df["pts_n"] +
        0.15 * df["fgm_n"] +
        0.10 * df["ftm_n"] + 12
    )

    df["Efficiency"] = (
        0.25 * df["ts_pct_n"] +
        0.20 * df["pts_per_48_n"] +
        0.20 * df["per_n"] +
        0.20 * df["fg_pct_n"] +
        0.15 * df["ft_pct_n"] + 30
    )

    df["Impact"] = (
        0.20 * df["obpm_n"] +
        0.20 * df["dbpm_n"] +
        0.20 * df["ws_per_48_n"] +
        0.15 * df["ws_n"] +
        0.15 * df["vorp_n"] +
        0.10 * df["minutes_n"] + 10
    )

    for cat in [
        "Shooting","Playmaking","Perimeter_Defense",
        "Interior_Defense","Rebounding","Scoring",
        "Efficiency","Impact"
    ]:
        df[cat] = df[cat].clip(0, 100)

    return df

train_df = build_attributes(train_df)
test_df  = build_attributes(test_df)

# ─────────────────────────────────────────────────────────────
# 5. Export to DB
# ─────────────────────────────────────────────────────────────
ATTR_COLS = [
    "Shooting",
    "Playmaking",
    "Perimeter_Defense",
    "Interior_Defense",
    "Rebounding",
    "Scoring",
    "Efficiency",
    "Impact",
]

train_out = train_df[
    ["player_name", "season"] + ATTR_COLS
].copy()

test_out = test_df[
    ["player_name", "season"] + ATTR_COLS
].copy()

train_out = train_out.drop_duplicates(
    subset=["player_name", "season"]
)

test_out = test_out.drop_duplicates(
    subset=["player_name", "season"]
)

train_out.to_sql(
    "training_data_attributes",
    engine,
    if_exists="replace",
    index=False,
    method="multi",
    chunksize=1000,
)

test_out.to_sql(
    "test_data_attributes",
    engine,
    if_exists="replace",
    index=False,
    method="multi",
    chunksize=1000,
)
