import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import os
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import re
import unicodedata

# =========================================================
# 1. PATHS + DB
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DELTA_PATH = os.path.join(BASE_DIR, "player_relative_delta.csv")

engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

# =========================================================
# 2. LOAD AND CLEAN DATA
# =========================================================
delta_df = pd.read_csv(DELTA_PATH)

context_df = pd.read_sql("""
    SELECT player_name_clean, season, age, years_in_league, minutes_per_game, usg_pct
    FROM player_attributes_base
""", engine)

def clean_name(name: str) -> str:
    if pd.isna(name):
        return None
    
    name = unicodedata.normalize("NFKD", name)
    name = name.lower().strip()
    name = re.sub(r"[‐-–—−-]", " ", name)
    name = re.sub(r"[’']", "", name)
    name = re.sub(r"\.", "", name)
    name = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", name)
    name = re.sub(r"[^a-z\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name

delta_df["player_name_clean"] = delta_df["player_name_clean"].apply(clean_name)
context_df["player_name_clean"] = context_df["player_name_clean"].apply(clean_name)

# =========================================================
# 3. MERGE TRAINING DATA
# =========================================================
df = (
    delta_df
    .merge(context_df, on=["player_name_clean", "season"], how="inner")
    .sort_values(["player_name_clean", "season"])
    .reset_index(drop=True)
)

# =========================================================
# 4. CONFIG
# =========================================================
ATTRS = [
    "Shooting", "Playmaking",
    "Perimeter_Defense", "Interior_Defense",
    "Rebounding", "Scoring",
    "Efficiency", "Impact",
]

DELTA_COL = lambda a: f"{a}_pct_delta_adj"
LAGS = [1, 2]

# =========================================================
# 5. LAG FEATURES + TARGET
# =========================================================
for attr in ATTRS:
    col = DELTA_COL(attr)
    if col == "Impact_pct_delta_adj":
        df[col] = df[col] * (df["minutes_per_game"] / (df["minutes_per_game"] + 20))
    else:
        df[col] = df[col] * (df["minutes_per_game"] / (df["minutes_per_game"] + 15))
    for lag in LAGS:
        df[f"{col}_lag{lag}"] = df.groupby("player_name_clean")[col].shift(lag)

    df[f"{col}_next"] = df.groupby("player_name_clean")[col].shift(-1)

# =========================================================
# 6. TRAIN MODELS
# =========================================================
models: dict[str, XGBRegressor] = {}
rmse_by_attr: dict[str, float] = {}

DEPTH = {"Efficiency": 3, "Impact": 6}
LR = {"Efficiency": 0.06, "Impact": 0.03}

for attr in ATTRS:
    col = DELTA_COL(attr)

    FEATURES = [
        "age", "years_in_league", "minutes_per_game", "usg_pct",
        col, f"{col}_lag1", f"{col}_lag2",
    ]
    TARGET = f"{col}_next"

    train_df = df.dropna(subset=FEATURES + [TARGET]).copy()
    if train_df.empty:
        print(f"[WARN] No training rows for {attr}. Skipping model.")
        continue

    players = train_df["player_name_clean"].unique()
    train_players, test_players = train_test_split(players, test_size=0.2, random_state=42)

    train_split = train_df[train_df["player_name_clean"].isin(train_players)]
    test_split  = train_df[train_df["player_name_clean"].isin(test_players)]

    X_train, y_train = train_split[FEATURES], train_split[TARGET]
    X_test,  y_test  = test_split[FEATURES],  test_split[TARGET]

    model = XGBRegressor(
        n_estimators=350,
        max_depth=DEPTH.get(attr, 4),
        learning_rate=LR.get(attr, 0.045),
        subsample=0.85,
        colsample_bytree=0.85,
        objective="reg:pseudohubererror",
        random_state=42,
    )

    model.fit(X_train, y_train)
    models[attr] = model

    preds = model.predict(X_test)

# =========================================================
# 7. 5-YEAR PROJECTION
# =========================================================
def project_player_5y(context, last_deltas, n_years=5):
    age = context["age"]
    yil = context["years_in_league"]
    mp  = context["minutes_per_game"]
    usg = context["usg_pct"]

    # momentum buffer (Δ_t, Δ_{t-1}, Δ_{t-2})
    deltas = {a: [last_deltas[a]] * 3 for a in ATTRS}

    projections = []

    for year_ahead in range(1, n_years + 1):
        year_out = {
            "year_ahead": year_ahead
        }

        for attr, model in models.items():
            X = np.array([[age, yil, mp, usg, *deltas[attr]]])
            delta_pred = float(model.predict(X)[0])

            # ---- aging adjustment (delta space only) ----
            AGE_PEAK = 29
            AGING_SLOPE = {
                "Shooting": 0.004,
                "Playmaking": 0.005,
                "Perimeter_Defense": 0.009,
                "Interior_Defense": 0.009,
                "Rebounding": 0.007,
                "Scoring": 0.005,
                "Efficiency": 0.004,
                "Impact": 0.006,
            }

            if age > AGE_PEAK:
                delta_pred -= AGING_SLOPE[attr] * np.sqrt(age - AGE_PEAK)

            def impact_reliability(mpg, K=20):
                return mpg / (mpg + K)
            
            if attr == "Impact":
                rel = impact_reliability(mp)
                delta_pred *= rel
                delta_pred = 0.15 * np.tanh(delta_pred / 0.15)

            # store delta (percentage change)
            year_out[attr] = round(delta_pred, 4)

            # update momentum
            deltas[attr] = [delta_pred, deltas[attr][0], deltas[attr][1]]

        projections.append(year_out)

        # advance context
        age += 1
        yil += 1

    return projections

# =========================================================
# 8. GET PLAYER STATE
# =========================================================
def get_player_state(player_name_clean):
    g = (
        delta_df[delta_df["player_name_clean"] == player_name_clean]
        .sort_values("season")
        .tail(1)
    )

    if g.empty:
        raise ValueError(f"No delta data for {player_name_clean}")

    season = g.iloc[0]["season"]

    ctx = context_df[
        (context_df["player_name_clean"] == player_name_clean) &
        (context_df["season"] == season)
    ]

    if ctx.empty:
        raise ValueError(f"No context data for {player_name_clean}")

    last_deltas = {a: float(g.iloc[0][DELTA_COL(a)]) for a in ATTRS}

    return {
        "age": int(ctx.iloc[0]["age"]),
        "years_in_league": int(ctx.iloc[0]["years_in_league"]),
        "minutes_per_game": float(ctx.iloc[0]["minutes_per_game"]),
        "usg_pct": float(ctx.iloc[0]["usg_pct"]),
    }, last_deltas

# =========================================================
# 9. EXPORT PROJECTIONS
# =========================================================
rows = []

for player in delta_df["player_name_clean"].unique():
    try:
        context, last_deltas = get_player_state(player)
        projections = project_player_5y(context, last_deltas)

        for p in projections:
            row = {"player_name_clean": player}
            row.update(p)
            rows.append(row)

    except Exception as e:
        print(f"Skipping {player}: {e}")

proj_df = pd.DataFrame(rows)
proj_df.to_csv("player_delta_projections.csv", index=False)

