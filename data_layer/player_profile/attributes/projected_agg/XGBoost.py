import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from xgboost import XGBRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.linear_model import LinearRegression

# ─────────────────────────────────────────────────────────────
# 1. Load Data
# ─────────────────────────────────────────────────────────────
engine = create_engine(
    "postgresql://postgres.fawypkcmahfkgnqhbmbb:MANFISHY0630@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres",
    pool_pre_ping=True,
)

df = pd.read_sql("""
    SELECT *
    FROM xgb_training_features
""", engine)

test_df = pd.read_sql("""
    SELECT *
    FROM xgb_test_features
""", engine)

# ─────────────────────────────────────────────────────────────
# 2. Sort df entries and introduce input columns
# ─────────────────────────────────────────────────────────────
df = df.sort_values(["player_name", "season_end_year"])
test_df = test_df.sort_values(["player_name", "season_end_year"])

# Role deltas (within-player)
df["delta_mpg"] = df.groupby("player_name")["mpg"].diff().fillna(0).clip(-15,15)
df["delta_usg_pct"] = df.groupby("player_name")["usg_pct"].diff().fillna(0).clip(-10,10)

test_df["delta_mpg"] = test_df.groupby("player_name")["mpg"].diff().fillna(0).clip(-15,15)
test_df["delta_usg_pct"] = test_df.groupby("player_name")["usg_pct"].diff().fillna(0).clip(-10,10)

# Player-age buckets
df["is_young"] = (df["age"] <= 23).astype(int)
df["is_prime"] = ((df["age"] >= 24) & (df["age"] <= 29)).astype(int)
df["is_old"]   = (df["age"] >= 30).astype(int)

test_df["is_young"] = (test_df["age"] <= 23).astype(int)
test_df["is_prime"] = ((test_df["age"] >= 24) & (test_df["age"] <= 29)).astype(int)
test_df["is_old"]   = (test_df["age"] >= 30).astype(int)

# Position buckets
for b in ["G", "W", "B"]:
    df[f"is_{b}"] = (df["pos_bucket"] == b).astype(int)
    test_df[f"is_{b}"] = (test_df["pos_bucket"] == b).astype(int)


ATTR_COLS = [
    "Shooting", "Playmaking", "Perimeter_Defense",
    "Interior_Defense", "Rebounding", "Scoring",
    "Efficiency", "Impact"
]

FEATURE_COLS = ATTR_COLS + [
    "age", "age_sq",
    "is_young", "is_prime", "is_old",
    "mpg", "log_mpg",
    "usg_pct",
    "delta_mpg",
    "delta_usg_pct",
    "minutes", "reliability_w",
    "season_end_year"
]

# ─────────────────────────────────────────────────────────────
# 3. Build 1-year transitions and drop null entries
# ─────────────────────────────────────────────────────────────
for col in ATTR_COLS:
    df[f"{col}_next"] = df.groupby("player_name")[col].shift(-1)
df = df.dropna(subset=[f"{c}_next" for c in ATTR_COLS])

for col in ATTR_COLS:
    df[f"d_{col}"] = df[f"{col}_next"] - df[col]
df = df.dropna(subset=[f"d_{c}" for c in ATTR_COLS])

for col in ATTR_COLS:
    test_df[f"{col}_next"] = test_df.groupby("player_name")[col].shift(-1)
test_df = test_df.dropna(subset=[f"{c}_next" for c in ATTR_COLS])

for col in ATTR_COLS:
    test_df[f"d_{col}"] = test_df[f"{col}_next"] - test_df[col]
test_df = test_df.dropna(subset=[f"d_{c}" for c in ATTR_COLS])

# Residulaize
resid_models = {}

for col in ATTR_COLS:
    lr = LinearRegression()
    role_vars = df[["delta_mpg", "delta_usg_pct"]]
    lr.fit(role_vars, df[f"d_{col}"])
    
    df[f"d_{col}_resid"] = df[f"d_{col}"] - lr.predict(role_vars)
    resid_models[col] = lr

for col in ATTR_COLS:
    lr = resid_models[col]
    role_vars_test = test_df[["delta_mpg", "delta_usg_pct"]]
    test_df[f"d_{col}_resid"] = test_df[f"d_{col}"] - lr.predict(role_vars_test)

# ─────────────────────────────────────────────────────────────
# 4. Build training matrices and run XGBoost
# ─────────────────────────────────────────────────────────────
def train_xgb_model(df_train):
    X = df_train[FEATURE_COLS].values
    y = df_train[[f"d_{c}_resid" for c in ATTR_COLS]].values

    base_model = XGBRegressor(
        n_estimators=50,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=42
    )

    model = MultiOutputRegressor(base_model)
    model.fit(X, y)

    return model

models = {}

for bucket in ["G", "W", "B"]:
    df_b = df[df["pos_bucket"] == bucket]
    models[bucket] = train_xgb_model(df_b)

# ─────────────────────────────────────────────────────────────
# 5. Evaluate 1-year transitions built by model for each pos bucket
# ─────────────────────────────────────────────────────────────
for bucket, model in models.items():
    test_b = test_df[test_df["pos_bucket"] == bucket]

    X_test = test_b[FEATURE_COLS].values
    y_test = test_b[[f"d_{c}_resid" for c in ATTR_COLS]].values

    y_pred = model.predict(X_test)
    rmse = np.sqrt(((y_pred - y_test) ** 2).mean(axis=0))

# ─────────────────────────────────────────────────────────────
# 6. Feed into current data to obtain 5-year projections
# ─────────────────────────────────────────────────────────────
current_df = pd.read_sql("""
    SELECT *
    FROM xgb_features_current_base
""", engine)

current_df["is_young"] = (current_df["age"] <= 23).astype(int)
current_df["is_prime"] = ((current_df["age"] >= 24) & (current_df["age"] <= 29)).astype(int)
current_df["is_old"]   = (current_df["age"] >= 30).astype(int)

current_df["delta_mpg"] = 0.0
current_df["delta_usg_pct"] = 0.0

if "season_end_year" not in current_df.columns:
    current_df["season_end_year"] = 2026

current_df["log_mpg"] = np.log(current_df["mpg"] + 1)
current_df["reliability_w"] = (
    current_df["minutes"] /
    (current_df["minutes"] + 800)
)

def build_X(row, FEATURE_COLS):
    # Ensures the model sees features in the exact order used during training
    return row[FEATURE_COLS].astype(float).values

def project_player(player_row, models):
    bucket = player_row["pos_bucket"]
    model = models[bucket]

    X = build_X(player_row, FEATURE_COLS)
    season = int(player_row["season_end_year"])

    rows = []

    # Year 0 (current)
    rows.append({
        "player_name_clean": player_row["player_name_clean"],
        "year_ahead": 0,
        "season": season,
        **{attr: X[FEATURE_COLS.index(attr)] for attr in ATTR_COLS}
    })

    for step in range(1, 6):
        delta = model.predict(X.reshape(1, -1))[0]
        delta = np.clip(delta, -8, 8)

        # update attributes
        for i, attr in enumerate(ATTR_COLS):
            idx = FEATURE_COLS.index(attr)
            X[idx] = np.clip(X[idx] + delta[i], 0, 100)

        # advance time
        X[FEATURE_COLS.index("age")] += 1
        X[FEATURE_COLS.index("age_sq")] = X[FEATURE_COLS.index("age")] ** 2
        X[FEATURE_COLS.index("season_end_year")] += 1

        # recompute derived features
        X[FEATURE_COLS.index("log_mpg")] = np.log(
            X[FEATURE_COLS.index("mpg")] + 1
        )
        X[FEATURE_COLS.index("reliability_w")] = (
            X[FEATURE_COLS.index("minutes")] /
            (X[FEATURE_COLS.index("minutes")] + 800)
        )

        rows.append({
            "player_name_clean": player_row["player_name_clean"],
            "year_ahead": step,
            "season": season + step,
            **{attr: X[FEATURE_COLS.index(attr)] for attr in ATTR_COLS}
        })

    return rows

all_rows = []

for _, row in current_df.iterrows():
    try:
        all_rows.extend(project_player(row, models))
    except Exception as e:
        print(f"Skipping {row['player_name_clean']}: {e}")

projections_df = pd.DataFrame(all_rows)

projections_df.to_sql(
    "player_attribute_projections_refined",
    engine,
    if_exists="replace",
    index=False
)