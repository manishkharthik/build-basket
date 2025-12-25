import pandas as pd
import os

# =========================
# 1. LOAD RAW DELTA CSV
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(BASE_DIR, "player_attribute_progression.csv")

df = pd.read_csv(INPUT_PATH)

# Ensure correct ordering
df = df.sort_values(["season", "player_name_clean"]).reset_index(drop=True)

# =========================
# 2. CONFIG
# =========================
ATTRS = [
    "Shooting",
    "Playmaking",
    "Perimeter_Defense",
    "Interior_Defense",
    "Rebounding",
    "Scoring",
    "Efficiency",
    "Impact",
]

DELTA_COLS = [f"{a}_pct_delta_adj" for a in ATTRS]

# =========================
# 3. COMPUTE LEAGUE BASELINES (PER SEASON)
# =========================
league_medians = (
    df.groupby("season")[DELTA_COLS]
      .median()
      .reset_index()
)

# Rename for clarity before merge
league_medians = league_medians.rename(
    columns={c: f"{c}_league" for c in DELTA_COLS}
)

# =========================
# 4. MERGE + COMPUTE RELATIVE DELTAS
# =========================
df = df.merge(league_medians, on="season", how="left")

for col in DELTA_COLS:
    df[col] = df[col] - df[f"{col}_league"]

# =========================
# 5. CLEAN UP
# =========================
df = df.drop(columns=[f"{c}_league" for c in DELTA_COLS])

df = df.sort_values(["player_name_clean", "season"]).reset_index(drop=True)

# =========================
# 6. EXPORT
# =========================
OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "player_relative_delta.csv"
)

df.to_csv(OUTPUT_PATH, index=False)

print(f"Exported relative deltas to {OUTPUT_PATH}")
