import pandas as pd
from sqlalchemy import create_engine, text
from nba_api.stats.endpoints import playerindex

# -----------------------------------
# 1. CONNECT TO SUPABASE DATABASE
# -----------------------------------

engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

# -----------------------------------
# 2. LOAD PLAYER LIST FROM MERGED TABLE
# -----------------------------------

# Use quoted identifiers because PostgreSQL is case-sensitive
merged_df = pd.read_sql(
    'SELECT player_id, "PLAYER_NAME", "Age" FROM merged_player_features',
    engine
)

merged_df.rename(columns={"PLAYER_NAME": "player_name"}, inplace=True)

# Convert player_id to Int64
merged_df["player_id"] = pd.to_numeric(merged_df["player_id"], errors="coerce").astype("Int64")

# Remove rows without valid IDs
merged_df = merged_df.dropna(subset=["player_id"])

print("\nLoaded players:")
print(merged_df.head())

# -----------------------------------
# 3. LOAD NBA PLAYERINDEX
# -----------------------------------

pi_df = playerindex.PlayerIndex().get_data_frames()[0]
pi_df = pi_df[['PERSON_ID', 'HEIGHT', 'WEIGHT', 'POSITION']]
pi_df.rename(columns={"PERSON_ID": "player_id"}, inplace=True)
pi_df["player_id"] = pd.to_numeric(pi_df["player_id"], errors="coerce").astype("Int64")

# -----------------------------------
# 4. CONVERSION HELPERS
# -----------------------------------

def height_to_cm(h):
    if not isinstance(h, str) or "-" not in h:
        return None
    ft, inch = h.split("-")
    return round(int(ft) * 30.48 + int(inch) * 2.54)

def weight_to_kg(w):
    try:
        return round(int(w) * 0.453592, 1)
    except:
        return None

def map_pos(pos):
    if pos is None or not isinstance(pos, str):
        return None
    pos = pos.upper()
    if pos == "PG": return 1
    if pos == "SG": return 2
    if pos == "SF": return 3
    if pos == "PF": return 4
    if pos == "C": return 5
    if pos in ["G-F"]: return 2
    if pos in ["F-G"]: return 3
    if pos in ["F-C", "C-F"]: return 4
    if "G" in pos: return 1
    if "F" in pos: return 3
    if "C" in pos: return 5
    return None

# Apply transformations
pi_df["height_cm"] = pi_df["HEIGHT"].apply(height_to_cm)
pi_df["weight_kg"] = pi_df["WEIGHT"].apply(weight_to_kg)
pi_df["pos"] = pi_df["POSITION"].apply(map_pos)

print("\nPlayerIndex sample:")
print(pi_df.head())

# -----------------------------------
# 5. MERGE BOTH DATASETS
# -----------------------------------

biodata_df = merged_df.merge(
    pi_df[['player_id', 'height_cm', 'weight_kg', 'pos']],
    on="player_id",
    how="left"
)

print("\nMerged biodata sample:")
print(biodata_df.head())

# Identify unmatched players
missing = biodata_df[biodata_df["height_cm"].isnull()]
if len(missing) > 0:
    print("\n⚠️ Unmatched players (no height/weight/pos found):")
    print(missing[["player_id", "player_name"]])

# -----------------------------------
# 6. INSERT INTO NEW TABLE biodata
# -----------------------------------

rows = 0

with engine.begin() as conn:
    for _, row in biodata_df.iterrows():

        # Skip invalid IDs
        if pd.isnull(row["player_id"]):
            continue

        conn.execute(
            text("""
                INSERT INTO biodata (
                    player_id, player_name, age, height_cm, weight_kg, pos
                )
                VALUES (
                    :player_id, :player_name, :age, :height_cm, :weight_kg, :pos
                )
                ON CONFLICT (player_id)
                DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    age = EXCLUDED.age,
                    height_cm = EXCLUDED.height_cm,
                    weight_kg = EXCLUDED.weight_kg,
                    pos = EXCLUDED.pos;
            """),
            {
                "player_id": int(row["player_id"]),
                "player_name": row["player_name"],
                "age": int(row["Age"]) if pd.notnull(row["Age"]) else None,
                "height_cm": row["height_cm"],
                "weight_kg": row["weight_kg"],
                "pos": int(row["pos"]) if pd.notnull(row["pos"]) else None,
            }
        )
        rows += 1

print(f"\n🎉 Successfully inserted/updated {rows} players into biodata!")
