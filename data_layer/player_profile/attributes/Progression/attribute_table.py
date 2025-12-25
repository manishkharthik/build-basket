from sqlalchemy import create_engine
import pandas as pd
import unicodedata
import re

engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

def clean_name(name: str) -> str:
    if pd.isna(name):
        return None

    # Unicode normalize
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("utf-8")

    name = name.lower().strip()

    # Remove apostrophes
    name = re.sub(r"[’']", "", name)

    # Replace hyphens with space
    name = re.sub(r"[-]", " ", name)

    # Remove periods
    name = re.sub(r"[.]", "", name)

    # Remove suffixes (jr, sr, ii, iii, iv, v)
    name = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", name)

    # Remove any remaining non-letters
    name = re.sub(r"[^a-z\s]", "", name)

    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    return name


stats_df = pd.read_sql("""
    SELECT *
    FROM player_advanced_stats
    ORDER BY player_name, season
""", engine)
stats_df["player_name_clean"] = stats_df["player_name_clean"].apply(clean_name)

attr_df = pd.read_csv("player_attribute_scores.csv")
attr_df["player_name_clean"] = attr_df["player_name"].apply(clean_name)

merged_df = stats_df.merge( 
    attr_df.drop(columns=["player_name"]),
    on="player_name_clean", 
    how="left",
    validate="many_to_one"
)

player_features = (
    merged_df
    .groupby(["player_name_clean", "season"], as_index=False)
    .agg({
        # Identity
        "player_name": "first",

        # Context (seasonal)
        "age": "first",
        "years_in_league": "first",
        "g": "sum",
        "mp": "sum",
        "usg_pct": "mean",

        # Aggregated attributes (constant per player)
        "Shooting": "first",
        "Playmaking": "first",
        "Perimeter_Defense": "first",
        "Interior_Defense": "first",
        "Rebounding": "first",
        "Scoring": "first",
        "Efficiency": "first",
        "Impact": "first",
    })
)

player_features["minutes_per_game"] = (
    player_features["mp"] / player_features["g"]
)

player_features.to_sql(
    "player_attributes_base",
    engine,
    if_exists="replace",
    index=False
)

print("Player attributes table created successfully.")
