import pandas as pd
from sqlalchemy import create_engine
from nba_api.stats.endpoints import playerindex

# Connect
engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

# Load your merged table
merged_df = pd.read_sql("SELECT * FROM merged_player_features", engine)

# Load playerindex
pi_df = playerindex.PlayerIndex().get_data_frames()[0]

# Construct full name
pi_df['full_name'] = pi_df['PLAYER_FIRST_NAME'] + " " + pi_df['PLAYER_LAST_NAME']
pi_df.rename(columns={'PERSON_ID': 'player_id'}, inplace=True)

# Merge to attach player_id
merged_with_id = merged_df.merge(
    pi_df[['full_name', 'player_id']],
    left_on="PLAYER_NAME",
    right_on="full_name",
    how="left"
)

merged_with_id.rename(columns={"player_id_y": "player_id"}, inplace=True)
merged_with_id.drop(columns=["player_id_x"], inplace=True)


print(merged_with_id.head(10))

from sqlalchemy import text

with engine.begin() as conn:
    for _, row in merged_with_id.iterrows():
        conn.execute(
            text("""
                UPDATE merged_player_features
                SET player_id = :player_id
                WHERE "PLAYER_NAME" = :player_name
            """),
            {
                "player_id": int(row["player_id"]) if pd.notnull(row["player_id"]) else None,
                "player_name": row["PLAYER_NAME"]
            }
        )
print("player_id column updated successfully!")
