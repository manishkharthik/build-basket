import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

df = pd.read_sql("""
    SELECT 
        player_name,
        fg3_pct, fg3_per_48, fta,
        ast_pct, ast_per_48, usg_pct, ast_tov_ratio,
        blk_pct, blk_per_48,
        stl_pct, stl_per_48,
        oreb_per_48, dreb_per_48,
        pts_per_48, obpm, ts_pct
    FROM player_stats
""", engine)

features = [
    'fg3_pct', 'fg3_per_48', 'fta',
    'ast_pct', 'ast_per_48', 'usg_pct', 'ast_tov_ratio',
    'blk_pct', 'blk_per_48',
    'stl_pct', 'stl_per_48',
    'oreb_per_48', 'dreb_per_48',
    'pts_per_48', 'obpm', 'ts_pct',
]

X = df[features].copy()
X = X.dropna()
df = df.loc[X.index] 

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

pca = PCA(n_components=6)
X_pca = pca.fit_transform(X_scaled)

player_vectors = pd.DataFrame(X_pca, index=df["player_name"])

# Choose number of archetypes
k = 8

# Fit KMeans to the PCA player vectors
kmeans = KMeans(n_clusters=k, n_init=10, random_state=42)
clusters = kmeans.fit_predict(X_pca)

# Add cluster labels
df["cluster"] = clusters

# Create sorted view
sorted_df = df[["player_name", "cluster"]].sort_values("cluster")

# Print sorted list
print(sorted_df)

# Save sorted list to CSV
sorted_df.to_csv("player_clusters.csv", index=False)

