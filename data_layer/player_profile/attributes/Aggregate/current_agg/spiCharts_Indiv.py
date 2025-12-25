import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# ─────────────────────────────────────────────────────────────
# 1. Load Data
# ─────────────────────────────────────────────────────────────
engine = create_engine(
    "postgresql://postgres:MANFISHY0630@db.fawypkcmahfkgnqhbmbb.supabase.co:5432/postgres"
)

df = pd.read_sql("""
    SELECT 
        player_name,
        fg3_pct, fg3_per_48, fg3m,
        ts_pct,
        ast_pct, ast_per_48, ast_tov_ratio, ast, tov,
        blk_pct, blk_per_48, blk,
        stl_pct, stl_per_48, stl,
        drtg_avg, pf,
        oreb_per_48, dreb_per_48, reb,
        pts_per_48, obpm, pts, fgm, ftm,
        per, fg_pct, ft_pct,
        dbpm, ws_per_48, ws, vorp, minutes
    FROM player_stats
""", engine)


# ─────────────────────────────────────────────────────────────
# 2. Scale all numeric columns to 0–100
# ─────────────────────────────────────────────────────────────

def scale(col):
    min_val = col.min()
    max_val = col.max()
    return 100 * (col - min_val) / (max_val - min_val) if max_val > min_val else col*0

df_norm = df.copy()
numeric_cols = df.columns.drop("player_name")

for c in numeric_cols:
    df_norm[c] = scale(df[c])


# ─────────────────────────────────────────────────────────────
# 3. Construct Attribute Categories (with weights inside category)
# ─────────────────────────────────────────────────────────────

df["Shooting"] = (
    0.30 * df_norm["fg3_pct"] +
    0.20 * df_norm["ts_pct"] +
    0.30 * df_norm["fg3_per_48"] +
    0.20 * df_norm["fg3m"] + 28
)

df["Playmaking"] = (
    0.25 * df_norm["ast_pct"] +
    0.20 * df_norm["ast_tov_ratio"] +
    0.25 * df_norm["ast_per_48"] +
    0.20 * df_norm["ast"] -
    0.10 * df_norm["tov"] + 30
)

df["Perimeter_Defense"] = (
    0.20 * df_norm["stl_pct"] +
    0.20 * df_norm["stl"] +
    0.30 * df_norm["stl_per_48"] +
    0.30 * df_norm["dbpm"] + 40
)


df["Interior_Defense"] = (
    0.20 * df_norm["blk_pct"] +
    0.20 * df_norm["blk"] +
    0.30 * df_norm["blk_per_48"] +
    0.30 * df_norm["dbpm"] + 40
)


df["Rebounding"] = (
    0.35 * df_norm["oreb_per_48"] +
    0.35 * df_norm["dreb_per_48"] +
    0.30 * df_norm["reb"] + 20
)

df["Scoring"] = (
    0.25 * df_norm["pts_per_48"] +
    0.25 * df_norm["obpm"] +
    0.25 * df_norm["pts"] +
    0.15 * df_norm["fgm"] +
    0.10 * df_norm["ftm"] + 12
)

df["Efficiency"] = (
    0.30 * df_norm["ts_pct"] +
    0.25 * df_norm["per"] +
    0.25 * df_norm["fg_pct"] +
    0.20 * df_norm["ft_pct"] 
)

df["Impact"] = (
    0.20 * df_norm["obpm"] +
    0.20 * df_norm["dbpm"] +
    0.20 * df_norm["ws_per_48"] +
    0.15 * df_norm["ws"] +
    0.15 * df_norm["vorp"] +
    0.10 * df_norm["minutes"] + 10
)

# Clamp negative values
def clamp_0_100(s):
    return s.clip(lower=0, upper=100)

for cat in ["Shooting","Playmaking","Perimeter_Defense",
            "Interior_Defense","Rebounding","Scoring",
            "Efficiency","Impact"]:
    df[cat] = clamp_0_100(df[cat])

# ─────────────────────────────────────────────────────────────
# 4. Output Preview
# ─────────────────────────────────────────────────────────────

def radar_chart(player_name, df):
    categories = [
        "Shooting", "Playmaking", "Perimeter_Defense",
        "Interior_Defense", "Rebounding", "Scoring",
        "Efficiency", "Impact"
    ]
    
    row = df[df["player_name"] == player_name]
    if row.empty:
        print(f"Player '{player_name}' not found.")
        return

    values = row[categories].values.flatten()
    values = np.append(values, values[0])  # Close the loop
    print(values)

    angles = np.linspace(0, 2*np.pi, len(categories), endpoint=False)
    angles = np.append(angles, angles[0])

    # Create figure + polar axis
    fig = plt.figure(figsize=(7, 7))
    ax = plt.subplot(111, polar=True)

    # ⭐ FIX: Force consistent radar scale (0 to 100)
    ax.set_ylim(0, 100)

    # Optional: Add gridlines at meaningful values
    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels(["20", "40", "60", "80", "100"])

    # Plot
    ax.plot(angles, values, marker="o")
    ax.fill(angles, values, alpha=0.25)

    # Labels
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories)
    plt.title(player_name, fontsize=14)

    plt.show()


radar_chart("Ronald Holland II", df)

