from sqlalchemy import create_engine, text

# Load data and define attributes
engine = create_engine(
    "postgresql://postgres.fawypkcmahfkgnqhbmbb:MANFISHY0630@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres",
    pool_pre_ping=True
)

# Simple filter parameters
user_filters = {
    'team': ("LAL", "SAC"),
    'age_min': 20,
    'age_max': 26, 
    'pos': (2, 3),
    'state': 5,
    'cluster': (2, 6),
    'sort_by': 'Impact',
    'order': 'desc'
}

# Clean the team names in user_filters
user_filters['team'] = tuple(team.lower() for team in user_filters['team'])  # Ensure teams are lowercase

# Simplified query to include team filter and join player_stats, player_attributes_current (pac), and player_attributes_projections (pap)
query = """
SELECT
    biodata.player_id,
    biodata.player_name,
    biodata.age,
    biodata.pos,
    player_stats.team_abbr, 
    pac.player_name_clean,
    pac.cluster, 
    CASE
        WHEN :state = 0 THEN pac."Shooting"  
        WHEN :state = 1 THEN pap."Shooting" 
        WHEN :state = 2 THEN pap."Shooting" 
        WHEN :state = 3 THEN pap."Shooting"
        WHEN :state = 4 THEN pap."Shooting"
        WHEN :state = 5 THEN pap."Shooting"
    END AS "Shooting",
    CASE
        WHEN :state = 0 THEN pac."Playmaking"
        WHEN :state = 1 THEN pap."Playmaking"
        WHEN :state = 2 THEN pap."Playmaking"
        WHEN :state = 3 THEN pap."Playmaking"
        WHEN :state = 4 THEN pap."Playmaking"
        WHEN :state = 5 THEN pap."Playmaking"
    END AS "Playmaking",
    CASE
        WHEN :state = 0 THEN pac."Rebounding"
        WHEN :state = 1 THEN pap."Rebounding"
        WHEN :state = 2 THEN pap."Rebounding"
        WHEN :state = 3 THEN pap."Rebounding"
        WHEN :state = 4 THEN pap."Rebounding"
        WHEN :state = 5 THEN pap."Rebounding"
    END AS "Rebounding",
    CASE
        WHEN :state = 0 THEN pac."Interior_Defense"
        WHEN :state = 1 THEN pap."Interior_Defense"
        WHEN :state = 2 THEN pap."Interior_Defense"
        WHEN :state = 3 THEN pap."Interior_Defense"
        WHEN :state = 4 THEN pap."Interior_Defense"
        WHEN :state = 5 THEN pap."Interior_Defense"
    END AS "Interior_Defense",
    CASE
        WHEN :state = 0 THEN pac."Perimeter_Defense"
        WHEN :state = 1 THEN pap."Perimeter_Defense"
        WHEN :state = 2 THEN pap."Perimeter_Defense"
        WHEN :state = 3 THEN pap."Perimeter_Defense"
        WHEN :state = 4 THEN pap."Perimeter_Defense"
        WHEN :state = 5 THEN pap."Perimeter_Defense"
    END AS "Perimeter_Defense",
    CASE
        WHEN :state = 0 THEN pac."Scoring"
        WHEN :state = 1 THEN pap."Scoring"
        WHEN :state = 2 THEN pap."Scoring"
        WHEN :state = 3 THEN pap."Scoring"
        WHEN :state = 4 THEN pap."Scoring"
        WHEN :state = 5 THEN pap."Scoring"
    END AS "Scoring",
    CASE
        WHEN :state = 0 THEN pac."Efficiency"
        WHEN :state = 1 THEN pap."Efficiency"
        WHEN :state = 2 THEN pap."Efficiency"
        WHEN :state = 3 THEN pap."Efficiency"
        WHEN :state = 4 THEN pap."Efficiency"
        WHEN :state = 5 THEN pap."Efficiency"
    END AS "Efficiency",
    CASE
        WHEN :state = 0 THEN pac."Impact"
        WHEN :state = 1 THEN pap."Impact"
        WHEN :state = 2 THEN pap."Impact"
        WHEN :state = 3 THEN pap."Impact"
        WHEN :state = 4 THEN pap."Impact"
        WHEN :state = 5 THEN pap."Impact"
    END AS "Impact"
FROM biodata
JOIN player_stats
    ON lower(biodata.player_name) = lower(player_stats.player_name)
LEFT JOIN player_attributes_current AS pac
    ON lower(biodata.player_name_clean) = lower(pac.player_name_clean)
LEFT JOIN player_attributes_projections AS pap
    ON lower(biodata.player_name_clean) = lower(pap.player_name_clean) 
    AND pap.year_ahead = :state  -- Filter projections by year_ahead = state (1-5)
WHERE
    biodata.age BETWEEN :age_min AND :age_max
    AND biodata.pos IN :pos
    AND lower(player_stats.team_abbr) IN :team
    AND pac.cluster IN :cluster
ORDER BY
    {sort_expression} {order};
"""

# Sanitize the input for sorting
sort_by = user_filters['sort_by']
order = user_filters['order'].lower()

# Map sort_by to corresponding CASE expression in the query
sort_columns_mapping = {
    'Shooting': 'CASE WHEN :state = 0 THEN pac."Shooting" WHEN :state IN (1, 2, 3, 4, 5) THEN pap."Shooting" END',
    'Playmaking': 'CASE WHEN :state = 0 THEN pac."Playmaking" WHEN :state IN (1, 2, 3, 4, 5) THEN pap."Playmaking" END',
    'Rebounding': 'CASE WHEN :state = 0 THEN pac."Rebounding" WHEN :state IN (1, 2, 3, 4, 5) THEN pap."Rebounding" END',
    'Interior_Defense': 'CASE WHEN :state = 0 THEN pac."Interior_Defense" WHEN :state IN (1, 2, 3, 4, 5) THEN pap."Interior_Defense" END',
    'Perimeter_Defense': 'CASE WHEN :state = 0 THEN pac."Perimeter_Defense" WHEN :state IN (1, 2, 3, 4, 5) THEN pap."Perimeter_Defense" END',
    'Scoring': 'CASE WHEN :state = 0 THEN pac."Scoring" WHEN :state IN (1, 2, 3, 4, 5) THEN pap."Scoring" END',
    'Efficiency': 'CASE WHEN :state = 0 THEN pac."Efficiency" WHEN :state IN (1, 2, 3, 4, 5) THEN pap."Efficiency" END',
    'Impact': 'CASE WHEN :state = 0 THEN pac."Impact" WHEN :state IN (1, 2, 3, 4, 5) THEN pap."Impact" END',
}

# Validate the column name for sorting
if sort_by not in sort_columns_mapping:
    raise ValueError(f"Invalid sort column: {sort_by}")

# Get the corresponding sorting expression
sort_expression = sort_columns_mapping[sort_by]

# Validate the sort direction
if order not in ['asc', 'desc']:
    raise ValueError(f"Invalid sort order: {order}")

# Format the query with dynamic sorting
query = query.format(sort_expression=sort_expression, order=order)

# Execute the query with parameters using 'params' as a dictionary
with engine.connect() as conn:
    result = conn.execute(text(query), user_filters)  # Pass parameters using 'params'
    players = result.fetchall()

# Display the results
for player in players:
    print(player)
