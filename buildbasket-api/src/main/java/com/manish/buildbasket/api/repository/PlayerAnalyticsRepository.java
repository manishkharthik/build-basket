package com.manish.buildbasket.api.repository;

import com.manish.buildbasket.api.dto.PlayerAnalyticsDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.stream.Collectors;

@Repository
public class PlayerAnalyticsRepository {

    private final JdbcTemplate jdbcTemplate;

    public PlayerAnalyticsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<PlayerAnalyticsDTO> fetchPlayerAnalytics(
            Integer ageMin,
            Integer ageMax,
            int state,
            List<String> teams,
            List<Integer> pos,
            List<Integer> archetypes,
            String sortBy,
            String order
    ) {

        // Default pos
        List<Integer> posList = pos.isEmpty() ? List.of(1, 2, 3, 4, 5) : pos;

        // Default ageMin & ageMax
        int finalAgeMin = ageMin == null ? 16 : ageMin;
        int finalAgeMax = ageMax == null ? 45 : ageMax;

        // Default archetype
        List<Integer> archetypeList = archetypes.isEmpty() ? List.of(1, 2, 3, 4, 5) : archetypes;

        // Default teams
        List<String> teamList = teams.isEmpty() ? List.of("DEN", "CHI", "HOU", "IND", "GSW", "BOS", "LAC", "CLE", "ATL", "POR",
            "NOP", "DAL", "SAC", "MIL", "WAS", "BKN", "LAL", "SAS", "TOR", "OKC",
            "CHA", "MIN", "PHX", "NYK", "MEM", "ORL", "PHI", "MIA", "UTA", "DET") : teams;

        // Dynamically create placeholders for the IN clauses, but handle empty lists properly
        String teamPlaceholders = teamList.stream().map(t -> "?").collect(Collectors.joining(","));
        String posPlaceholders = posList.stream().map(p -> "?").collect(Collectors.joining(","));
        String archetypePlaceholders = archetypeList.stream().map(a -> "?").collect(Collectors.joining(","));

        // Construct conditions for team, pos, and archetypes
        String teamCondition = " AND team IN (" + teamPlaceholders + ")";
        String posCondition = " AND pos IN (" + posPlaceholders + ")";
        String archetypeCondition = " AND archetype IN (" + archetypePlaceholders + ")";

        // Validate sortBy and order to ensure they match expected columns and directions
        List<String> validColumns = List.of("player_id", "player_name", "age", "pos", "team", "archetype", "state", "shooting", "playmaking", "rebounding", "interior_defense", "perimeter_defense", "scoring", "efficiency", "impact");
        List<String> validOrders = List.of("ASC", "DESC");

        // Validate the input parameters
        if (sortBy != null && !validColumns.contains(sortBy)) {
            throw new IllegalArgumentException("Invalid sortBy value.");
        }

        if (order != null && !validOrders.contains(order)) {
            throw new IllegalArgumentException("Invalid order value.");
        }

        // SQL query construction with proper parameter placeholders
        String query =
                "SELECT player_id, player_name, age, pos, team, archetype, state, shooting, playmaking, rebounding, " +
                "interior_defense, perimeter_defense, scoring, efficiency, impact " +
                "FROM analytics_final " +
                // Add the age filter if ageMin and ageMax are provided
                " WHERE age BETWEEN ? AND ?" +
                " AND state = ?" + 
                teamCondition +
                posCondition +
                archetypeCondition +
                (sortBy != null && order != null ? " ORDER BY " + sortBy + " " + order : "");

        // Prepare PreparedStatementSetter
        PreparedStatementSetter setter = ps -> {
            int index = 1;
            
            ps.setInt(index++, finalAgeMin);  // ageMin
            ps.setInt(index++, finalAgeMax);  // ageMax

            // Always set state (state is required)
            ps.setInt(index++, state);

            // Set team parameters if provided
            for (String team : teams) {
                ps.setString(index++, team);  // team (String)
            }

            // Set pos parameters (always set pos, as it's now required in the logic)
            for (Integer p : posList) {  // Using posList which may be the default [1, 2, 3, 4, 5]
                ps.setInt(index++, p);  // pos (Integer)
            }

            // Set archetype parameters if provided
            for (Integer archetype : archetypes) {
                ps.setInt(index++, archetype);  // archetype (Integer)
            }
        };

        // Execute the query with PreparedStatementSetter
        return jdbcTemplate.query(query, setter, (rs, rowNum) -> new PlayerAnalyticsDTO(
                rs.getInt("player_id"),
                rs.getString("player_name"),
                rs.getInt("age"),
                rs.getInt("pos"),
                rs.getString("team"),
                rs.getInt("archetype"), 
                rs.getInt("state"),
                rs.getDouble("shooting"),
                rs.getDouble("playmaking"),
                rs.getDouble("rebounding"),
                rs.getDouble("interior_defense"),
                rs.getDouble("perimeter_defense"),
                rs.getDouble("scoring"),
                rs.getDouble("efficiency"),
                rs.getDouble("impact")
        ));
    }
}
