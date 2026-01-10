package com.manish.buildbasket.api.repository;

import com.manish.buildbasket.api.dto.AttributePercentilesDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AttributePercentilesRepository {

    private final JdbcTemplate jdbcTemplate;

    public AttributePercentilesRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public AttributePercentilesDTO findPercentilesByPlayerIdAndYear(
        int playerId,
        int yearAhead
    ) {
        String sql = """
            SELECT
                pap.player_name_clean,

                MAX(CASE WHEN attribute = 'Shooting' THEN percentile END) AS shooting,
                MAX(CASE WHEN attribute = 'Playmaking' THEN percentile END) AS playmaking,
                MAX(CASE WHEN attribute = 'Perimeter_Defense' THEN percentile END) AS perimeterDefense,
                MAX(CASE WHEN attribute = 'Interior_Defense' THEN percentile END) AS interiorDefense,
                MAX(CASE WHEN attribute = 'Rebounding' THEN percentile END) AS rebounding,
                MAX(CASE WHEN attribute = 'Scoring' THEN percentile END) AS scoring,
                MAX(CASE WHEN attribute = 'Efficiency' THEN percentile END) AS efficiency,
                MAX(CASE WHEN attribute = 'Impact' THEN percentile END) AS impact

            FROM player_attribute_percentiles pap
            JOIN player_stats ps
              ON regexp_replace(
                   regexp_replace(lower(pap.player_name_clean),
                   '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$','', 'i'),
                   '[^a-z]','', 'g'
                 )
              =
                 regexp_replace(
                   regexp_replace(lower(ps.clean_name),
                   '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$','', 'i'),
                   '[^a-z]','', 'g'
                 )

            WHERE ps.player_id = ?
              AND pap.year_ahead = ?

            GROUP BY pap.player_name_clean;
        """;

        return jdbcTemplate.queryForObject(
            sql,
            (rs, rowNum) -> new AttributePercentilesDTO(
                rs.getString("player_name_clean"),
                rs.getDouble("shooting"),
                rs.getDouble("playmaking"),
                rs.getDouble("perimeterDefense"),
                rs.getDouble("interiorDefense"),
                rs.getDouble("rebounding"),
                rs.getDouble("scoring"),
                rs.getDouble("efficiency"),
                rs.getDouble("impact")
            ),
            playerId,
            yearAhead
        );
    }
}
