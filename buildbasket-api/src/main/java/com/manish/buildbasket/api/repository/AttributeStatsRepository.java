package com.manish.buildbasket.api.repository;

import com.manish.buildbasket.api.dto.AttributeStatsDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AttributeStatsRepository {
    private final JdbcTemplate jdbcTemplate;

    public AttributeStatsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public AttributeStatsDTO findStatsByYearAhead(int yearAhead) {

        String sql = """
            SELECT
                year_ahead,

                MAX(CASE WHEN attribute = 'Shooting' THEN mean END) AS shootingMean,
                MAX(CASE WHEN attribute = 'Shooting' THEN std  END) AS shootingStd,

                MAX(CASE WHEN attribute = 'Playmaking' THEN mean END) AS playmakingMean,
                MAX(CASE WHEN attribute = 'Playmaking' THEN std  END) AS playmakingStd,

                MAX(CASE WHEN attribute = 'Perimeter_Defense' THEN mean END) AS perimeterDefenseMean,
                MAX(CASE WHEN attribute = 'Perimeter_Defense' THEN std  END) AS perimeterDefenseStd,

                MAX(CASE WHEN attribute = 'Interior_Defense' THEN mean END) AS interiorDefenseMean,
                MAX(CASE WHEN attribute = 'Interior_Defense' THEN std  END) AS interiorDefenseStd,

                MAX(CASE WHEN attribute = 'Rebounding' THEN mean END) AS reboundingMean,
                MAX(CASE WHEN attribute = 'Rebounding' THEN std  END) AS reboundingStd,

                MAX(CASE WHEN attribute = 'Scoring' THEN mean END) AS scoringMean,
                MAX(CASE WHEN attribute = 'Scoring' THEN std  END) AS scoringStd,

                MAX(CASE WHEN attribute = 'Efficiency' THEN mean END) AS efficiencyMean,
                MAX(CASE WHEN attribute = 'Efficiency' THEN std  END) AS efficiencyStd,

                MAX(CASE WHEN attribute = 'Impact' THEN mean END) AS impactMean,
                MAX(CASE WHEN attribute = 'Impact' THEN std  END) AS impactStd

            FROM attribute_stats
            WHERE year_ahead = ?

            GROUP BY year_ahead;
        """;

        return jdbcTemplate.queryForObject(
            sql,
            (rs, rowNum) -> new AttributeStatsDTO(
                rs.getInt("year_ahead"),

                rs.getDouble("shootingMean"),
                rs.getDouble("shootingStd"),

                rs.getDouble("playmakingMean"),
                rs.getDouble("playmakingStd"),

                rs.getDouble("perimeterDefenseMean"),
                rs.getDouble("perimeterDefenseStd"),

                rs.getDouble("interiorDefenseMean"),
                rs.getDouble("interiorDefenseStd"),

                rs.getDouble("reboundingMean"),
                rs.getDouble("reboundingStd"),

                rs.getDouble("scoringMean"),
                rs.getDouble("scoringStd"),

                rs.getDouble("efficiencyMean"),
                rs.getDouble("efficiencyStd"),

                rs.getDouble("impactMean"),
                rs.getDouble("impactStd")
            ),
            yearAhead
        );
    }
}
