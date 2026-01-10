package com.manish.buildbasket.api.repository;

import com.manish.buildbasket.api.dto.ProjectedAttributesDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ProjectedAttributesRepository {

    private final JdbcTemplate jdbcTemplate;

    public ProjectedAttributesRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<ProjectedAttributesDTO> findProjectedAttributesByPlayerId(int playerId) {
        String sql = """
            SELECT
                pap.player_name_clean,
                pap.year_ahead,
                pap."Shooting",
                pap."Playmaking",
                pap."Perimeter_Defense",
                pap."Interior_Defense",
                pap."Rebounding",
                pap."Scoring",
                pap."Efficiency",
                pap."Impact"
            FROM player_attributes_projections pap
            JOIN player_stats ps
            ON
                regexp_replace(
                regexp_replace(
                    lower(pap.player_name_clean),
                    '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$',
                    '',
                    'i'
                ),
                '[^a-z]',
                '',
                'g'
                )
                =
                regexp_replace(
                regexp_replace(
                    lower(ps.clean_name),
                    '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$',
                    '',
                    'i'
                ),
                '[^a-z]',
                '',
                'g'
                )
            WHERE ps.player_id = ?
            ORDER BY pap.year_ahead;
        """;

        return jdbcTemplate.query(
            sql,
            (rs, rowNum) -> new ProjectedAttributesDTO(
                rs.getString("player_name_clean"),
                rs.getInt("year_ahead"),
                rs.getDouble("Shooting"),
                rs.getDouble("Playmaking"),
                rs.getDouble("Perimeter_Defense"),
                rs.getDouble("Interior_Defense"),
                rs.getDouble("Rebounding"),
                rs.getDouble("Scoring"),
                rs.getDouble("Efficiency"),
                rs.getDouble("Impact")
            ),
            playerId
        );
    }
}
