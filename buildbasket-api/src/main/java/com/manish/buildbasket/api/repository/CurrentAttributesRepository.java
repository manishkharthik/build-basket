package com.manish.buildbasket.api.repository;

import com.manish.buildbasket.api.dto.CurrentAttributesDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class CurrentAttributesRepository {

    private final JdbcTemplate jdbcTemplate;

    public CurrentAttributesRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public CurrentAttributesDTO findCurrentAttributesByPlayerId(int playerId) {
        String sql = """
            SELECT
                pac.player_name_clean,
                pac."Shooting",
                pac."Playmaking",
                pac."Perimeter_Defense",
                pac."Interior_Defense",
                pac."Rebounding",
                pac."Scoring",
                pac."Efficiency",
                pac."Impact"
            FROM player_stats ps
            LEFT JOIN player_attributes_current pac
            ON
                regexp_replace(
                regexp_replace(
                    lower(pac.player_name_clean),
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
            WHERE ps.player_id = ?;
        """;

        return jdbcTemplate.queryForObject(
            sql,
            (rs, rowNum) -> new CurrentAttributesDTO(
                rs.getString("player_name_clean"),
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
