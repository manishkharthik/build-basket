package com.manish.buildbasket.api.repository;

import com.manish.buildbasket.api.dto.PlayerSummaryDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class PlayerSummaryRepository {

    private final JdbcTemplate jdbcTemplate;

    public PlayerSummaryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<PlayerSummaryDTO> findAllPlayerSummaries() {
        String sql = """
            SELECT
                ps.player_id,
                ps.player_name,
                b.age,
                pbs.team_abbr,
                pac.cluster AS archetype
            FROM player_stats ps

            LEFT JOIN biodata b
            ON b.player_id = ps.player_id

            -- Join current archetype
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

            -- Join latest team (robust + suffix-safe)
            LEFT JOIN (
                SELECT DISTINCT ON (
                regexp_replace(
                    regexp_replace(
                    lower(player_name_clean),
                    '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$',
                    '',
                    'i'
                    ),
                    '[^a-z]',
                    '',
                    'g'
                )
                )
                    regexp_replace(
                    regexp_replace(
                        lower(player_name_clean),
                        '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$',
                        '',
                        'i'
                    ),
                    '[^a-z]',
                    '',
                    'g'
                    ) AS canonical_name,
                    team_abbr
                FROM player_basic_stats
                ORDER BY
                regexp_replace(
                    regexp_replace(
                    lower(player_name_clean),
                    '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$',
                    '',
                    'i'
                    ),
                    '[^a-z]',
                    '',
                    'g'
                ),
                season DESC
            ) pbs
            ON pbs.canonical_name =
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
        """;

        return jdbcTemplate.query(
            sql,
            (rs, rowNum) -> new PlayerSummaryDTO(
                rs.getInt("player_id"),
                rs.getString("player_name"),
                rs.getInt("age"),
                rs.getString("team_abbr"),
                rs.getInt("archetype")
            )
        );
    }
}
