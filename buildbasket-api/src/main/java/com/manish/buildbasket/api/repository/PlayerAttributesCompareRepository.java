package com.manish.buildbasket.api.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

@Repository
public class PlayerAttributesCompareRepository {

    private final JdbcTemplate jdbcTemplate;

    public PlayerAttributesCompareRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private String quotedAttrs(List<String> attrs) {
    return attrs.stream()
        .map(a -> "pac.\"" + a + "\"")
        .collect(Collectors.joining(", "));
}

    public Map<String, Double> findPlayerAttributes(
        String playerName,
        int yearAhead,
        List<String> attrs
    ) {
        String table = yearAhead == 0
            ? "player_attributes_current"
            : "player_attributes_projections";

        String sql = """
            SELECT %s
            FROM %s pac
            JOIN player_stats ps
            ON
                pac.player_name_clean =
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            unaccent(lower(ps.player_name)),
                            '[-]', ' ', 'g'
                        ),
                        '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$','', 'i'
                    ),
                    '[^a-z ]','', 'g'
                )
            WHERE
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            unaccent(lower(ps.player_name)),
                            '[-]', ' ', 'g'
                        ),
                        '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$','', 'i'
                    ),
                    '[^a-z ]','', 'g'
                ) = ?
            %s
        """.formatted(
            quotedAttrs(attrs),
            table,
            yearAhead == 0 ? "" : "AND pac.year_ahead = ?"
        );

        return jdbcTemplate.queryForObject(
            sql,
            (rs, rowNum) -> extractAttrs(rs, attrs),
            yearAhead == 0
                ? new Object[]{(playerName)}
                : new Object[]{(playerName), yearAhead}
        );
    }

    public List<Map<String, Object>> findCandidates(
        String excludeName,
        int yearAhead,
        List<String> attrs
    ) {
        String table = yearAhead == 0
            ? "player_attributes_current"
            : "player_attributes_projections";

        String sql = """
            SELECT
                ps.player_name,
                %s
            FROM %s pac
            JOIN player_stats ps
            ON
                pac.player_name_clean =
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            unaccent(lower(ps.player_name)),
                            '[-]', ' ', 'g'
                        ),
                        '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$','', 'i'
                    ),
                    '[^a-z ]','', 'g'
                )
            WHERE pac.player_name_clean !=
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            unaccent(lower(?)),
                            '[-]', ' ', 'g'
                        ),
                        '\\s+(jr\\.?|sr\\.?|ii|iii|iv)$','', 'i'
                    ),
                    '[^a-z ]','', 'g'
                )
            %s
        """.formatted(
            quotedAttrs(attrs),
            table,
            yearAhead == 0 ? "" : "AND pac.year_ahead = ?"
        );

        return jdbcTemplate.query(
            sql,
            (rs, rowNum) -> {
                Map<String, Object> m = new HashMap<>();
                m.put("name", rs.getString("player_name"));
                for (String a : attrs) {
                    m.put(a, rs.getBigDecimal(a).doubleValue());
                }
                return m;
            },
            yearAhead == 0
                ? new Object[]{(excludeName)}
                : new Object[]{(excludeName), yearAhead}
        );
    }

    private Map<String, Double> extractAttrs(
        java.sql.ResultSet rs,
        List<String> attrs
    ) throws java.sql.SQLException {
        Map<String, Double> map = new HashMap<>();
        for (String a : attrs) {
            map.put(a, rs.getBigDecimal(a).doubleValue());
        }
        return map;
    }
}

