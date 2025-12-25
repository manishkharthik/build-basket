package com.manish.buildbasket.api.dto;

public record PlayerSummaryDTO(
    Integer playerId,
    String playerName,
    Integer age,
    String teamAbbr,
    Integer archetype
) {}
