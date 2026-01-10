package com.manish.buildbasket.api.dto;

public record AttributeStatsDTO(
    int yearAhead,

    Double shootingMean,
    Double shootingStd,

    Double playmakingMean,
    Double playmakingStd,

    Double perimeterDefenseMean,
    Double perimeterDefenseStd,

    Double interiorDefenseMean,
    Double interiorDefenseStd,

    Double reboundingMean,
    Double reboundingStd,

    Double scoringMean,
    Double scoringStd,

    Double efficiencyMean,
    Double efficiencyStd,

    Double impactMean,
    Double impactStd
) {}
