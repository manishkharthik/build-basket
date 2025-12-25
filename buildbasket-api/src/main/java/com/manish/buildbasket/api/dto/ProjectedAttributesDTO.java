package com.manish.buildbasket.api.dto;

public record ProjectedAttributesDTO(
    String playerNameClean,
    Integer yearAhead,
    Double shooting,
    Double playmaking,
    Double perimeterDefense,
    Double interiorDefense,
    Double rebounding,
    Double scoring,
    Double efficiency,
    Double impact
) {}

