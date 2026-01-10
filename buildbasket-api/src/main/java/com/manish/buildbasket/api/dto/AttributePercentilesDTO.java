package com.manish.buildbasket.api.dto;

public record AttributePercentilesDTO(
    String playerNameClean,
    Double shooting,
    Double playmaking,
    Double perimeterDefense,
    Double interiorDefense,
    Double rebounding,
    Double scoring,
    Double efficiency,
    Double impact
) {}
