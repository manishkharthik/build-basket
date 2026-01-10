package com.manish.buildbasket.api.dto;

import java.util.List;

public record CompareRequestDTO(
    String playerAName,
    int stateA,
    int stateB,
    List<String> attributes
) {}
