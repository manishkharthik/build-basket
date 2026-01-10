package com.manish.buildbasket.api.dto;

import java.util.Map;

public record PlayerSnapshotDTO(
    String name,
    int state,
    Map<String, Double> attributes
) {}

