package com.manish.buildbasket.api.dto;

import java.util.Map;

public record CandidateDistance(
    String name,
    int state,
    Map<String, Double> attributes,
    double distance
) {}

