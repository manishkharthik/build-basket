package com.manish.buildbasket.api.dto;

import java.util.List;

public record CompareResponseDTO (
    PlayerSideDTO playerA,
    PlayerSideDTO closestPlayer,
    List<SimilarPlayerSummaryDTO> top5
) {}

