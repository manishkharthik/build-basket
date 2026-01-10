package com.manish.buildbasket.api.controller;

import com.manish.buildbasket.api.dto.AttributePercentilesDTO;
import com.manish.buildbasket.api.dto.CurrentAttributesDTO;
import com.manish.buildbasket.api.dto.PlayerSummaryDTO;
import com.manish.buildbasket.api.dto.ProjectedAttributesDTO;
import com.manish.buildbasket.api.repository.CurrentAttributesRepository;
import com.manish.buildbasket.api.repository.ProjectedAttributesRepository;
import com.manish.buildbasket.api.repository.AttributePercentilesRepository;
import com.manish.buildbasket.api.service.PlayerService;

import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/players")
@CrossOrigin(origins = "http://localhost:3000")
public class PlayerController {

    private final PlayerService playerService;
    private final CurrentAttributesRepository currentAttributesRepository;
    private final ProjectedAttributesRepository projectedAttributesRepository;
    private final AttributePercentilesRepository attributePercentilesRepository;

    public PlayerController(
        PlayerService playerService,
        CurrentAttributesRepository currentAttributesRepository,
        ProjectedAttributesRepository projectedAttributesRepository,
        AttributePercentilesRepository attributePercentilesRepository
    ) {
        this.playerService = playerService;
        this.currentAttributesRepository = currentAttributesRepository;
        this.projectedAttributesRepository = projectedAttributesRepository;
        this.attributePercentilesRepository = attributePercentilesRepository;
    }

    // Player list
    @GetMapping
    public List<PlayerSummaryDTO> getPlayers() {
        return playerService.getAllPlayers();
    }

    // Current attributes
    @GetMapping("/{playerId}/attributes/current")
    public CurrentAttributesDTO current(@PathVariable int playerId) {
        return currentAttributesRepository
            .findCurrentAttributesByPlayerId(playerId);
    }

    // Projected attributes
    @GetMapping("/{playerId}/attributes/projected")
    public List<ProjectedAttributesDTO> projected(@PathVariable int playerId) {
        return projectedAttributesRepository
            .findProjectedAttributesByPlayerId(playerId);
    }

    // Current attribute percentiles
    @GetMapping("/{playerId}/attributes/percentiles/current")
    public AttributePercentilesDTO currentPercentiles(
        @PathVariable int playerId
    ) {
        return attributePercentilesRepository
            .findPercentilesByPlayerIdAndYear(playerId, 0);
    }

    // Projected attribute percentiles
    @GetMapping("/{playerId}/attributes/percentiles/projected")
    public List<AttributePercentilesDTO> projectedPercentiles(
        @PathVariable int playerId
    ) {
        return List.of(
            attributePercentilesRepository.findPercentilesByPlayerIdAndYear(playerId, 1),
            attributePercentilesRepository.findPercentilesByPlayerIdAndYear(playerId, 2),
            attributePercentilesRepository.findPercentilesByPlayerIdAndYear(playerId, 3),
            attributePercentilesRepository.findPercentilesByPlayerIdAndYear(playerId, 4),
            attributePercentilesRepository.findPercentilesByPlayerIdAndYear(playerId, 5)
        );
    }
}
