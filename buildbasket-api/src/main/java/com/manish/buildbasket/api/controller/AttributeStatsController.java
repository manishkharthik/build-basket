package com.manish.buildbasket.api.controller;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.manish.buildbasket.api.dto.AttributeStatsDTO;
import com.manish.buildbasket.api.repository.AttributeStatsRepository;

import java.util.List;

@RestController
@RequestMapping("/api/attributes/stats")
@CrossOrigin(origins = "http://localhost:3000")
public class AttributeStatsController {
    private final AttributeStatsRepository attributeStatsRepository;

    public AttributeStatsController(AttributeStatsRepository attributeStatsRepository) {
        this.attributeStatsRepository = attributeStatsRepository;
    }
    
    @GetMapping("/current")
    public AttributeStatsDTO currentAttributeStats() {
        return attributeStatsRepository.findStatsByYearAhead(0);
    }

    @GetMapping("/projected")
    public List<AttributeStatsDTO> projectedAttributeStats() {
        return List.of(
            attributeStatsRepository.findStatsByYearAhead(1),
            attributeStatsRepository.findStatsByYearAhead(2),
            attributeStatsRepository.findStatsByYearAhead(3),
            attributeStatsRepository.findStatsByYearAhead(4),
            attributeStatsRepository.findStatsByYearAhead(5)
        );
    }
}
