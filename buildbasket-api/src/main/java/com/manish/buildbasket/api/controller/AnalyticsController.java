package com.manish.buildbasket.api.controller;

import com.manish.buildbasket.api.dto.PlayerAnalyticsDTO;
import com.manish.buildbasket.api.service.PlayerAnalyticsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/players")
@CrossOrigin(origins = "http://localhost:3000")
public class AnalyticsController {

    @Autowired
    private PlayerAnalyticsService playerAnalyticsService;

    // Endpoint to get player analytics data
    @GetMapping("/analytics")
    public List<PlayerAnalyticsDTO> getPlayerAnalytics(
            @RequestParam(required = false) Integer ageMin, 
            @RequestParam(required = false) Integer ageMax, 
            @RequestParam(required = false) List<String> teams,
            @RequestParam(required = false) List<Integer> pos,
            @RequestParam int state, 
            @RequestParam (required = false) List<Integer> archetypes, 
            @RequestParam (required = false) String sortBy, 
            @RequestParam (required = false) String order
        ) {
            // Validate if sortBy is provided, then order must be provided
            if (sortBy != null && order == null) {
                throw new IllegalArgumentException("If sortBy is provided, order must also be provided.");
            }
            
            // If sortBy is not provided, set order to null
            if (sortBy == null) {
                order = null;
            }
            return playerAnalyticsService.getPlayerAnalytics(ageMin, ageMax, state, teams, pos, archetypes, sortBy, order);
        }
}
