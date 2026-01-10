package com.manish.buildbasket.api.service;

import com.manish.buildbasket.api.dto.PlayerAnalyticsDTO;
import com.manish.buildbasket.api.repository.PlayerAnalyticsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PlayerAnalyticsService {

    @Autowired
    private PlayerAnalyticsRepository playerAnalyticsRepository;

    // Method to fetch player data based on filters
    public List<PlayerAnalyticsDTO> getPlayerAnalytics(Integer ageMin, Integer ageMax, int state, List<String> teams, List<Integer> pos, List<Integer> archetypes, String sortBy, String order) {
        if (pos == null || pos.isEmpty()) {
            pos = List.of(1, 2, 3, 4, 5);
        }
        if (ageMin == null) {
            ageMin = 16;
        }
        if (ageMax == null) {
            ageMax = 45;
        }
        if (archetypes == null || archetypes.isEmpty()) {
            archetypes = List.of(0, 1, 2, 3, 4, 5, 6, 7);
        }
        if (teams == null || teams.isEmpty()) {
            teams = List.of("DEN", "CHI", "HOU", "IND", "GSW", "BOS", "LAC", "CLE", "ATL", "POR",
            "NOP", "DAL", "SAC", "MIL", "WAS", "BKN", "LAL", "SAS", "TOR", "OKC",
            "CHA", "MIN", "PHX", "NYK", "MEM", "ORL", "PHI", "MIA", "UTA", "DET");
        }
        return playerAnalyticsRepository.fetchPlayerAnalytics(ageMin, ageMax, state, teams, pos, archetypes, sortBy, order);
    }
}
