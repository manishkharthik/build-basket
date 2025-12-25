package com.manish.buildbasket.api.service;

import com.manish.buildbasket.api.dto.PlayerSummaryDTO;
import com.manish.buildbasket.api.repository.PlayerSummaryRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PlayerService {

    private final PlayerSummaryRepository repo;

    public PlayerService(PlayerSummaryRepository repo) {
        this.repo = repo;
    }

    public List<PlayerSummaryDTO> getAllPlayers() {
        return repo.findAllPlayerSummaries();
    }
}
