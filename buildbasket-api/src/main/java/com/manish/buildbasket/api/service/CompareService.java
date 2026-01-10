package com.manish.buildbasket.api.service;

import com.manish.buildbasket.api.dto.*;
import com.manish.buildbasket.api.repository.*;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.util.*;

@Service
public class CompareService {

    private final PlayerAttributesCompareRepository attrRepo;
    private final AttributeStatsRepository statsRepo;

    public CompareService(
        PlayerAttributesCompareRepository attrRepo,
        AttributeStatsRepository statsRepo
    ) {
        this.attrRepo = attrRepo;
        this.statsRepo = statsRepo;
    }

    public static String toClean(String name) {
        if (name == null) return null;

        return Normalizer.normalize(name, Normalizer.Form.NFD)
            .replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
            .toLowerCase()
            .replaceAll("-", " ")  
            .replaceAll("\\s+(jr\\.?|sr\\.?|ii|iii|iv)$", "")
            .replaceAll("[^a-z\\s]", "") 
            .replaceAll("\\s+", " ")       
            .trim();
    }

    public CompareResponseDTO compare(CompareRequestDTO req) {

        var attrs = req.attributes();

        String lookupName = toClean(req.playerAName());

        // Player A attributes
        Map<String, Double> aAttrs =
            attrRepo.findPlayerAttributes(
                lookupName, req.stateA(), attrs
            );

        // Candidate Bs
        List<Map<String, Object>> candidates =
            attrRepo.findCandidates(
                lookupName, req.stateB(), attrs
            );

        // League stats (normalize in B space)
        AttributeStatsDTO stats =
            statsRepo.findStatsByYearAhead(req.stateB());

        List<CandidateDistance> distances = new ArrayList<>();

        for (var b : candidates) {
            double sum = 0.0;
            Map<String, Double> bAttrs = new HashMap<>();

            for (String attr : attrs) {
                double a = aAttrs.get(attr);
                double bv = ((Number) b.get(attr)).doubleValue();
                double std = stdFor(stats, attr);

                bAttrs.put(attr, bv);

                double z = (a - bv) / std;
                sum += z * z;
            }

            double dist = Math.sqrt(sum);

            distances.add(new CandidateDistance(
                (String) b.get("name"),
                req.stateB(),
                bAttrs,
                dist
            ));
        }

        if (distances.isEmpty()) {
            throw new IllegalArgumentException("No matching candidates found");
        }

        // Sort by distance
        distances.sort(Comparator.comparingDouble(CandidateDistance::distance));

        // Closest player (rank #1)
        CandidateDistance closest = distances.get(0);

        // Top 5 summaries
        List<SimilarPlayerSummaryDTO> top5 =
            distances.stream()
                .limit(5)
                .map(d -> new SimilarPlayerSummaryDTO(
                    d.name(),
                    d.state(),
                    d.distance()
                ))
                .toList();

        return new CompareResponseDTO(
            new PlayerSideDTO(
                req.playerAName(),
                req.stateA(),
                aAttrs
            ),
            new PlayerSideDTO(
                closest.name(),
                req.stateB(),
                closest.attributes()
            ),
            top5
        );
    }

    private double stdFor(AttributeStatsDTO s, String attr) {
        return switch (attr) {
            case "Shooting" -> s.shootingStd();
            case "Playmaking" -> s.playmakingStd();
            case "Perimeter_Defense" -> s.perimeterDefenseStd();
            case "Interior_Defense" -> s.interiorDefenseStd();
            case "Rebounding" -> s.reboundingStd();
            case "Scoring" -> s.scoringStd();
            case "Efficiency" -> s.efficiencyStd();
            case "Impact" -> s.impactStd();
            default -> throw new IllegalArgumentException(attr);
        };
    }
}
