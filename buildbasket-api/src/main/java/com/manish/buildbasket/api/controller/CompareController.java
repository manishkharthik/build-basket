package com.manish.buildbasket.api.controller;

import com.manish.buildbasket.api.dto.*;
import com.manish.buildbasket.api.service.CompareService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/compare")
@CrossOrigin(origins = "http://localhost:3000")
public class CompareController {

    private final CompareService compareService;

    public CompareController(CompareService compareService) {
        this.compareService = compareService;
    }

    @PostMapping
    public CompareResponseDTO compare(
        @RequestBody CompareRequestDTO req
    ) {
        return compareService.compare(req);
    }
}
