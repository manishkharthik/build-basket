"use client";

import PlayerRadar from "../../../players/[playerId]/top/PlayerRadar";
import { PlayerSide } from "@/app/types/player";

interface PlayerComparisonRadarProps {
  playerA: PlayerSide;
  playerB: PlayerSide;
}

export default function PlayerComparisonRadar({
  playerA,
  playerB,
}: PlayerComparisonRadarProps) {
  return (
    <PlayerRadar
      projected={{
        playerNameClean: playerA.name,
        shooting: playerA.attributes.Shooting,
        playmaking: playerA.attributes.Playmaking,
        perimeterDefense: playerA.attributes.Perimeter_Defense,
        interiorDefense: playerA.attributes.Interior_Defense,
        rebounding: playerA.attributes.Rebounding,
        scoring: playerA.attributes.Scoring,
        efficiency: playerA.attributes.Efficiency,
        impact: playerA.attributes.Impact,
      }}
      current={{
        playerNameClean: playerB.name,
        shooting: playerB.attributes.Shooting,
        playmaking: playerB.attributes.Playmaking,
        perimeterDefense: playerB.attributes.Perimeter_Defense,
        interiorDefense: playerB.attributes.Interior_Defense,
        rebounding: playerB.attributes.Rebounding,
        scoring: playerB.attributes.Scoring,
        efficiency: playerB.attributes.Efficiency,
        impact: playerB.attributes.Impact,
      }}
    />
  );
}