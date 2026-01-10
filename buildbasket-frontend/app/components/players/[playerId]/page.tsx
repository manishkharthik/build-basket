"use client";

import { use, useEffect, useState } from "react";
import PlayerHeader from "./top/PlayerHeader";
import PlayerRadar from "./top/PlayerRadar";
import AttributeList from "./top/AttributeList";
import ViewTabs from "./bottom/ViewTabs";
import ProgressionView from "./bottom/ProgressionView";
import RadarComparisonView from "./bottom/RadarComparisonView";

import { PlayerSummary, CurrentAttributes, ProjectedAttributes, DisplayAttributes, AttributePercentiles, ProjectedAttributePercentiles } from "@/app/types/player";

type BottomTab = "progression" | "radar";

export default function PlayerPage({
  params,
}: {
  params: Promise<{ playerId: string }>;
}) {
    const [activeTab, setActiveTab] = useState<BottomTab>("progression");
    const [selectedAttribute, setSelectedAttribute] = useState<keyof DisplayAttributes>("impact");
    const [selectedYearIndex, setSelectedYearIndex] = useState(0);

    const { playerId } = use(params);

    const [player, setPlayer] = useState<PlayerSummary | null>(null);
    const [current, setCurrent] = useState<CurrentAttributes | null>(null);
    const [projected, setProjected] = useState<ProjectedAttributes[]>([]);
    const [currPercentiles, setCurrPercentiles] = useState<AttributePercentiles | null>(null);
    const [projPercentiles, setProjPercentiles] = useState<ProjectedAttributePercentiles[]>([]);

    useEffect(() => {
      Promise.all([
        fetch(`http://localhost:8080/api/players`).then(r => r.json()),
        fetch(`http://localhost:8080/api/players/${playerId}/attributes/current`).then(r => r.json()),
        fetch(`http://localhost:8080/api/players/${playerId}/attributes/projected`).then(r => r.json()),
        fetch(`http://localhost:8080/api/players/${playerId}/attributes/percentiles/current`).then(r => r.json()),
        fetch(`http://localhost:8080/api/players/${playerId}/attributes/percentiles/projected`).then(r => r.json()),
      ]).then(([players, current, projected, currPercentiles, projPercentiles]) => {
        const selectedPlayer = players.find(
          (p: PlayerSummary) => String(p.playerId) === String(playerId)
        );

        setPlayer(selectedPlayer);
        setCurrent(current);
        setProjected(projected);
        setCurrPercentiles(currPercentiles);
        setProjPercentiles(projPercentiles);
      });
    }, [playerId]);

    if (!player || !current || !currPercentiles || projPercentiles.length === 0) {
      return <div className="text-white">Loading…</div>;
    }

    return (
      <div className="min-h-screen bg-black text-white px-10 py-8">
        <PlayerHeader player={player} />

        <div className="mt-5 grid grid-cols-2 gap-12">
          <PlayerRadar current={current} />
          <AttributeList
            attributes={{
              shooting: current.shooting,
              playmaking: current.playmaking,
              perimeterDefense: current.perimeterDefense,
              interiorDefense: current.interiorDefense,
              rebounding: current.rebounding,
              scoring: current.scoring,
              efficiency: current.efficiency,
              impact: current.impact,
            }}
          />
        </div>

        {/* BOTTOM HALF */}
        <div className="mt-16">
          <ViewTabs active={activeTab} onChange={setActiveTab} />

          {activeTab === "progression" && (
            <ProgressionView
              current={current}
              projected={projected}
              attribute={selectedAttribute}
              onAttributeChange={setSelectedAttribute}
            />
          )}

          {activeTab === "radar" && (
            <RadarComparisonView
              current={current}
              projected={projected}
              currPercentiles={currPercentiles}
              projPercentiles={projPercentiles}
              selectedYearIndex={selectedYearIndex}
              onYearChange={setSelectedYearIndex}
            />
          )}
        </div>
      </div>
    );
  }
