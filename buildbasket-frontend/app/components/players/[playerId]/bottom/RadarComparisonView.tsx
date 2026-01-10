import { CurrentAttributes, ProjectedAttributes, AttributePercentiles, ProjectedAttributePercentiles } from "@/app/types/player";
import PlayerRadar from "../top/PlayerRadar";
import { useState } from "react";

interface RadarComparisonViewProps {
  current: CurrentAttributes;
  projected: ProjectedAttributes[];
  currPercentiles: AttributePercentiles;
  projPercentiles: ProjectedAttributePercentiles[];
  selectedYearIndex: number;
  onYearChange: (i: number) => void;
}

type ViewMode = "attribute" | "percentile";

const ATTRIBUTES = [
  "shooting",
  "playmaking",
  "perimeterDefense",
  "interiorDefense",
  "rebounding",
  "scoring",
  "efficiency",
  "impact",
] as const;

const LABELS: Record<typeof ATTRIBUTES[number], string> = {
  shooting: "Shooting",
  playmaking: "Playmaking",
  perimeterDefense: "Perimeter Defense",
  interiorDefense: "Interior Defense",
  rebounding: "Rebounding",
  scoring: "Scoring",
  efficiency: "Efficiency",
  impact: "Impact",
};

export default function RadarComparisonView({
  current,
  projected,
  currPercentiles,
  projPercentiles,
  selectedYearIndex,
  onYearChange,
}: RadarComparisonViewProps) {
  const [viewMode, setViewMode] = useState<ViewMode>("attribute");
  const activeCurrent = viewMode === "attribute" ? current : currPercentiles;
  const activeProjected = viewMode === "attribute" ? projected : projPercentiles;

  if (projected.length === 0) {
    return <div className="text-gray-400">No projection data available.</div>;
  }

  const selectedProjection = projected[selectedYearIndex];
  const format = (v: number) =>
    viewMode === "percentile" ? `${v.toFixed(1)}%` : v.toFixed(1);

  return (
    <div className="grid grid-cols-12 items-start">
      {/* Radar */}
      <div className="col-span-7">
        {/* Controls */}
        <div className="flex items-center justify-between mb-4">
            {/* Toggle year */}
            <select
                value={selectedYearIndex}
                onChange={e => onYearChange(Number(e.target.value))}
                className="bg-black border border-gray-700 px-3 py-2 rounded"
            >
            {projected.map((p, i) => (
                <option key={i} value={i}>
                Year +{p.yearAhead}
                </option>
            ))}
            </select>
            {/* Toggle view */}
            <select
              value={viewMode}
              onChange={e => setViewMode(e.target.value as ViewMode)}
              className="bg-black border border-gray-700 px-3 py-2 rounded"
            >
              <option value="attribute">Attribute Change</option>
              <option value="percentile">Percentile Change</option>
            </select>
            {/* Legend */}
            <div className="flex items-center gap-15 text-md mr-75 text-gray-300">
                <div className="flex items-center gap-2 mt-10">
                    <span className="w-3 h-3 rounded-sm bg-[#74069C]" />
                    <span>Current</span>
                </div>
                <div className="flex items-center mt-10 gap-2">
                    <span className="w-3 h-3 rounded-sm bg-[#06369C]" />
                    <span>Projected</span>
                </div>
            </div>
        </div>
        <PlayerRadar
            current={current}
            projected={selectedProjection}
        />
        </div>

      {/* Table */}
      <div className="col-span-4">
        <table className="w-full mt-38.5 text-md">
            {/* Headers */}
            <thead className="text-gray-400 border-b border-gray-700">
            <tr>
              <th className="text-left">Attribute</th>
              <th className="text-right">Current</th>
              <th className="text-right">Projected</th>
              <th className="text-right">
                {viewMode === "percentile" ? "%" : "Δ"}
              </th>
            </tr>
            </thead>

            {/* Values */}
            <tbody>
              {ATTRIBUTES.map(attr => {
                const curr = activeCurrent[attr];
                const proj = selectedProjection[attr];
                const delta = Number((proj - curr).toFixed(1));

                return (
                  <tr key={attr} className="border-b border-gray-800">
                    <td className="py-2">{LABELS[attr]}</td>
                    <td className="text-right">{format(curr)}</td>
                    <td className="text-right text-[#3645ea]">
                      {format(proj)}
                    </td>
                    <td
                      className={`text-right ${
                        delta > 0
                          ? "text-green-500"
                          : delta < 0
                          ? "text-red-500"
                          : "text-gray-500"
                      }`}
                    >
                      {delta > 0 ? `+${delta.toFixed(1)}` : delta.toFixed(1)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
        </table>
      </div>
    </div>
  );
}
