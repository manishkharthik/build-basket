"use client";
import { ComparisonOutput } from "@/app/types/player"
import TopMatchesList from "./TopMatchesList";
import AttributeComparisonTable from "./AttributeComparisonTable";
import PlayerComparisonRadar from "./PlayerComparisonRadar";

interface ComparisonOutputsProps {
    data: ComparisonOutput;
}

function similarityFromDistance(d: number) {
  const sigma = 1;
  return Math.round(
    (100 * Math.exp(-(d * d) / (2 * sigma * sigma))) + 15
  );
}

export default function ComparisonOutputs({ data }: ComparisonOutputsProps) {
  const {
    playerA,
    closestPlayer,
    top5,
  } = data;

    return (
    <section className="mt-12 space-y-10">
      {/* Header */}
      <header className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-white">
            Closest Player: {closestPlayer.name}
          </h2>
          <p className="text-lg text-white/60 font-bold">
            Similarity Score: {similarityFromDistance(top5[0].distance)}%
          </p>
        </div>
      </header>

      {/* Legend */}
      <div className="flex items-center gap-6 text-md text-gray-300 mb-4 ml-42">
        <div className="flex items-center gap-2">
          <span className="w-3 h-3 rounded-sm bg-[#74069C]" />
          <span>{closestPlayer.name}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="w-3 h-3 rounded-sm bg-[#06369C]" />
          <span>{playerA.name}</span>
        </div>
      </div>

      {/* Radar + table */}
      <div className="grid grid-cols-12 gap-18">
        <div className="col-span-7">
          <PlayerComparisonRadar
            playerA={playerA}
            playerB={closestPlayer}
          />
        </div>

        <div className="col-span-5 mt-10">
          <AttributeComparisonTable
            playerA={playerA}
            playerB={closestPlayer}
          />
        </div>
      </div>

      {/* Top 5 */}
      <TopMatchesList top5={top5} />
    </section>
  );
}
