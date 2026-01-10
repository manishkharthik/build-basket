"use client";

import { Top5Entry } from "@/app/types/player";

interface TopMatchesListProps {
  top5: Top5Entry[];
}

function similarityFromDistance(d: number) {
  const sigma = 1;
  return Math.round(
    (100 * Math.exp(-(d * d) / (2 * sigma * sigma))) + 15
  );
}

export default function TopMatchesList({
  top5,
}: TopMatchesListProps) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
      <h3 className="mb-5 text-lg text-center font-medium text-white/80">
        Top 5 Similar Players
      </h3>

      <ul className="space-y-3">
        {top5.map((p, i) => {
          const score = similarityFromDistance(p.distance);

          return (
            <li
              key={p.name}
              className="flex items-center justify-between rounded-xl border border-white/10 bg-black/30 px-4 py-3"
            >
              {/* Player Name and Rank */}
              <div className="flex items-center gap-3 w-full">
                <span className="w-5 text-sm text-white/40">{i + 1}</span>
                <span className="w-70 font-medium text-white">{p.name}</span>

                {/* Progress Bar and Percentage */}
                <div className="flex justify-end items-center w-full gap-3">
                  <span className="rounded-lg bg-white/10 px-3 py-1 text-sm text-white">
                    {score}%
                  </span>

                  <div className="relative w-150 h-4 bg-gray-600 rounded-full overflow-hidden">
                    <div
                      className="absolute top-0 left-0 h-full bg-gradient-to-r from-[#74069C] via-[#451893] to-[#06369C] rounded-full"
                      style={{ width: `${score}%` }}
                    ></div>
                  </div>
                </div>
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
