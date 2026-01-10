"use client";

import { PlayerSide } from "@/app/types/player";

interface AttributeComparisonTableProps {
  playerA: PlayerSide;
  playerB: PlayerSide;
}

const ATTRIBUTES = [
  "Shooting",
  "Playmaking",
  "Perimeter_Defense",
  "Interior_Defense",
  "Rebounding",
  "Scoring",
  "Efficiency",
  "Impact",
];

export default function AttributeComparisonTable({
  playerA,
  playerB,
}: AttributeComparisonTableProps) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
      <table className="w-full text-sm">
        <thead className="border-b border-white/10 text-white/60">
          <tr>
            <th className="text-left py-2">Attribute</th>
            <th className="text-center">{playerB.name}</th>
            <th className="text-center">{playerA.name}</th>
            <th className="text-right">Δ</th>
          </tr>
        </thead>

        <tbody>
          {ATTRIBUTES.map(attr => {
            const a = playerA.attributes[attr];
            const b = playerB.attributes[attr];
            const delta = Number((a - b).toFixed(1));

            return (
              <tr
                key={attr}
                className="border-b border-white/5 last:border-none"
              >
                <td className="py-2">{attr.replace("_", " ")}</td>

                <td className="text-center">
                  {b.toFixed(1)}
                </td>

                <td className="text-center text-blue-400">
                  {a.toFixed(1)}
                </td>

                <td
                  className={`text-right ${
                    delta > 0
                      ? "text-green-400"
                      : delta < 0
                      ? "text-red-400"
                      : "text-white/40"
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
  );
}
