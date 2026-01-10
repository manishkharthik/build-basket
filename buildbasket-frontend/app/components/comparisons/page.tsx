"use client";

import { useEffect, useMemo, useState } from "react";
import { ComparisonOutput } from "@/app/types/player"

import PlayerInputs from "./components/player-inputs/PlayerInputs";
import ClosestPlayerFinder from "./components/ClosestPlayerFinder";
import ComparisonOutputs from "./components/comparison-outputs/ComparisonOutputs"

// attribute constants
const ALL_ATTRS = [
  "Shooting",
  "Playmaking",
  "Perimeter_Defense",
  "Interior_Defense",
  "Rebounding",
  "Scoring",
  "Efficiency",
  "Impact"
];

export default function PlayerComparatorPage() {
  /* -----------------------------
     Core state
  ----------------------------- */
  const [players, setPlayers] = useState([]);

  const [playerA, setPlayerA] = useState<string>("");
  const [stateA, setStateA] = useState<number>(0);
  const [stateB, setStateB] = useState<number>(0);

  const [useAllAttrs, setUseAllAttrs] = useState<boolean>(true);
  const [selectedAttrs, setSelectedAttrs] = useState<string[]>(ALL_ATTRS);

  const [loading, setLoading] = useState<boolean>(false);
  const [result, setResult] = useState<ComparisonOutput | null>(null);
  const [error, setError] = useState<string>("");

  /* -----------------------------
     Initial data load
  ----------------------------- */
  useEffect(() => {
    fetch("http://localhost:8080/api/players")
      .then(res => res.json())
      .then(setPlayers)
      .catch(() => setPlayers([]));
  }, []);

  /* -----------------------------
     Derived values
  ----------------------------- */
  const effectiveAttributes = useMemo(() => {
    return useAllAttrs ? ALL_ATTRS : selectedAttrs;
  }, [useAllAttrs, selectedAttrs]);

  const canRunCompare =
    playerA.length > 0 && effectiveAttributes.length > 0;

  /* -----------------------------
     Actions
  ----------------------------- */
  async function runCompare() {
    if (!canRunCompare) return;

    setLoading(true);
    setError("");
    setResult(null);

    try {
      const res = await fetch("http://localhost:8080/api/compare", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerAName: playerA,
          stateA,
          stateB,
          attributes: effectiveAttributes
        })
      });

      if (!res.ok) {
        throw new Error("Comparison failed");
      }

      const data = await res.json();
      setResult(data);
    } catch {
      setError("Failed to find closest player");
    } finally {
      setLoading(false);
    }
  }

  /* -----------------------------
     Render
  ----------------------------- */
  return (
    <div className="min-h-screen bg-black text-white">
      <div className="mx-auto max-w-6xl px-6 py-10 space-y-8">

        {/* Header */}
        <header>
          <h1 className="text-2xl font-semibold">
            Player Comparator
          </h1>
          <p className="text-md text-white/60 mt-3">
            Compare Player A at a chosen time-state to the closest matching
            Player B
          </p>
        </header>

        {/* Player inputs */}
        <PlayerInputs
          players={players}
          playerA={playerA}
          setPlayerA={setPlayerA}
          stateA={stateA}
          setStateA={setStateA}
          stateB={stateB}
          setStateB={setStateB}
        />

        {/* Run comparison */}
        <ClosestPlayerFinder
          onRun={runCompare}
          loading={loading}
          disabled={!canRunCompare}
        />

        {/* Status / error */}
        {error && (
          <div className="text-sm text-red-400">
            {error}
          </div>
        )}
        <div className="mt-10">
            {/* Result rendering (top 5 + radar + table) */}
            {result && (
                <ComparisonOutputs data={result} />
            )}
        </div>
      </div>
    </div>
  );
}