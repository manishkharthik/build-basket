"use client";

import { useMemo, useState } from "react";
import { Player } from "@/app/types/player";

type PlayerAInputProps = {
  players: Player[];
  playerA: string;
  setPlayerA: (playerName: string) => void;
  stateA: number;
  setStateA: (state: number) => void;
};

export default function PlayerAInput({
  players,
  setPlayerA,
  stateA,
  setStateA
}: PlayerAInputProps) {
  const [query, setQuery] = useState("");
  const [dropDown, setDropdown] = useState(false);

  const filtered = useMemo(() => {
    const q = query.toLowerCase();
    return players
      .filter(p => p.playerName.toLowerCase().includes(q))
      .slice(0, 8);
  }, [players, query]);

  return (
    <div>
      <h3 className="text-sm font-medium text-white/80">
        Player A
      </h3>

      {/* Search input */}
      <div className="relative mt-3">
        <input
          type="text"
          placeholder="e.g. Nikola Jokić"
          value={query}
          onChange={e => {
            setQuery(e.target.value);
            setDropdown(true);
          }}
          className="w-full rounded-xl bg-black/30 border border-white/10 px-4 py-2
                     focus:outline-none focus:ring-2 focus:ring-purple-500"
        />

        {/* Dropdown */}
        {query.length > 0 && dropDown && (
          <div className="absolute z-10 mt-2 w-full rounded-xl
                          bg-black border border-white/10">
            {filtered.length === 0 ? (
              <div className="px-4 py-2 text-sm text-white/40">
                No matches found
              </div>
            ) : (
              filtered.map(player => (
                <button
                  key={player.playerId}
                  type="button"
                  onClick={() => {
                    setPlayerA(player.playerName);
                    setQuery(player.playerName);
                    setDropdown(false)
                  }}
                  className="w-full text-left px-4 py-2
                             hover:bg-purple-500/20 transition"
                >
                  {player.playerName}
                  <span className="ml-2 text-xs text-white/40">
                    {player.teamAbbr}
                  </span>
                </button>
              ))
            )}
          </div>
        )}
      </div>

      {/* State selector */}
      <div className="mt-3">
        <label className="block text-sm text-white/60">
          State
        </label>
        <select
          value={stateA}
          onChange={e => setStateA(Number(e.target.value))}
          className="mt-3 w-full rounded-xl bg-black/30 border border-white/10 px-4 py-2"
        >
          {[0,1,2,3,4,5].map(y => (
            <option key={y} value={y}>
              {y === 0 ? "Current" : `Year +${y}`}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
