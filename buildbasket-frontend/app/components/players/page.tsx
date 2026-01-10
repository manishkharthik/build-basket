"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

interface Player {
  playerId: number;
  playerName: string;
  teamAbbr: string;
  age: number;
}

export default function PlayersPage() {
  const [players, setPlayers] = useState<Player[]>([]);
  const [query, setQuery] = useState("");
  const router = useRouter();

  useEffect(() => {
    fetch("http://localhost:8080/api/players")
      .then(res => res.json())
      .then((data: Player[]) => {

        // Defensive sort
        const sorted = data.sort((a, b) => {
          const teamA = a.teamAbbr || "";
          const teamB = b.teamAbbr || "";

          const teamCompare = teamA.localeCompare(teamB);
          if (teamCompare !== 0) return teamCompare;

          return a.playerName.localeCompare(b.playerName);
        });

        setPlayers(sorted);
      });
  }, []);

  const filtered =
    query.length === 0
      ? []
      : players.filter(p =>
          p.playerName.toLowerCase().startsWith(query.toLowerCase())
        ).slice(0, 5);

  const goToPlayer = (playerId: number) => {
    router.push(`/components/players/${playerId}`);
  };

  return (
    <div className="min-h-screen bg-black text-white px-6 py-8">
      
      {/* Search bar */}
      <div className="relative max-w-md mx-auto mb-8">
        <input
          type="text"
          placeholder="Search players..."
          value={query}
          onChange={e => setQuery(e.target.value)}
          className="w-full rounded-md bg-gray-900 border border-gray-700 px-4 py-2 focus:outline-none focus:ring-2 focus:ring-purple-500"
        />

        {/* Search results dropdown */}
        {query.length > 0 && (
          <div className="absolute z-10 w-full bg-gray-900 border border-gray-700 rounded-md mt-2">
            {filtered.length === 0 ? (
              <div className="px-4 py-2 text-gray-400">
                No matches found
              </div>
            ) : (
              filtered.map(player => (
                <button
                  key={player.playerId}
                  onClick={() => goToPlayer(player.playerId)}
                  className="w-full text-left px-4 py-2 hover:bg-purple-500/20 transition"
                >
                  {player.playerName}
                  <span className="text-gray-400 ml-2 text-sm">
                    {player.teamAbbr}
                  </span>
                </button>
              ))
            )}
          </div>
        )}
      </div>

      {/* Player grid (scrollable fallback) */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
        {players.map(player => (
          <div
            key={player.playerId}
            onClick={() => goToPlayer(player.playerId)}
            className="cursor-pointer bg-gray-900 border border-gray-800 rounded-lg p-4 hover:border-purple-500/50 transition"
          >
            <img
              src={`https://cdn.nba.com/headshots/nba/latest/260x190/${player.playerId}.png`}
              alt={player.playerName}
              className="rounded mb-3"
            />
            <div className="font-medium">{player.playerName}</div>
            <div className="text-sm text-gray-400">
              {player.teamAbbr} · Age {player.age}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
