"use client"
import React, { useState, useEffect } from 'react';
import Image from "next/image";
import { useRouter } from "next/navigation"; 
import Filters from "./Filters";

// Define PlayerAnalyticsDTO interface for the data structure
interface PlayerAnalyticsDTO {
  playerId: number;
  playerName: string;
  age: number;
  pos: number;
  team: string;
  archetype: number;
  state: number;
  shooting: number;
  playmaking: number;
  rebounding: number;
  interiorDefense: number;
  perimeterDefense: number;
  scoring: number;
  efficiency: number;
  impact: number;
}

export const ARCHETYPE_BADGE_STYLE: Record<number, { bg: string; text: string }> = {
  0: { bg: "bg-blue-800/20", text: "text-blue-300" },     // Low Usage Playmaker
  1: { bg: "bg-violet-800/20", text: "text-violet-300" }, // Secondary Scorer
  2: { bg: "bg-green-800/20", text: "text-green-300" },   // 3&D Wing
  3: { bg: "bg-red-800/20", text: "text-red-300" },       // Rim Protector
  4: { bg: "bg-amber-800/20", text: "text-amber-300" },   // Stretch Big
  5: { bg: "bg-gray-800/20", text: "text-gray-300" },     // Fringe Prospect
  6: { bg: "bg-pink-800/20", text: "text-pink-300" },     // Alpha Creator
  7: { bg: "bg-teal-800/20", text: "text-teal-300" },     // Defensive Specialist
};

const AnalyticsPage: React.FC = () => {
  // States for filters and sorting
  const [ageMin, setAgeMin] = useState<number | null>(18);
  const [ageMax, setAgeMax] = useState<number | null>(40);
  const [yearsAhead, setYearsAhead] = useState<number>(0);
  const [teams, setTeams] = useState<string[]>([]); // default empty
  const [pos, setPos] = useState<number[]>([1, 2, 3, 4, 5]);
  const [archetypes, setArchetypes] = useState<number[]>([0, 1, 2, 3, 4, 5, 6, 7]);
  const [sortBy, setSortBy] = useState<string | null>("impact");
  const [order, setOrder] = useState<string | null>("DESC");

  const [players, setPlayers] = useState<PlayerAnalyticsDTO[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const [isModalOpen, setIsModalOpen] = useState<boolean>(false);

  // Filter change handler
  const handleFilterChange = (e: React.ChangeEvent<HTMLSelectElement | HTMLInputElement>) => {
    const { name, value, checked } = e.target as HTMLInputElement;
    if (name === 'ageMin') setAgeMin(value ? parseInt(value, 10) : null);
    if (name === 'ageMax') setAgeMax(value ? parseInt(value, 10) : null);
    if (name === 'yearsAhead') setYearsAhead(parseInt(value, 10));
    if (name === 'teams') setTeams(Array.from((e.target as HTMLSelectElement).selectedOptions, (option) => option.value));
    if (name === "pos") {
      setPos((prevPos) =>
        checked ? [...prevPos, parseInt(value)] : prevPos.filter((p) => p !== parseInt(value))
      );
    }
    if (name === "archetypes") {
      setArchetypes((prev) => {
        const updatedArchetypes = prev.includes(parseInt(value, 10))
          ? prev.filter((archetype) => archetype !== parseInt(value, 10))
          : [...prev, parseInt(value, 10)];

        return updatedArchetypes.filter((archetype) => !isNaN(archetype));
      });
    }
    if (name === 'sortBy') setSortBy(value);
    if (name === 'order') setOrder(value);
  };

  // Handle filter submit
  const handleFilterSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setIsModalOpen(false); // Close modal when filters are applied
  };

  // Call the fetch function whenever filters change
  useEffect(() => {
    const fetchPlayerAnalytics = async () => {
      setLoading(true);
      setError(null);
      try {
        // Prepare query params based on filters
        const queryParams: Record<string, string> = {};

        // Age range (if provided)
        if (ageMin && ageMax) {
          queryParams.ageMin = ageMin.toString();
          queryParams.ageMax = ageMax.toString();
        }
        queryParams.state = yearsAhead.toString();

        // Optional filters
        if (teams.length > 0) queryParams.teams = teams.join(',');
        if (pos.length > 0) queryParams.pos = pos.join(',');
        if (archetypes.length > 0) queryParams.archetypes = archetypes.join(',');

        // Sorting (if provided)
        if (sortBy && order) {
          queryParams.sortBy = sortBy;
          queryParams.order = order;
        }

        const queryString = new URLSearchParams(queryParams).toString();
        console.log(`http://localhost:8080/api/players/analytics?${queryString}`)
        const response = await fetch(`http://localhost:8080/api/players/analytics?${queryString}`);
        if (!response.ok) {
          throw new Error('Failed to fetch player data');
        }

        const data = await response.json();
        console.log(data);
        setPlayers(data);
      } catch {
      } finally {
        setLoading(false);
      }
    };

    fetchPlayerAnalytics();
  }, [ageMin, ageMax, yearsAhead, teams, pos, archetypes, sortBy, order]);

  const PlayerRow: React.FC<{ player: PlayerAnalyticsDTO; index: number }> = ({ player, index }) => {
    // Get the style for the archetype
    const archetypeStyle = ARCHETYPE_BADGE_STYLE[player.archetype];

    // Define archetype names corresponding to the index
    const archetypeNames = [
      "Low-Usage Playmaker",
      "Secondary Scorer",
      "3&D Wing",
      "Rim Protector",
      "Stretch Big",
      "Fringe Prospect",
      "Alpha Creator",
      "Defensive Specialist",
    ];

    const positions = ["NA", "PG", "SG", "SF", "PF", "C"];

    // Get the name of the archetype based on its index
    const archetypeName = archetypeNames[player.archetype];
    const positionName = positions[player.pos]

    // Navigate to player page on click
    const router = useRouter(); 
    const handleRowClick = (player: PlayerAnalyticsDTO) => {
      router.push(`/components/players/${player.playerId}`);
    };

    return (
      <tr key={index} onClick={() => handleRowClick(player)} className="cursor-pointer border-b border-gray-600 text-center font-bold">
        <td className="px-4 py-2 text-xl">{index + 1}</td>
        <td className="py-2">
          <Image
            src={`https://cdn.nba.com/headshots/nba/latest/260x190/${player.playerId}.png`}
            alt={player.playerName}
            width={120}
            height={120}
            className="h-30 w-30 rounded-full object-cover"
            priority
          />
        </td>
        <td className="px-4 py-2">{player.playerName}</td>
        <td className="px-4 py-2 text-lg">{player.age + player.state}</td>
        <td className="px-4 py-2 text-lg">{positionName}</td>
        <td className="px-4 py-2 text-lg">{player.team}</td>
        <td className="px-4 py-2">
          {/* Archetype Badge */}
          <span className={`px-2 py-1 rounded-md ${archetypeStyle.bg} ${archetypeStyle.text}`}>
            {archetypeName}
          </span>
        </td>
        <td className="px-4 py-2 text-lg">{player.impact.toFixed(1)}</td>
        <td className="px-4 py-2 text-lg">{player.efficiency.toFixed(1)}</td>
        <td className="px-4 py-2 text-lg">{player.shooting.toFixed(1)}</td>
        <td className="px-4 py-2 text-lg">{player.scoring.toFixed(1)}</td>
        <td className="px-4 py-2 text-lg">{player.playmaking.toFixed(1)}</td>
        <td className="px-4 py-2 text-lg">{player.interiorDefense.toFixed(1)}</td>
        <td className="px-4 py-2 text-lg">{player.perimeterDefense.toFixed(1)}</td>
        <td className="px-4 py-2 text-lg">{player.rebounding.toFixed(1)}</td>
      </tr>
    );
  };

  return (
    <div className="p-8">
      {/* Row for "Player Analytics" title, no. of results and filter button */}
      <div className="flex justify-between items-center mb-6 mt-2">

        {/* Filter Button */}
        <button
          onClick={() => setIsModalOpen(true)}
          className="bg-[#74069C] text-white font-bold px-6 py-3 rounded-md flex items-center space-x-2 cursor-pointer hover:bg-purple-900 focus:outline-none text-lg"
        >
          <span>Filter Options</span>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="w-5 h-5 text-white"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="2"
              d="M4 6h16M4 12h16M4 18h16"
            />
          </svg>
        </button>

        {/* Title */}
        <h1 className="text-4xl text-center font-bold">Player Analytics</h1>

        {/* Results Count */}
        <div className="text-xl font-bold text-gray-300">
          Displaying: {players.length} players
        </div>
      </div>

      {/* Filter Modal */}
      {isModalOpen && (
        <Filters
          ageMin={ageMin}
          ageMax={ageMax}
          yearsAhead={yearsAhead}
          teams={teams}
          pos={pos}
          archetypes={archetypes}
          sortBy={sortBy}
          order={order}
          handleFilterChange={handleFilterChange}
          handleFilterSubmit={handleFilterSubmit}
          closeModal={() => setIsModalOpen(false)}
        />
      )}

      <div>
        {/* Loading Indicator */}
        {loading && <p>Loading...</p>}

        {/* Error Message */}
        {error && <p>Error: {error}</p>}

        {/* Display "No Players Found" if players length is 0 */}
        {players.length === 0 ? (
          <p className="text-lg text-center text-gray-500">No players found</p>
        ) : (
          // Player Table
          <table className="min-w-full bg-gray-800 text-white border border-gray-600">
            <thead>
              <tr className="border-b border-gray-600">
                <th className="px-4 py-2 border-b">Rank</th>
                <th className="px-4 py-2 border-b">Image</th>
                <th className="px-4 py-2 border-b">Player Name</th>
                <th className="px-4 py-2 border-b">Age</th>
                <th className="px-4 py-2 border-b">Pos</th>
                <th className="px-4 py-2 border-b">Team</th>
                <th className="px-4 py-2 border-b">Archetype</th>
                <th className="px-4 py-2 border-b">Impact</th>
                <th className="px-4 py-2 border-b">Efficiency</th>
                <th className="px-4 py-2 border-b">Shooting</th>
                <th className="px-4 py-2 border-b">Scoring</th>
                <th className="px-4 py-2 border-b">Playmaking</th>
                <th className="px-4 py-2 border-b">I.Def</th>
                <th className="px-4 py-2 border-b">P.Def</th>
                <th className="px-4 py-2 border-b">Reb</th>
              </tr>
            </thead>
            <tbody>
              {players.map((player, index) => (
                <PlayerRow key={index} player={player} index={index}/>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

export default AnalyticsPage;
