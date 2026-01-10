"use client";
import Image from "next/image";
import { PlayerSummary } from "@/app/types/player";
import { ARCHETYPE_MAP } from "@/app/lib/archetype";

export const ARCHETYPE_BADGE_STYLE: Record<number,{ bg: string; text: string }> = {
  0: { bg: "bg-blue-500/20", text: "text-blue-300" },     // Low Usage Playmaker
  1: { bg: "bg-violet-500/20", text: "text-violet-300" }, // Secondary Scorer
  2: { bg: "bg-green-500/20", text: "text-green-300" },   // 3&D Wing
  3: { bg: "bg-red-500/20", text: "text-red-300" },       // Rim Protector
  4: { bg: "bg-amber-500/20", text: "text-amber-300" },   // Stretch Big
  5: { bg: "bg-gray-500/20", text: "text-gray-300" },     // Fringe Prospect
  6: { bg: "bg-pink-500/20", text: "text-pink-300" },     // Alpha Creator
  7: { bg: "bg-teal-500/20", text: "text-teal-300" },     // Defensive Specialist
};

export default function PlayerHeader({ player }: { player: PlayerSummary }) {
  
  const badgeStyle = ARCHETYPE_BADGE_STYLE[player.archetype] ?? {
    bg: "bg-neutral-700/40",
    text: "text-neutral-300",
  };

  return (
    <div className="flex justify-center">
      <div className="flex items-center gap-10">
        {/* Headshot */}
        <div className="relative w-50 h-50 rounded-full overflow-hidden
                        border border-white/10
                        shadow-[0_0_50px_rgba(168,85,247,0.25)]">
          <Image
            src={`https://cdn.nba.com/headshots/nba/latest/260x190/${player.playerId}.png`}
            alt={player.playerName}
            fill
            className="object-cover"
            priority
          />
        </div>

        {/* Text */}
        <div className="flex flex-col justify-center">
          <div className="mb-3">
            <span
              className={`inline-block px-5 py-1.5 rounded-full
                          text-base font-medium
                          ${badgeStyle.bg} ${badgeStyle.text}`}
            > 
              {ARCHETYPE_MAP[player.archetype] ?? "Unknown Archetype"}
            </span>
          </div>

          <h1 className="text-5xl font-semibold tracking-tight">
            {player.playerName}
          </h1>

          <p className="mt-2 text-xl text-white/60">
            {player.teamAbbr} · Age {player.age}
          </p>
        </div>
      </div>
    </div>
  );
}
