import {
  Radar,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  ResponsiveContainer,
} from "recharts";

import { CurrentAttributes } from "@/app/types/player";

interface PlayerRadarProps {
  current: CurrentAttributes;
  projected?: Partial<CurrentAttributes>;
}

export default function PlayerRadar({ current, projected }: PlayerRadarProps) {
  const data = [
    { stat: "Shooting", current: current.shooting, projected: projected?.shooting },
    { stat: "Playmaking", current: current.playmaking, projected: projected?.playmaking },
    { stat: "Perimeter D", current: current.perimeterDefense, projected: projected?.perimeterDefense },
    { stat: "Interior D", current: current.interiorDefense, projected: projected?.interiorDefense },
    { stat: "Rebounding", current: current.rebounding, projected: projected?.rebounding },
    { stat: "Scoring", current: current.scoring, projected: projected?.scoring },
    { stat: "Efficiency", current: current.efficiency, projected: projected?.efficiency },
    { stat: "Impact", current: current.impact, projected: projected?.impact },
  ];

  return (
    <ResponsiveContainer width="100%" height={500}>
      <RadarChart data={data}>
        <PolarGrid stroke="#333" />
        <PolarAngleAxis dataKey="stat" stroke="#aaa" />
        <PolarRadiusAxis
          domain={[0, 100]}
          tickCount={6}
          tick={{ fill: "#FFFFFF", fontSize: 0 }}
          axisLine={false}
        />

        {/* CURRENT */}
        <Radar
          dataKey="current"
          stroke="#74069C"
          fill="#74069C"
          fillOpacity={0.45}
        />

        {/* PROJECTED */}
        {projected && (
          <Radar
            dataKey="projected"
            stroke="#06369C"
            fill="#06369C"
            fillOpacity={0.45}
          />
        )}
      </RadarChart>
    </ResponsiveContainer>
  );
}
