import { ProjectedAttributes, CurrentAttributes } from "@/app/types/player";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

interface ProgressionViewProps {
  current: CurrentAttributes
  projected: ProjectedAttributes[];
  attribute: keyof Omit<ProjectedAttributes, "playerNameClean" | "yearAhead">;
  onAttributeChange: (
    attr: keyof Omit<ProjectedAttributes, "playerNameClean" | "yearAhead">
  ) => void;
}

export default function ProgressionView({ current, projected, attribute, onAttributeChange }: ProgressionViewProps) {
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

    const ATTRIBUTE_LABELS: Record< (typeof ATTRIBUTES)[number], string > = {
        shooting: "Shooting",
        playmaking: "Playmaking",
        perimeterDefense: "Perimeter Defense",
        interiorDefense: "Interior Defense",
        rebounding: "Rebounding",
        scoring: "Scoring",
        efficiency: "Efficiency",
        impact: "Impact",
    };

    const timeline = [
        {
            year: 0,
            value: Number(current[attribute].toFixed(1)),
        },
        ...projected.map(p => ({
            year: p.yearAhead,
            value: Number(p[attribute].toFixed(1)),
        })),
    ];

    const values = timeline.map(d => d.value);
    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);

    const range = maxVal - minVal || 1; 
    const padding = Math.max(2, range * 0.15);

    const yMin = Math.max(0, minVal - padding);
    const yMax = Math.min(100, maxVal + padding);

    return (
        <div className="mt-8 grid grid-cols-12 gap-10">
            {/* Table */}
            <div className="col-span-5">
                {/* Select Attribute */}
                <select
                    value={attribute}
                    onChange={e =>
                        onAttributeChange(
                            e.target.value as keyof Omit<ProjectedAttributes, "playerNameClean" | "yearAhead">
                        )
                    }
                    className="bg-black border border-gray-700 px-3 py-2 rounded"
                    >
                    {ATTRIBUTES.map(attr => (
                        <option key={attr} value={attr}>
                            {ATTRIBUTE_LABELS[attr]}
                        </option>
                    ))}
                </select>
                {/* Table of Changes */}
                <table className="w-full text-left mt-6">
                    {/* Headers */}
                    <thead className="text-gray-400 border-b border-gray-700">
                        <tr>
                        <th>Year</th>
                        <th>Value</th>
                        <th>Δ</th>
                        </tr>
                    </thead>
                    {/* Values */}
                    <tbody>
                        {timeline.map((row, i) => {
                            const prev = timeline[i - 1]?.value;
                            const delta = prev !== undefined ? row.value - prev : null;
                            const roundedDelta = delta === null ? null : Number(delta.toFixed(1));
                        
                            const deltaClass =
                                roundedDelta === null
                                    ? "text-gray-500"
                                    : roundedDelta > 0
                                    ? "text-green-500"
                                    : roundedDelta < 0
                                    ? "text-red-500"
                                    : "text-gray-500";

                            const deltaText =
                                roundedDelta === null
                                    ? "—"
                                    : roundedDelta > 0
                                    ? `+${roundedDelta.toFixed(1)}`
                                    : roundedDelta.toFixed(1);

                            return (
                                <tr key={row.year} className="border-b border-gray-800">
                                    <td>{row.year === 0 ? "Current" : row.year}</td>
                                    <td className="text-purple-300">{row.value.toFixed(1)}</td>
                                    <td className={deltaClass}>{deltaText}</td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
            {/* Graph */}
            <div className="col-span-7">
                <div className="h-65">
                    <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={timeline}>
                        <XAxis
                            dataKey="year"
                            tickFormatter={y => (y === 0 ? "Current" : `+${y}`)}
                            stroke="#9ca3af"
                        />
                        <YAxis
                            domain={[yMin, yMax]}
                            stroke="#9ca3af"
                            tick={{ fill: "#9ca3af", fontSize: 12 }}
                            tickFormatter={(v: number) => v.toFixed(1)}
                        />
                        <Tooltip
                            formatter={(value: number | undefined) =>
                                value !== undefined
                                ? [value.toFixed(1), ATTRIBUTE_LABELS[attribute]]
                                : ["", ATTRIBUTE_LABELS[attribute]]
                            }
                            labelFormatter={(label: number) =>
                                label === 0 ? "Current" : `Year +${label}`
                            }
                            contentStyle={{
                                backgroundColor: "#000",
                                border: "1px solid #333",
                            }}
                        />
                        <Line
                            type="monotone"
                            dataKey="value"
                            stroke="#8b5cf6"
                            strokeWidth={2}
                            dot={{ r: 4 }}
                            activeDot={{ r: 6 }}
                        />
                        </LineChart>
                    </ResponsiveContainer>
                </div>
            </div>
        </div>
    );
}