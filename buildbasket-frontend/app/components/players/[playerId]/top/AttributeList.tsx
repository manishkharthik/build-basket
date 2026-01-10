import { DisplayAttributes } from "@/app/types/player";

export const ATTRIBUTE_DISPLAY_ORDER: {
  key: keyof DisplayAttributes;
  label: string;
  section?: "offense" | "defense" | "other";
}[] = [
  { key: "shooting", label: "Shooting", section: "offense" },
  { key: "playmaking", label: "Playmaking", section: "offense" },

  { key: "perimeterDefense", label: "Perimeter Defense", section: "defense" },
  { key: "interiorDefense", label: "Interior Defense", section: "defense" },

  { key: "rebounding", label: "Rebounding", section: "other" },
  { key: "scoring", label: "Scoring", section: "other" },
  { key: "efficiency", label: "Efficiency", section: "other" },
  { key: "impact", label: "Impact", section: "other" },
];

export default function AttributeList({ attributes }: { attributes: DisplayAttributes }) {
  return (
    <div className="space-y-2 text-xl mt-22 mr-15">
      {ATTRIBUTE_DISPLAY_ORDER.map((attr, index) => {
        const prev = ATTRIBUTE_DISPLAY_ORDER[index - 1];
        const addSpacing =
          attr.section === "defense" && prev?.section !== "defense";

        return (
          <div
            key={attr.key}
            className={`flex justify-between border-b border-gray-800 pb-1
              ${addSpacing ? "mt-4" : ""}
            `}
          >
            <span>{attr.label}</span>
            <span className="text-purple-400">
              {attributes[attr.key].toFixed(1)}
            </span>
          </div>
        );
      })}
    </div>
  );
}
