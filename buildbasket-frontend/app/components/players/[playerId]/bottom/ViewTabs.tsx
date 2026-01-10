type BottomTab = "progression" | "radar";

interface ViewTabsProps {
  active: BottomTab;
  onChange: (tab: BottomTab) => void;
}

export default function ViewTabs({ active, onChange }: ViewTabsProps) {
  return (
    <div className="flex gap-6 border-b border-gray-700 mb-8">
      {["progression", "radar"].map(tab => (
        <button
          key={tab}
          onClick={() => onChange(tab as BottomTab)}
          className={`pb-3 text-lg font-medium transition
            ${active === tab
              ? "text-white border-b-2 border-purple-400"
              : "text-gray-400 hover:text-white"
            }`}
        >
          {tab === "progression" ? "Progression" : "Radar Comparison"}
        </button>
      ))}
    </div>
  );
}
