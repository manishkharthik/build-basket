interface AttributeSelectorProps {
  useAll: boolean;
  setUseAll: (value: boolean) => void;
  selected: string[];
  setSelected: (value: string[] | ((prev: string[]) => string[])) => void;
  offensePreset: string[];
  defensePreset: string[];
}

export default function AttributeSelector({
  useAll,
  setUseAll,
  selected,
  setSelected,
  offensePreset,
  defensePreset
}: AttributeSelectorProps) {
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

  return (
    <section className="rounded-2xl border border-white/10 bg-white/5 p-6 mt-10">
      <h2 className="mb-4 text-lg font-medium text-white/80">
        Attribute Selector
      </h2>

      <div className="flex gap-3">
        <button
          type="button"
          onClick={() => {
            setUseAll(true);
            setSelected(ALL_ATTRS);
          }}
          className={`px-4 py-2 rounded-lg border text-sm transition
            ${useAll
              ? "bg-purple-500/20 border-purple-500 text-white"
              : "bg-black/30 border-white/10 text-white/60 hover:border-white/30"}
          `}
        >
          Use All Attributes
        </button>

        <button
          type="button"
          onClick={() => {
            setUseAll(false);
            setSelected([]);
          }}
          className={`px-4 py-2 rounded-lg border text-sm transition
            ${!useAll
              ? "bg-purple-500/20 border-purple-500 text-white"
              : "bg-black/30 border-white/10 text-white/60 hover:border-white/30"}
          `}
        >
          Use Custom Attributes
        </button>
      </div>

      <div className="flex gap-2 mb-4 mt-5">
        <button
          type="button"
          onClick={() => setSelected(offensePreset)}
          className="rounded-xl border border-white/10 px-3 py-1 text-sm disabled:opacity-60"
          disabled={useAll}
        >
          Offense preset
        </button>

        <button
          type="button"
          onClick={() => setSelected(defensePreset)}
          className="rounded-xl border border-white/10 px-3 py-1 text-sm disabled:opacity-60"
          disabled={useAll}
        >
          Defense preset
        </button>
      </div>

      <div className="grid grid-cols-2 gap-3 mt-4">
        {ALL_ATTRS.map(attr => {
          const isSelected = selected.includes(attr);

          return (
            <button
              key={attr}
              type="button"
              disabled={useAll}
              onClick={() => {
                if (isSelected) {
                  setSelected(prev => prev.filter(a => a !== attr));
                } else {
                  setSelected(prev => [...prev, attr]);
                }
              }}
              className={`px-3 py-2 rounded-lg border text-sm transition
                ${isSelected
                    ? "bg-purple-500/20 border-purple-500 text-white"
                    : "bg-black/30 border-white/10 text-white/60 hover:border-white/30"}
              `}
            >
              {attr}
            </button>
          );
        })}
      </div>
    </section>
  );
}
