export default function PlayerBInput({ stateB, setStateB }: { stateB: number; setStateB: (value: number) => void }) {
  return (
    <div>
      <h3 className="mb-2 text-sm font-medium text-white/80">
        Player B Pool
      </h3>

      <label className="block text-sm text-white/60">
        Candidate State
      </label>
      <select
        value={stateB}
        onChange={e => setStateB(Number(e.target.value))}
        className="mt-3 w-full rounded-xl bg-black/30 border border-white/10 px-4 py-2"
      >
        {[0,1,2,3,4,5].map(y => (
          <option key={y} value={y}>
            {y === 0 ? "Current" : `Year +${y}`}
          </option>
        ))}
      </select>

      <p className="mt-3 text-sm text-white/50">
        Closest Player B will be selected from this pool.
      </p>
    </div>
  );
}
