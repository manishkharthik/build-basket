interface ClosestPlayerFinderProps {
  onRun: () => void;
  disabled: boolean;
  loading: boolean;
}

export default function ClosestPlayerFinder({
  onRun,
  disabled,
  loading
}: ClosestPlayerFinderProps) {
  return (
    <section className="flex items-center gap-10 mt-10">
      <button
        onClick={onRun}
        disabled={disabled || loading}
        className="
          rounded-xl
          bg-white text-black
          text-lg font-medium
          px-6 py-2
          cursor-pointer
          transition-colors duration-200 ease-out
          hover:bg-white/10
        "
      >
        {loading ? "Comparing..." : "Find Closest Player"}
      </button>
    </section>
  );
}
