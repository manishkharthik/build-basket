import Link from "next/link";

export default function HomePage() {
  return (
    <main className="min-h-screen bg-black text-white flex flex-col justify-between">
      
      {/* Hero section */}
      <section className="flex flex-col items-center justify-center px-6 pt-32 text-center">
        <h1 className="text-5xl font-bold tracking-tight mb-6">
          BuildBasket
        </h1>

        <p className="text-xl text-gray-300 max-w-2xl mb-6">
          NBA analytics taken to the next level.
        </p>

        <p className="text-gray-400 max-w-3xl leading-relaxed">
          BuildBasket helps you understand <span className="text-white">player archetypes</span>,
          track how players <span className="text-white">develop over time</span>, explore how the
          <span className="text-white"> league may evolve over the next 5 years</span>, and run
          deep <span className="text-white">player-to-player comparisons</span> powered by data-driven
          projections.
        </p>
      </section>

      {/* Bottom navigation tabs */}
      <section className="border-t border-gray-800">
        <div className="grid grid-cols-3 text-center">
          
          <Link
            href="/components/players"
            className="py-6 hover:bg-gray-900 transition"
          >
            <div className="text-lg font-medium">View Players</div>
            <div className="text-sm text-gray-400">
              Browse players & archetypes
            </div>
          </Link>

          <Link
            href="/components/analytics"
            className="py-6 hover:bg-gray-900 transition border-x border-gray-800"
          >
            <div className="text-lg font-medium">Future Deep-Dive</div>
            <div className="text-sm text-gray-400">
              League outlook & projections
            </div>
          </Link>

          <Link
            href="/components/comparisons"
            className="py-6 hover:bg-gray-900 transition"
          >
            <div className="text-lg font-medium">Player Comparator</div>
            <div className="text-sm text-gray-400">
              Head-to-head player analysis
            </div>
          </Link>

        </div>
      </section>
    </main>
  );
}
