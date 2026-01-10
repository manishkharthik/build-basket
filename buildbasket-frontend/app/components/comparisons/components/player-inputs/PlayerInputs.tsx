import PlayerAInput from "./PlayerAInput";
import PlayerBInput from "./PlayerBInput";
import { Player } from "@/app/types/player";

type PlayerInputsProps = {
  players: Player[];
  playerA: string;
  setPlayerA: (playerName: string) => void;
  stateA: number;
  setStateA: (state: number) => void;
  stateB: number;
  setStateB: (state: number) => void;
};

export default function PlayerInputs({
  players,
  playerA,
  setPlayerA,
  stateA,
  setStateA,
  stateB,
  setStateB
}: PlayerInputsProps) {
  return (
    <section className="rounded-2xl border border-white/10 bg-white/5 p-6 mt-8">
      <h2 className="mb-4 text-lg font-medium text-white/80">
        Player Inputs
      </h2>

      <div className="grid gap-6 md:grid-cols-2">
        <PlayerAInput
          players={players}
          playerA={playerA}
          setPlayerA={setPlayerA}
          stateA={stateA}
          setStateA={setStateA}
        />

        <PlayerBInput
          stateB={stateB}
          setStateB={setStateB}
        />
      </div>
    </section>
  );
}
