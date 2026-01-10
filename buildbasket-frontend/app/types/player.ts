export interface PlayerSummary {
  playerId: number;
  playerName: string;
  playerNameClean: string;
  age: number;
  teamAbbr: string | null;
  archetype: number;
}

export interface CurrentAttributes {
  playerNameClean: string;
  shooting: number;
  playmaking: number;
  perimeterDefense: number;
  interiorDefense: number;
  rebounding: number;
  scoring: number;
  efficiency: number;
  impact: number;
}

export type DisplayAttributes = Omit<CurrentAttributes, "playerNameClean">;

export interface ProjectedAttributes {
  playerNameClean: string;
  yearAhead: number;
  shooting: number;
  playmaking: number;
  perimeterDefense: number;
  interiorDefense: number;
  rebounding: number;
  scoring: number;
  efficiency: number;
  impact: number;
}

export interface AttributePercentiles {
  playerNameClean: string;
  shooting: number;
  playmaking: number;
  perimeterDefense: number;
  interiorDefense: number;
  rebounding: number;
  scoring: number;
  efficiency: number;
  impact: number;
}

export interface ProjectedAttributePercentiles extends AttributePercentiles {
  yearAhead: number;
}

export interface Player {
  playerId: string;
  playerName: string;
  playerNameClean: string;
  teamAbbr: string;
}

export interface ComparisonOutput {
  playerA: PlayerSide;
  closestPlayer: PlayerSide;
  top5: Top5Entry[];
  similarityScore: number;
}

export type PlayerSide = {
  name: string;
  state: number;
  attributes: Record<string, number>;
};

export type Top5Entry = {
  name: string;
  state: number;
  distance: number;
};