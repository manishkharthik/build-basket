"use client";

import React from "react";

interface FiltersProps {
  ageMin: number | null;
  ageMax: number | null;
  yearsAhead: number;
  teams: string[];
  pos: number[];
  archetypes: number[];
  sortBy: string | null;
  order: string | null;
  handleFilterChange: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => void;
  handleFilterSubmit: (e: React.FormEvent) => void;
  closeModal: () => void;
}

const archetypeOptions = [
  { value: 0, label: "Low Usage Playmaker" },
  { value: 1, label: "Secondary Scorer" },
  { value: 2, label: "3&D Wing" },
  { value: 3, label: "Rim Protector" },
  { value: 4, label: "Stretch Big" },
  { value: 5, label: "Fringe Prospect" },
  { value: 6, label: "Alpha Creator" },
  { value: 7, label: "Defensive Specialist" }
];

const Filters: React.FC<FiltersProps> = ({
  ageMin,
  ageMax,
  yearsAhead,
  pos,
  archetypes,
  sortBy,
  order,
  handleFilterChange,
  handleFilterSubmit,
  closeModal
}) => {

  return (
    <div className="fixed inset-0 bg-gray-700 bg-opacity-50 flex justify-center items-center">
      <div className="bg-gray-800 text-white border border-gray-600 w-1/2">
        <h2 className="mt-5 text-2xl text-center font-bold mb-4">Filter Options</h2>
        <form onSubmit={handleFilterSubmit}>
          {/* Age Range */}
          <div className="mb-8">
            <label className="block text-xl font-bold mb-5 ml-5">1.) Current Age </label>
            <div className="flex space-x-10">
              <div className="flex space-x-5">
                <label htmlFor="ageMin" className="text-lg mt-2 ml-5">From</label>
                <input
                  type="number"
                  id="ageMin"
                  name="ageMin"
                  value={ageMin ?? ""}
                  onChange={handleFilterChange}
                  className="p-2 border border-gray-300 rounded-md"
                  placeholder="Min Age"
                />
              </div>
              <div className="flex space-x-5">
                <label htmlFor="ageMax" className="text-lg mt-2 ml-5">To</label>
                <input
                  type="number"
                  id="ageMax"
                  name="ageMax"
                  value={ageMax ?? ""}
                  onChange={handleFilterChange}
                  className="p-2 border border-gray-300 rounded-md"
                  placeholder="Max Age"
                />
              </div>
            </div>
          </div>
          {/* Years Ahead */}
          <div className="mb-8">
            <label className="block text-xl font-bold mb-5 ml-5">2.) Years Ahead</label>
            <div className="flex ml-5 space-x-4">
              {[0, 1, 2, 3, 4, 5].map((value) => (
                <div key={value} className="flex items-center">
                  <input
                    type="radio"
                    name="yearsAhead"
                    value={value}
                    checked={yearsAhead === value}
                    onChange={handleFilterChange}
                    className="mr-2"
                  />
                  <label>{value}</label>
                </div>
              ))}
            </div>
          </div>

          {/* Position */}
          <div className="mb-8">
            <label className="block text-xl font-bold mb-5 ml-5">3.) Position</label>
            <div className="flex space-x-4 ml-5">
                {/* Checkbox for each position, defaulting to "ALL" (1, 2, 3, 4, 5) */}
                <div>
                <input
                    type="checkbox"
                    name="pos"
                    value="1"
                    checked={pos.includes(1)}
                    onChange={handleFilterChange}
                    className="mr-2"
                />
                <label>PG</label>
                </div>
                <div>
                <input
                    type="checkbox"
                    name="pos"
                    value="2"
                    checked={pos.includes(2)}
                    onChange={handleFilterChange}
                    className="mr-2"
                />
                <label>SG</label>
                </div>
                <div>
                <input
                    type="checkbox"
                    name="pos"
                    value="3"
                    checked={pos.includes(3)}
                    onChange={handleFilterChange}
                    className="mr-2"
                />
                <label>SF</label>
                </div>
                <div>
                <input
                    type="checkbox"
                    name="pos"
                    value="4"
                    checked={pos.includes(4)}
                    onChange={handleFilterChange}
                    className="mr-2"
                />
                <label>PF</label>
                </div>
                <div>
                <input
                    type="checkbox"
                    name="pos"
                    value="5"
                    checked={pos.includes(5)}
                    onChange={handleFilterChange}
                    className="mr-2"
                />
                <label>C</label>
                </div>
            </div>
          </div>

          {/* Archetype */}
          <label className="block text-xl font-bold mb-5 ml-5">4.) Archetype</label>
          <div className="mb-8">
            <div className="flex flex-wrap gap-4 ml-5">
              {archetypeOptions.slice(0, 4).map((option) => (
                <div key={option.value} className="flex items-center">
                  <input
                    type="checkbox"
                    name="archetypes"
                    value={option.value}
                    checked={archetypes.includes(option.value)}
                    onChange={handleFilterChange}
                    className="mr-2"
                  />
                  <label>{option.label}</label>
                </div>
              ))}
            </div>
            <div className="flex flex-wrap gap-4 ml-5 mt-4">
              {archetypeOptions.slice(4).map((option) => (
                <div key={option.value} className="flex items-center">
                  <input
                    type="checkbox"
                    name="archetypes"
                    value={option.value}
                    checked={archetypes.includes(option.value)}
                    onChange={handleFilterChange}
                    className="mr-2"
                  />
                  <label>{option.label}</label>
                </div>
              ))}
            </div>
          </div>

          {/* Sorting */}
          <div className="mb-8">
            <label className="block text-xl font-bold mb-5 ml-5">5a.) Sort By</label>
            <select
              name="sortBy"
              value={sortBy ?? ""}
              onChange={handleFilterChange}
              className="p-2 border border-gray-300 rounded-md ml-5"
            >
              <option value="player_name">Player Name</option>
              <option value="age">Age</option>
              <option value="impact">Impact</option>
              <option value="efficiency">Efficiency</option>
              <option value="shooting">Shooting</option>
              <option value="scoring">Scoring</option>
              <option value="playmaking">Playmaking</option>
              <option value="interior_defense">I.Defense</option>
              <option value="perimeter_defense">P.Defense</option>
              <option value="rebounding">Rebounding</option>
            </select>
          </div>

          <div className="mb-8">
            <label className="block text-xl font-bold mb-5 ml-5">5b.) Order</label>
            <div className="flex items-center gap-4">
              <div>
                <input
                  type="radio"
                  name="order"
                  value="ASC"
                  checked={order === "ASC"}
                  onChange={handleFilterChange}
                  className="ml-5 mr-2"
                  disabled={!sortBy}
                />
                <label>Ascending</label>
              </div>

              <div>
                <input
                  type="radio"
                  name="order"
                  value="DESC"
                  checked={order === "DESC"}
                  onChange={handleFilterChange}
                  className="mr-2"
                  disabled={!sortBy}
                />
                <label>Descending</label>
              </div>
            </div>
          </div>
          
          {/* Apply and Exit buttons */}
          <div className="flex justify-between mt-4">
            <button
              type="button"
              onClick={closeModal}
              className="bg-[#5b5556] text-white text-lg font-bold px-6 py-3 rounded-md flex items-center space-x-2 cursor-pointer hover:bg-gray-700 focus:outline-none"
            >
              Cancel changes
            </button>
            <button 
              type="submit" 
              className="bg-[#066d9c] text-white text-lg font-bold px-6 py-3 rounded-md flex items-center space-x-2 cursor-pointer hover:bg-blue-700 focus:outline-none"
            >
              Apply filters
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default Filters;
