#pragma once
#include <vector>
#include <string>
#include <cstdint>

namespace tip::model {

    /// A structured phase representing a compatible group of lanes.
    struct Phase {
        std::string              name;       ///< Human-readable label (e.g., "NS-through")
        std::vector<std::size_t> laneIndices; ///< Indices into the lane vector

        Phase() = default;
        Phase(std::string n, std::vector<std::size_t> idx)
            : name(std::move(n)), laneIndices(std::move(idx)) {}
    };

}