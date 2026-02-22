#pragma once
/// Thread-safe for concurrent reads after construction.

#include "Lane.hpp"
#include "Geometry.hpp"

#include <vector>
#include <cstdint>
#include <cstddef>
#include <stdexcept>

namespace tip::model {

    /// Bitmask type — supports up to 64 lanes.
    using LaneMask = uint64_t;

    /// N×N bitmask conflict matrix; immutable after construction.
    class ConflictMatrix {
    public:
        /// Build the conflict matrix from lane path geometry.
        /// @throws std::runtime_error if lane count exceeds 64.
        explicit ConflictMatrix(const std::vector<Lane>& lanes);

        /// Query whether two lanes conflict.
        [[nodiscard]] bool conflicts(std::size_t i, std::size_t j) const noexcept {
            if (i >= n_ || j >= n_) return true;
            return (mask_[i] >> j) & 1U;
        }

        /// Check whether a set of simultaneously active lanes is conflict-free.
        [[nodiscard]] bool isFeasible(LaneMask activeMask) const noexcept;

        /// Get the conflict mask for a single lane.
        [[nodiscard]] LaneMask conflictsOf(std::size_t i) const noexcept {
            if (i >= n_) return ~LaneMask{0};
            return mask_[i];
        }

        [[nodiscard]] std::size_t size() const noexcept { return n_; }

    private:
        std::size_t n_;
        std::vector<LaneMask> mask_;
    };

} -