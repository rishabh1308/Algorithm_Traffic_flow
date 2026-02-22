#pragma once

/// For N-way intersections, generates phases by pairing opposing approaches:
///   - For each opposing pair: one THROUGH phase + one LEFT_PROTECTED phase
///   - For odd N (no true opposing): each approach gets its own phase
///
/// Example 4-way: NS-through, NS-left, EW-through, EW-left (4 phases)
/// Example 6-way: A0A3-through, A0A3-left, A1A4-through, A1A4-left, A2A5-through, A2A5-left (6 phases)
/// Example 3-way: A0-through, A0-left, A1-through, A1-left, A2-through, A2-left (up to 6 phases)

#include "../model/Lane.hpp"
#include "../model/Phase.hpp"
#include "../model/ConflictMatrix.hpp"

#include <vector>

namespace tip::engine {

    /// Builds structured phase plans automatically from lane configuration.
    class PhaseBuilder {
    public:
        /// Generate the phase plan from an arbitrary lane set.
        /// Validates that no phase contains conflicting lanes.
        [[nodiscard]] static std::vector<model::Phase> build(
            const std::vector<model::Lane>& lanes,
            const model::ConflictMatrix& conflicts);

    private:
        /// Collect lane indices matching a set of approach indices and a movement type.
        [[nodiscard]] static std::vector<std::size_t> collectLanes(
            const std::vector<model::Lane>& lanes,
            const std::vector<uint16_t>& approachIndices,
            model::MovementType movement);

        /// Verify no internal conflicts within a phase.
        static void validatePhase(
            const model::Phase& phase,
            const model::ConflictMatrix& conflicts);
    };

}