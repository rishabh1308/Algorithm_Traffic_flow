#pragma once
#include "../engine/TrafficEngine.hpp"

#include <vector>
#include <memory>
#include <cstdint>

namespace tip::coordination {

    /// Configuration for a single intersection within a corridor.
    struct IntersectionEntry {
        std::shared_ptr<engine::TrafficEngine> engine;
        int32_t offsetSeconds = 0; ///< Offset from corridor master clock (seconds)
    };

    /// Coordinates multiple intersections along a corridor
    /// using offset-based green wave synchronization.
    class CorridorCoordinator {
    public:
        /// Add an intersection to the corridor with its offset.
        void addIntersection(std::shared_ptr<engine::TrafficEngine> engine,
                             int32_t offsetSeconds);

        /// Run one global tick. Each intersection steps if its offset aligns.
        void tick(uint32_t globalTime);

        /// Get all decisions from the latest tick.
        [[nodiscard]] const std::vector<model::Decision>& lastDecisions() const noexcept {
            return decisions_;
        }

        /// Number of intersections.
        [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }

    private:
        std::vector<IntersectionEntry> entries_;
        std::vector<model::Decision>   decisions_;
    };

}
