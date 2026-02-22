#pragma once

/// Each lane has a direction (approach index), movement type, queue length,
/// starvation counter, BLE boost, and priority state.

#include "Direction.hpp"
#include "MovementType.hpp"
#include "PriorityReason.hpp"
#include "Point.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace tip::model {

    /// Represents a single lane at an intersection.
    struct Lane {
        std::size_t        id;                                     /// Unique lane identifier
        Direction          direction;                              /// Approach direction
        MovementType       movement;                               /// Movement type
        std::vector<Point> path;                                   /// Centerline polyline through intersection
        uint32_t        queueLength    = 0;                     /// Current vehicle queue (Q_i)
        uint32_t        waitCounter    = 0;                     /// Starvation fairness counter (W_i)
        double          bleBoost       = 0.0;                   /// BLE priority boost (B_i)
        PriorityReason  priorityReason = PriorityReason::NONE;  /// Active priority

        /// Compute adaptive score: S_i = Q_i + α·W_i + β·B_i
        [[nodiscard]] double score(double alpha, double beta) const noexcept {
            return static_cast<double>(queueLength)
                 + alpha * static_cast<double>(waitCounter)
                 + beta  * bleBoost;
        }

        /// Reset starvation counter (called when lane gets green).
        void resetWait() noexcept { waitCounter = 0; }

        /// Increment starvation counter (called when lane does NOT get green).
        void incrementWait() noexcept { ++waitCounter; }

        /// Human-readable label.
        [[nodiscard]] std::string label() const {
            return to_string(direction) + "-" + to_string(movement)
                 + " [id=" + std::to_string(id) + "]";
        }
    };

}