#pragma once
/// This is a tuning interface — the RL agent observes intersection state
/// and adjusts engine parameters (alpha, beta, greenPerVehicle) to
/// optimize throughput and fairness metrics.

#include "../engine/TrafficEngine.hpp"

#include <vector>
#include <cstdint>

namespace tip::rl {

    /// State observation for the RL agent.
    struct StateObservation {
        std::vector<uint32_t> queueLengths;
        std::vector<uint32_t> waitCounters;
        double                lastPhaseScore = 0.0;
        uint32_t              lastGreenDuration = 0;
    };

    /// Action: parameter deltas to apply.
    struct TuningAction {
        double deltaAlpha         = 0.0;
        double deltaBeta          = 0.0;
        double deltaGreenPerVeh   = 0.0;
    };

    /// Simple RL agent hook for parameter tuning.
    /// In production, replace with a trained policy (DQN, PPO, etc.).
    class RLAgent {
    public:
        /// Observe current state from the engine.
        [[nodiscard]] StateObservation observe(const engine::TrafficEngine& engine) const;

        /// Compute a tuning action (placeholder: rule-based heuristic).
        [[nodiscard]] TuningAction computeAction(const StateObservation& state) const;

        /// Apply tuning action to the engine's config.
        void apply(engine::TrafficEngine& engine, const TuningAction& action) const;

        /// Full observe → act → apply cycle.
        void tune(engine::TrafficEngine& engine);
    };

}