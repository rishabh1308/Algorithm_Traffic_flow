#pragma once
/// Responsibilities:
///   - Adaptive phase scoring: S_i = Q_i + α·W_i + β·B_i
///   - Emergency override detection
///   - Signal state machine (GREEN → YELLOW → ALL_RED → GREEN)
///   - Green duration computation (proportional, bounded)
///   - Starvation fairness updates

#include "EngineConfig.hpp"
#include "PhaseBuilder.hpp"
#include "../model/Lane.hpp"
#include "../model/Phase.hpp"
#include "../model/ConflictMatrix.hpp"
#include "../model/Decision.hpp"
#include "../ble/BLEPriorityManager.hpp"

#include <vector>
#include <optional>
#include <memory>

namespace tip::engine {

/// The main traffic control engine for a single intersection.
class TrafficEngine {
public:
    /// Construct engine with lanes and configuration.
    TrafficEngine(std::vector<model::Lane> lanes, EngineConfig config);

    /// Run one decision cycle. Returns the decision for this step.
    [[nodiscard]] model::Decision step();

    /// Access lanes for external updates (queue, priority, BLE boost).
    [[nodiscard]] std::vector<model::Lane>& lanes() noexcept { return lanes_; }
    [[nodiscard]] const std::vector<model::Lane>& lanes() const noexcept { return lanes_; }

    /// Access config for RL parameter tuning.
    [[nodiscard]] EngineConfig& config() noexcept { return config_; }

    /// Get current signal state.
    [[nodiscard]] model::SignalPhase currentSignal() const noexcept { return currentSignal_; }

    /// Read-only access to the conflict matrix.
    [[nodiscard]] const model::ConflictMatrix& conflictMatrix() const noexcept { return conflicts_; }

    /// Get the phase plan.
    [[nodiscard]] const std::vector<model::Phase>& phases() const noexcept { return phases_; }

private:
    std::vector<model::Lane>  lanes_;
    EngineConfig              config_;
    model::ConflictMatrix     conflicts_;
    std::vector<model::Phase> phases_;

    model::SignalPhase currentSignal_    = model::SignalPhase::ALL_RED;
    std::size_t        currentPhaseIdx_  = 0;
    uint32_t           remainingTime_    = 0; ///< Ticks remaining in current signal state

    /// Check for emergency override across all lanes.
    [[nodiscard]] std::optional<std::size_t> findEmergencyPhase() const;

    /// Score all phases and return the best index.
    [[nodiscard]] std::size_t selectBestPhase() const;

    /// Compute score for a single phase.
    [[nodiscard]] double scorePhase(const model::Phase& phase) const;

    /// Compute green duration for selected phase.
    [[nodiscard]] uint32_t computeGreenDuration(const model::Phase& phase) const;

    /// Update starvation counters after phase selection.
    void updateFairness(std::size_t selectedPhaseIdx);
};

}