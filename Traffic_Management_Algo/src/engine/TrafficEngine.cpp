
#include "engine/TrafficEngine.hpp"

#include <algorithm>
#include <numeric>
#include <cmath>
#include <stdexcept>

namespace tip::engine {

TrafficEngine::TrafficEngine(std::vector<model::Lane> lanes, EngineConfig config)
    : lanes_(std::move(lanes))
    , config_(config)
    , conflicts_(lanes_)
    , phases_(PhaseBuilder::build(lanes_, conflicts_))
    , currentSignal_(model::SignalPhase::ALL_RED)
    , currentPhaseIdx_(0)
    , remainingTime_(config_.allRedTime)  // Start with all-red
{
    if (lanes_.empty()) {
        throw std::runtime_error("TrafficEngine: Cannot initialize with zero lanes");
    }
}

model::Decision TrafficEngine::step() {
    model::Decision decision;

    // If time remains in current state, decrement and return current state info
    if (remainingTime_ > 0) {
        --remainingTime_;
        decision.selectedPhaseIndex = currentPhaseIdx_;
        decision.phaseName = phases_[currentPhaseIdx_].name;
        decision.signalState = currentSignal_;
        decision.greenDuration = remainingTime_;
        return decision;
    }

    // Time expired — advance the state machine
    switch (currentSignal_) {
        case model::SignalPhase::GREEN: {
            // GREEN → YELLOW
            currentSignal_ = model::SignalPhase::YELLOW;
            remainingTime_ = config_.yellowTime;
            break;
        }
        case model::SignalPhase::YELLOW: {
            // YELLOW → ALL_RED
            currentSignal_ = model::SignalPhase::ALL_RED;
            remainingTime_ = config_.allRedTime;
            break;
        }
        case model::SignalPhase::ALL_RED: {
            // ALL_RED → select next phase → GREEN

            // Check for emergency override first
            auto emergencyPhase = findEmergencyPhase();
            if (emergencyPhase.has_value()) {
                currentPhaseIdx_ = emergencyPhase.value();
                decision.activePriority = model::PriorityReason::EMERGENCY;
            } else {
                currentPhaseIdx_ = selectBestPhase();
                // Check if selected phase has BLE priority
                for (auto idx : phases_[currentPhaseIdx_].laneIndices) {
                    if (lanes_[idx].priorityReason == model::PriorityReason::BLE) {
                        decision.activePriority = model::PriorityReason::BLE;
                        break;
                    }
                }
            }

            // Compute green duration
            uint32_t greenTime = computeGreenDuration(phases_[currentPhaseIdx_]);
            currentSignal_ = model::SignalPhase::GREEN;
            remainingTime_ = greenTime;

            // Update fairness counters
            updateFairness(currentPhaseIdx_);

            decision.greenDuration = greenTime;
            decision.phaseScore = scorePhase(phases_[currentPhaseIdx_]);
            break;
        }
    }

    decision.selectedPhaseIndex = currentPhaseIdx_;
    decision.phaseName = phases_[currentPhaseIdx_].name;
    decision.signalState = currentSignal_;

    return decision;
}

std::optional<std::size_t> TrafficEngine::findEmergencyPhase() const {
    // Find the first phase containing an emergency-priority lane
    for (std::size_t p = 0; p < phases_.size(); ++p) {
        for (auto laneIdx : phases_[p].laneIndices) {
            if (lanes_[laneIdx].priorityReason == model::PriorityReason::EMERGENCY) {
                return p;
            }
        }
    }
    return std::nullopt;
}

std::size_t TrafficEngine::selectBestPhase() const {
    std::size_t bestIdx = 0;
    double bestScore = -1.0;

    for (std::size_t p = 0; p < phases_.size(); ++p) {
        double s = scorePhase(phases_[p]);
        if (s > bestScore) {
            bestScore = s;
            bestIdx = p;
        }
    }

    return bestIdx;
}

double TrafficEngine::scorePhase(const model::Phase& phase) const {
    double total = 0.0;
    for (auto idx : phase.laneIndices) {
        total += lanes_[idx].score(config_.alpha, config_.beta);
    }
    return total;
}

uint32_t TrafficEngine::computeGreenDuration(const model::Phase& phase) const {
    // Sum queue lengths across phase lanes
    uint32_t totalQueue = 0;
    for (auto idx : phase.laneIndices) {
        totalQueue += lanes_[idx].queueLength;
    }

    // Proportional green time, bounded
    auto rawGreen = static_cast<uint32_t>(
        std::ceil(static_cast<double>(totalQueue) * config_.greenPerVehicle));

    return std::clamp(rawGreen, config_.minGreen, config_.maxGreen);
}

void TrafficEngine::updateFairness(std::size_t selectedPhaseIdx) {
    // Build a set of lanes in the selected phase for O(1) lookup
    const auto& selectedLanes = phases_[selectedPhaseIdx].laneIndices;

    for (auto& lane : lanes_) {
        bool inPhase = std::find(selectedLanes.begin(), selectedLanes.end(), lane.id)
                       != selectedLanes.end();
        if (inPhase) {
            lane.resetWait();     // W_i(t+1) = 0 if green
        } else {
            lane.incrementWait(); // W_i(t+1) = W_i(t) + 1 otherwise
        }
    }
}

}