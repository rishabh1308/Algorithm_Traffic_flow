
/// This is a placeholder heuristic. In production, replace computeAction()
/// with a trained policy network (DQN, PPO, A2C, etc.).

#include "rl/RLAgent.hpp"

#include <numeric>
#include <algorithm>
#include <cmath>

namespace tip::rl {

StateObservation RLAgent::observe(const engine::TrafficEngine& engine) const {
    StateObservation state;
    const auto& lanes = engine.lanes();

    state.queueLengths.reserve(lanes.size());
    state.waitCounters.reserve(lanes.size());

    for (const auto& lane : lanes) {
        state.queueLengths.push_back(lane.queueLength);
        state.waitCounters.push_back(lane.waitCounter);
    }

    return state;
}

TuningAction RLAgent::computeAction(const StateObservation& state) const {
    TuningAction action;

    if (state.queueLengths.empty()) return action;

    // Heuristic: if average wait is high, increase alpha (prioritize starved lanes)
    double avgWait = std::accumulate(state.waitCounters.begin(),
                                     state.waitCounters.end(), 0.0)
                   / static_cast<double>(state.waitCounters.size());

    double maxWait = static_cast<double>(
        *std::max_element(state.waitCounters.begin(), state.waitCounters.end()));

    // If starvation is detected (any lane waiting > 10 cycles), boost alpha
    if (maxWait > 10.0) {
        action.deltaAlpha = 0.1;
    } else if (avgWait < 2.0) {
        action.deltaAlpha = -0.05; // Relax when things are flowing
    }

    // Heuristic: if queues are long, increase green per vehicle slightly
    double avgQueue = std::accumulate(state.queueLengths.begin(),
                                      state.queueLengths.end(), 0.0)
                    / static_cast<double>(state.queueLengths.size());

    if (avgQueue > 15.0) {
        action.deltaGreenPerVeh = 0.1;
    } else if (avgQueue < 3.0) {
        action.deltaGreenPerVeh = -0.05;
    }

    return action;
}

void RLAgent::apply(engine::TrafficEngine& engine, const TuningAction& action) const {
    auto& cfg = engine.config();

    cfg.alpha = std::max(0.0, cfg.alpha + action.deltaAlpha);
    cfg.beta  = std::max(0.0, cfg.beta + action.deltaBeta);
    cfg.greenPerVehicle = std::clamp(
        cfg.greenPerVehicle + action.deltaGreenPerVeh, 1.0, 5.0);
}

void RLAgent::tune(engine::TrafficEngine& engine) {
    auto state  = observe(engine);
    auto action = computeAction(state);
    apply(engine, action);
}

}