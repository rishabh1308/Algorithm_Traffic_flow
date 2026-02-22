
#include "coordination/CorridorCoordinator.hpp"

namespace tip::coordination {

    void CorridorCoordinator::addIntersection(
        std::shared_ptr<engine::TrafficEngine> engine,
        int32_t offsetSeconds)
    {
        entries_.push_back({std::move(engine), offsetSeconds});
        decisions_.resize(entries_.size());
    }

    void CorridorCoordinator::tick(uint32_t globalTime) {
        for (std::size_t i = 0; i < entries_.size(); ++i) {
            auto& entry = entries_[i];

            // Each intersection steps when (globalTime - offset) is non-negative
            // The offset shifts when the intersection "starts" its cycle
            int32_t localTime = static_cast<int32_t>(globalTime) - entry.offsetSeconds;

            if (localTime >= 0) {
                decisions_[i] = entry.engine->step();
            } else {
                // Not yet active — hold ALL_RED
                model::Decision hold;
                hold.phaseName = "WAITING";
                hold.signalState = model::SignalPhase::ALL_RED;
                decisions_[i] = hold;
            }
        }
    }

}