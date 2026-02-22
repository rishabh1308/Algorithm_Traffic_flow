#pragma once
#include "Phase.hpp"
#include "SignalPhase.hpp"
#include "PriorityReason.hpp"

#include <string>
#include <cstdint>

namespace tip::model {

    /// Encapsulates the engine's decision for the current cycle.
    struct Decision {
        std::size_t     selectedPhaseIndex = 0;      /// Index into the phase plan
        std::string     phaseName;                    /// Human-readable phase name
        SignalPhase     signalState = SignalPhase::GREEN;
        double          phaseScore  = 0.0;            /// Computed adaptive score
        uint32_t        greenDuration = 0;            /// Computed green time (seconds)
        PriorityReason  activePriority = PriorityReason::NONE;

        [[nodiscard]] std::string summary() const {
            return "Phase: " + phaseName
                 + " | Signal: " + to_string(signalState)
                 + " | Score: " + std::to_string(phaseScore)
                 + " | Green: " + std::to_string(greenDuration) + "s"
                 + " | Priority: " + to_string(activePriority);
        }
    };

}