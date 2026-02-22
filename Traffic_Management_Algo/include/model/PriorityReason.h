#pragma once
#include <cstdint>
#include <string>
#include <stdexcept>

namespace tip::model {

    /// Priority request categories ordered by precedence.
    enum class PriorityReason : uint8_t {
        NONE      = 0,  ///< No priority active
        BLE       = 1,  ///< BLE beacon detected (transit, fleet)
        EMERGENCY = 2   ///< Emergency vehicle — highest priority, always overrides
    };

    [[nodiscard]] inline std::string to_string(PriorityReason p) {
        switch (p) {
            case PriorityReason::NONE:      return "NONE";
            case PriorityReason::BLE:       return "BLE";
            case PriorityReason::EMERGENCY: return "EMERGENCY";
        }
        throw std::invalid_argument("Unknown PriorityReason");
    }

}
