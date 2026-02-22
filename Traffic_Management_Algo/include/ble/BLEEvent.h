#pragma once
#include "../model/Direction.hpp"

#include <string>
#include <chrono>

namespace tip::ble {

    /// A single BLE detection event from a roadside beacon.
    struct BLEEvent {
        std::string  deviceId;         ///< Unique BLE device identifier
        model::Direction direction;    ///< Approach direction of the detection
        double       weight = 1.0;     ///< Priority weight (e.g., bus=2.0, fleet=1.0)
        std::chrono::steady_clock::time_point timestamp = std::chrono::steady_clock::now();
    };

}