#pragma once
#include <cstdint>

namespace tip::engine {

    /// Engine tuning parameters.
    struct EngineConfig {
        double   alpha     = 1.0;    ///< Weight for starvation fairness (W_i)
        double   beta      = 2.0;    ///< Weight for BLE boost (B_i)
        uint32_t minGreen  = 8;      ///< Minimum green duration (seconds)
        uint32_t maxGreen  = 60;     ///< Maximum green duration (seconds)
        uint32_t yellowTime = 4;     ///< Yellow clearance (seconds)
        uint32_t allRedTime = 2;     ///< All-red clearance (seconds)
        double   greenPerVehicle = 2.0; ///< Seconds of green per queued vehicle
    };

}
