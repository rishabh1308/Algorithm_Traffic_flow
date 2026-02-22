#pragma once
#include "BLEEvent.hpp"
#include "BLERegistry.hpp"
#include "../model/Direction.hpp"

#include <chrono>
#include <unordered_map>
#include <vector>
#include <deque>

namespace tip::ble {

    /// Configuration for BLE priority behavior.
    struct BLEConfig {
        std::chrono::seconds cooldownWindow{30};   ///< Min time between activations per device
        std::size_t          maxActivationsPerHour = 10; ///< Rate cap per device
    };

    /// Processes BLE events and computes per-direction boost values.
    class BLEPriorityManager {
    public:
        explicit BLEPriorityManager(BLEConfig config, BLERegistry registry);

        /// Process a BLE event. Returns true if the event was accepted.
        bool processEvent(const BLEEvent& event);

        /// Get aggregated BLE boost for a direction.
        [[nodiscard]] double getBoost(const model::Direction& dir) const;

        /// Clear all boosts (called after phase selection).
        void clearBoosts();

    private:
        BLEConfig  config_;
        BLERegistry registry_;

        /// Per-device: last activation time
        std::unordered_map<std::string, std::chrono::steady_clock::time_point> lastActivation_;

        /// Per-device: activation timestamps within the last hour
        std::unordered_map<std::string, std::deque<std::chrono::steady_clock::time_point>> activationHistory_;

        /// Accumulated boost per approach index
        std::unordered_map<uint16_t, double> directionBoosts_;

        [[nodiscard]] bool checkCooldown(const std::string& deviceId,
                                         std::chrono::steady_clock::time_point now) const;
        [[nodiscard]] bool checkRateLimit(const std::string& deviceId,
                                          std::chrono::steady_clock::time_point now) const;
        void recordActivation(const std::string& deviceId,
                              std::chrono::steady_clock::time_point now);
    };

}
