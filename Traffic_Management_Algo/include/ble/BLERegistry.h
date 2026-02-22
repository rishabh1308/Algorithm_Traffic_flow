#pragma once
#include <string>
#include <unordered_set>

namespace tip::ble {

    /// Maintains a whitelist of authorized BLE device IDs.
    class BLERegistry {
    public:
        void authorize(const std::string& deviceId) {
            authorized_.insert(deviceId);
        }

        void revoke(const std::string& deviceId) {
            authorized_.erase(deviceId);
        }

        [[nodiscard]] bool isAuthorized(const std::string& deviceId) const {
            return authorized_.contains(deviceId);
        }

    private:
        std::unordered_set<std::string> authorized_;
    };

}
