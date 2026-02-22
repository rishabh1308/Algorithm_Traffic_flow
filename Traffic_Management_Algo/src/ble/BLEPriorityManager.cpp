
#include "ble/BLEPriorityManager.hpp"
#include <algorithm>

namespace tip::ble {

BLEPriorityManager::BLEPriorityManager(BLEConfig config, BLERegistry registry)
    : config_(std::move(config)), registry_(std::move(registry)) {}

bool BLEPriorityManager::processEvent(const BLEEvent& event) {
    // Authorization check
    if (!registry_.isAuthorized(event.deviceId)) {
        return false;
    }

    auto now = event.timestamp;

    // Cooldown check
    if (!checkCooldown(event.deviceId, now)) {
        return false;
    }

    // Rate limit check
    if (!checkRateLimit(event.deviceId, now)) {
        return false;
    }

    // Accept the event
    recordActivation(event.deviceId, now);

    // Accumulate boost for the approach index
    directionBoosts_[event.direction.index] += event.weight;

    return true;
}

double BLEPriorityManager::getBoost(const model::Direction& dir) const {
    auto it = directionBoosts_.find(dir.index);
    if (it != directionBoosts_.end()) {
        return it->second;
    }
    return 0.0;
}

void BLEPriorityManager::clearBoosts() {
    directionBoosts_.clear();
}

bool BLEPriorityManager::checkCooldown(const std::string& deviceId,
                                        std::chrono::steady_clock::time_point now) const {
    auto it = lastActivation_.find(deviceId);
    if (it == lastActivation_.end()) {
        return true; // First activation
    }
    return (now - it->second) >= config_.cooldownWindow;
}

bool BLEPriorityManager::checkRateLimit(const std::string& deviceId,
                                         std::chrono::steady_clock::time_point now) const {
    auto it = activationHistory_.find(deviceId);
    if (it == activationHistory_.end()) {
        return true;
    }

    const auto& history = it->second;
    auto oneHourAgo = now - std::chrono::hours(1);

    auto count = std::count_if(history.begin(), history.end(),
        [&](const auto& ts) { return ts >= oneHourAgo; });

    return static_cast<std::size_t>(count) < config_.maxActivationsPerHour;
}

void BLEPriorityManager::recordActivation(const std::string& deviceId,
                                            std::chrono::steady_clock::time_point now) {
    lastActivation_[deviceId] = now;

    auto& history = activationHistory_[deviceId];
    history.push_back(now);

    auto oneHourAgo = now - std::chrono::hours(1);
    while (!history.empty() && history.front() < oneHourAgo) {
        history.pop_front();
    }
}

}
