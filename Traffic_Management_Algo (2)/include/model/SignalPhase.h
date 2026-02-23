#pragma once

#include <cstdint>
#include <string>
#include <stdexcept>

namespace tip::model {

    /// Traffic signal display states following standard sequencing.
    /// State machine: GREEN → YELLOW → ALL_RED → GREEN
    enum class SignalPhase : uint8_t {
        GREEN   = 0,
        YELLOW  = 1,
        ALL_RED = 2
    };

    [[nodiscard]] inline std::string to_string(SignalPhase s) {
        switch (s) {
            case SignalPhase::GREEN:   return "GREEN";
            case SignalPhase::YELLOW:  return "YELLOW";
            case SignalPhase::ALL_RED: return "ALL_RED";
        }
        throw std::invalid_argument("Unknown SignalPhase");
    }

    /// Advance to the next state in the signal cycle.
    [[nodiscard]] constexpr SignalPhase next_phase(SignalPhase s) noexcept {
        switch (s) {
            case SignalPhase::GREEN:   return SignalPhase::YELLOW;
            case SignalPhase::YELLOW:  return SignalPhase::ALL_RED;
            case SignalPhase::ALL_RED: return SignalPhase::GREEN;
        }
        return SignalPhase::ALL_RED; // unreachable, safe fallback
    }

}