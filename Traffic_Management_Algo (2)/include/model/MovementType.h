#pragma once
#include <cstdint>
#include <string>
#include <stdexcept>

namespace tip::model {

    /// Movement types that a lane can serve.
    enum class MovementType : uint8_t {
        THROUGH        = 0,  ///< Straight-through movement
        LEFT_PROTECTED = 1   ///< Protected left turn
    };

    inline constexpr std::size_t NUM_MOVEMENT_TYPES = 2;

    [[nodiscard]] inline std::string to_string(MovementType m) {
        switch (m) {
            case MovementType::THROUGH:        return "THROUGH";
            case MovementType::LEFT_PROTECTED: return "LEFT_PROTECTED";
        }
        throw std::invalid_argument("Unknown MovementType");
    }

}