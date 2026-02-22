#pragma once

/// Instead of a fixed NORTH/SOUTH/EAST/WEST enum, Direction is now
/// an approach index (0 to N-1) arranged uniformly around the intersection.
/// For a 4-way intersection: 0=NORTH, 1=EAST, 2=SOUTH, 3=WEST (clockwise).
/// For a 6-way: 0°, 60°, 120°, 180°, 240°, 300°, etc.
///
/// Opposing approach: (index + N/2) % N  (only exact for even N).
/// Crossing: any approach that is neither same nor opposing.

#include <cstdint>
#include <string>
#include <cmath>

namespace tip::model {

/// Represents an approach direction at an N-way intersection.
struct Direction {
    uint16_t index = 0;       ///< Approach index [0, numApproaches)
    uint16_t numApproaches = 4; ///< Total number of approaches at the intersection

    Direction() = default;
    Direction(uint16_t idx, uint16_t total) : index(idx), numApproaches(total) {}

    bool operator==(const Direction& o) const noexcept {
        return index == o.index && numApproaches == o.numApproaches;
    }
    bool operator!=(const Direction& o) const noexcept { return !(*this == o); }

    /// Angular position in degrees: index * (360 / numApproaches).
    [[nodiscard]] double angleDeg() const noexcept {
        return static_cast<double>(index) * 360.0 / static_cast<double>(numApproaches);
    }

    /// Angular separation between this and another direction (0..180°).
    [[nodiscard]] double angularSeparation(const Direction& other) const noexcept {
        double diff = std::abs(angleDeg() - other.angleDeg());
        return diff > 180.0 ? 360.0 - diff : diff;
    }

    /// Check if another direction is opposing (180° apart, ±tolerance).
    [[nodiscard]] bool isOpposing(const Direction& other) const noexcept {
        return std::abs(angularSeparation(other) - 180.0) < 1.0;
    }

    /// Check if another direction is the same approach.
    [[nodiscard]] bool isSame(const Direction& other) const noexcept {
        return index == other.index;
    }

    /// Check if another direction is a crossing (not same, not opposing).
    [[nodiscard]] bool isCrossing(const Direction& other) const noexcept {
        return !isSame(other) && !isOpposing(other);
    }

    /// Get the opposing direction (only exact for even N).
    [[nodiscard]] Direction opposing() const noexcept {
        return {static_cast<uint16_t>((index + numApproaches / 2) % numApproaches), numApproaches};
    }
};

/// Human-readable label for a direction.
[[nodiscard]] inline std::string to_string(const Direction& d) {
    // For classic 4-way, use cardinal names
    if (d.numApproaches == 4) {
        switch (d.index) {
            case 0: return "NORTH";
            case 1: return "EAST";
            case 2: return "SOUTH";
            case 3: return "WEST";
            default: break;
        }
    }
    return "A" + std::to_string(d.index) + "/" + std::to_string(d.numApproaches);
}

/// Hash support for use in unordered containers.
struct DirectionHash {
    std::size_t operator()(const Direction& d) const noexcept {
        return std::hash<uint32_t>{}(
            (static_cast<uint32_t>(d.numApproaches) << 16) | d.index);
    }
};

}