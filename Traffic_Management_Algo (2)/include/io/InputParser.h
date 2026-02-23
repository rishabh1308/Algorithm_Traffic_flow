#pragma once
/// JSON schema expected:
/// {
///   "numApproaches": 4,
///   "lanes": [
///     {
///       "approachIndex": 0,
///       "movement": "THROUGH",
///       "normalVehicles": 15,
///       "emergencyVehicles": 0,
///       "path": [[0,0],[10,20],[20,40]]   // optional polyline
///     },
///     ...
///   ],
///   "bleEvents": [
///     {
///       "deviceId": "BUS-001",
///       "approachIndex": 1,
///       "weight": 3.0
///     }
///   ]
/// }
///
/// Output JSON schema (returned after simulation):
/// {
///   "lanes": [
///     {
///       "id": 0,
///       "approach": "NORTH",
///       "movement": "THROUGH",
///       "normalVehicles": 15,
///       "emergencyVehicles": 0,
///       "priorityReason": "NONE"
///     }
///   ],
///   "bleEvents": [
///     { "deviceId": "BUS-001", "approachIndex": 1, "weight": 3.0, "accepted": true }
///   ],
///   "summary": {
///     "totalNormalVehicles": 40,
///     "totalEmergencyVehicles": 1,
///     "totalBLEEvents": 1,
///     "acceptedBLEEvents": 1
///   }
/// }

#include "../model/Lane.hpp"
#include "../model/Direction.hpp"
#include "../model/MovementType.hpp"
#include "../model/PriorityReason.hpp"
#include "../ble/BLEEvent.hpp"

#include <string>
#include <vector>
#include <cstdint>

namespace tip::io {

/// Per-lane input descriptor parsed from JSON.
struct LaneInput {
    uint16_t    approachIndex    = 0;
    std::string movement        = "THROUGH";
    uint32_t    normalVehicles   = 0;
    uint32_t    emergencyVehicles = 0;
    std::vector<model::Point> path;  ///< Optional polyline
};

/// BLE event descriptor parsed from JSON.
struct BLEInput {
    std::string deviceId;
    uint16_t    approachIndex = 0;
    double      weight        = 1.0;
};

/// Parsed input bundle ready for engine consumption.
struct ParsedInput {
    uint16_t               numApproaches = 4;
    std::vector<LaneInput> lanes;
    std::vector<BLEInput>  bleEvents;
};

/// Parses JSON input and produces engine-ready structures.
class InputParser {
public:
    /// Parse a JSON string into a ParsedInput structure.
    /// Throws std::runtime_error on malformed input.
    [[nodiscard]] static ParsedInput parseJSON(const std::string& jsonStr);

    /// Convert ParsedInput into model::Lane vector ready for TrafficEngine.
    [[nodiscard]] static std::vector<model::Lane> toLanes(const ParsedInput& input);

    /// Convert ParsedInput BLE entries into ble::BLEEvent vector.
    [[nodiscard]] static std::vector<ble::BLEEvent> toBLEEvents(const ParsedInput& input);

    /// Serialize the processed result back to JSON string.
    /// Takes the original parsed input plus BLE acceptance results.
    [[nodiscard]] static std::string toJSON(
        const ParsedInput& input,
        const std::vector<model::Lane>& lanes,
        const std::vector<bool>& bleAccepted);
};

} // namespace tip::io
