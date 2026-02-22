
#include "engine/PhaseBuilder.hpp"
#include <stdexcept>
#include <set>
#include <algorithm>

namespace tip::engine {

std::vector<model::Phase> PhaseBuilder::build(
    const std::vector<model::Lane>& lanes,
    const model::ConflictMatrix& conflicts)
{
    if (lanes.empty()) {
        throw std::runtime_error("PhaseBuilder: No lanes provided");
    }

    // Determine the number of approaches from the lanes
    uint16_t numApproaches = 0;
    for (const auto& lane : lanes) {
        numApproaches = std::max(numApproaches,
            static_cast<uint16_t>(lane.direction.index + 1));
    }
    // Also check numApproaches field
    for (const auto& lane : lanes) {
        numApproaches = std::max(numApproaches, lane.direction.numApproaches);
    }

    std::vector<model::Phase> phases;
    std::set<uint16_t> processedApproaches;

    // For even N: pair opposing approaches (i with i + N/2)
    // For odd N: each approach is its own group
    bool hasOpposing = (numApproaches % 2 == 0) && (numApproaches >= 2);

    for (uint16_t i = 0; i < numApproaches; ++i) {
        if (processedApproaches.count(i)) continue;

        std::vector<uint16_t> groupApproaches;
        groupApproaches.push_back(i);

        if (hasOpposing) {
            uint16_t opp = (i + numApproaches / 2) % numApproaches;
            if (opp != i) {
                groupApproaches.push_back(opp);
                processedApproaches.insert(opp);
            }
        }
        processedApproaches.insert(i);

        // Build a readable name for this group
        std::string groupName;
        if (numApproaches == 4) {
            // Classic names
            const char* names[] = {"N", "E", "S", "W"};
            for (auto idx : groupApproaches) {
                if (!groupName.empty()) groupName += "";
                if (idx < 4) groupName += names[idx];
                else groupName += "A" + std::to_string(idx);
            }
        } else {
            for (auto idx : groupApproaches) {
                if (!groupName.empty()) groupName += "-";
                groupName += "A" + std::to_string(idx);
            }
        }

        // Through phase
        auto throughLanes = collectLanes(lanes, groupApproaches, model::MovementType::THROUGH);
        if (!throughLanes.empty()) {
            model::Phase p(groupName + "-through", std::move(throughLanes));
            validatePhase(p, conflicts);
            phases.push_back(std::move(p));
        }

        // Left-protected phase
        auto leftLanes = collectLanes(lanes, groupApproaches, model::MovementType::LEFT_PROTECTED);
        if (!leftLanes.empty()) {
            model::Phase p(groupName + "-left", std::move(leftLanes));
            validatePhase(p, conflicts);
            phases.push_back(std::move(p));
        }
    }

    if (phases.empty()) {
        throw std::runtime_error("PhaseBuilder: No valid phases could be constructed from lanes");
    }

    return phases;
}

std::vector<std::size_t> PhaseBuilder::collectLanes(
    const std::vector<model::Lane>& lanes,
    const std::vector<uint16_t>& approachIndices,
    model::MovementType movement)
{
    std::vector<std::size_t> indices;
    for (std::size_t i = 0; i < lanes.size(); ++i) {
        if (lanes[i].movement == movement) {
            for (auto ai : approachIndices) {
                if (lanes[i].direction.index == ai) {
                    indices.push_back(i);
                    break;
                }
            }
        }
    }
    return indices;
}

void PhaseBuilder::validatePhase(
    const model::Phase& phase,
    const model::ConflictMatrix& conflicts)
{
    const auto& idx = phase.laneIndices;
    for (std::size_t i = 0; i < idx.size(); ++i) {
        for (std::size_t j = i + 1; j < idx.size(); ++j) {
            if (conflicts.conflicts(idx[i], idx[j])) {
                throw std::runtime_error(
                    "PhaseBuilder: Internal conflict in phase '" + phase.name +
                    "' between lane " + std::to_string(idx[i]) +
                    " and lane " + std::to_string(idx[j]));
            }
        }
    }
}

}