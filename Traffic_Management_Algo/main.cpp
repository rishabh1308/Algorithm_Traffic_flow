
/// Demonstrates:
///   1. N-lane N-way intersection setup (4-way, 6-way, 3-way)
///   2. Adaptive switching under normal traffic
///   3. Emergency vehicle override
///   4. BLE transit priority
///   5. Multi-intersection corridor coordination
///   6. RL parameter tuning

#include "model/Direction.hpp"
#include "model/MovementType.hpp"
#include "model/Lane.hpp"
#include "model/PriorityReason.hpp"
#include "engine/EngineConfig.hpp"
#include "engine/TrafficEngine.hpp"
#include "ble/BLEEvent.hpp"
#include "ble/BLERegistry.hpp"
#include "ble/BLEPriorityManager.hpp"
#include "coordination/CorridorCoordinator.hpp"
#include "rl/RLAgent.hpp"

#include <iostream>
#include <iomanip>
#include <memory>
#include <vector>

using namespace tip;


// Each approach gets one THROUGH + one LEFT_PROTECTED lane = 2*N lanes total
static std::vector<model::Lane> createNWayIntersection(uint16_t numApproaches) {
    std::vector<model::Lane> lanes;
    std::size_t id = 0;

    for (uint16_t a = 0; a < numApproaches; ++a) {
        model::Direction dir(a, numApproaches);
        lanes.push_back({id++, dir, model::MovementType::THROUGH,        0, 0, 0.0, model::PriorityReason::NONE});
        lanes.push_back({id++, dir, model::MovementType::LEFT_PROTECTED, 0, 0, 0.0, model::PriorityReason::NONE});
    }

    return lanes;
}
static void printDecision(const std::string& label, const model::Decision& d) {
    std::cout << "[" << std::setw(22) << label << "] " << d.summary() << "\n";
}

static void setQueues(std::vector<model::Lane>& lanes,
                      const std::vector<uint32_t>& queues) {
    for (std::size_t i = 0; i < lanes.size() && i < queues.size(); ++i) {
        lanes[i].queueLength = queues[i];
    }
}

int main() {
    std::cout << "\n";
    std::cout << "   Traffic Intelligence Platform — N-Way Demo\n";
    std::cout << "\n\n";

    engine::EngineConfig config;
    config.alpha = 1.0;
    config.beta  = 2.0;
    config.minGreen = 8;
    config.maxGreen = 45;


    std::cout << "Demo 1: 4-Way Intersection (8 lanes) ";
    {
        auto lanes = createNWayIntersection(4);
        auto engine1 = std::make_shared<engine::TrafficEngine>(lanes, config);

        std::cout << "✓ Created " << engine1->lanes().size() << "-lane, 4-way intersection with "
                  << engine1->phases().size() << " phases:\n";
        for (const auto& p : engine1->phases()) {
            std::cout << "  • " << p.name << " (lanes:";
            for (auto idx : p.laneIndices) std::cout << " " << idx;
            std::cout << ")\n";
        }

        setQueues(engine1->lanes(), {15, 3, 4, 1, 12, 2, 5, 1});

        for (int step = 0; step < 15; ++step) {
            auto d = engine1->step();
            printDecision("4-way step " + std::to_string(step), d);
        }
    }
    std::cout << "\n";


    std::cout << "Demo 2: 6-Way Intersection (12 lanes)\n";
    {
        auto lanes = createNWayIntersection(6);
        auto engine6 = std::make_shared<engine::TrafficEngine>(lanes, config);

        std::cout << "✓ Created " << engine6->lanes().size() << "-lane, 6-way intersection with "
                  << engine6->phases().size() << " phases:\n";
        for (const auto& p : engine6->phases()) {
            std::cout << "  • " << p.name << " (lanes:";
            for (auto idx : p.laneIndices) std::cout << " " << idx;
            std::cout << ")\n";
        }

        setQueues(engine6->lanes(), {10, 2, 8, 3, 6, 1, 12, 4, 5, 2, 7, 1});

        for (int step = 0; step < 15; ++step) {
            auto d = engine6->step();
            printDecision("6-way step " + std::to_string(step), d);
        }
    }
    std::cout << "\n";

    std::cout << "Demo 3: 3-Way T-Intersection (6 lanes)\n";
    {
        auto lanes = createNWayIntersection(3);
        auto engine3 = std::make_shared<engine::TrafficEngine>(lanes, config);

        std::cout << "✓ Created " << engine3->lanes().size() << "-lane, 3-way intersection with "
                  << engine3->phases().size() << " phases:\n";
        for (const auto& p : engine3->phases()) {
            std::cout << "  • " << p.name << " (lanes:";
            for (auto idx : p.laneIndices) std::cout << " " << idx;
            std::cout << ")\n";
        }

        setQueues(engine3->lanes(), {8, 3, 5, 2, 10, 1});

        for (int step = 0; step < 10; ++step) {
            auto d = engine3->step();
            printDecision("3-way step " + std::to_string(step), d);
        }
    }
    std::cout << "\n";

    std::cout << "Demo 4: Emergency Vehicle Override \n";
    {
        auto lanes = createNWayIntersection(4);
        setQueues(lanes, {15, 3, 4, 1, 12, 2, 5, 1});
        auto engine1 = std::make_shared<engine::TrafficEngine>(lanes, config);

        // Set emergency on approach 1 THROUGH lane (index 2)
        engine1->lanes()[2].priorityReason = model::PriorityReason::EMERGENCY;
        engine1->lanes()[2].queueLength = 1;

        for (int step = 0; step < 10; ++step) {
            auto d = engine1->step();
            printDecision("Emergency " + std::to_string(step), d);
        }
        engine1->lanes()[2].priorityReason = model::PriorityReason::NONE;
    }
    std::cout << "\n";

    std::cout << "Demo 5: BLE Transit Priority \n";
    {
        auto lanes = createNWayIntersection(4);
        setQueues(lanes, {15, 3, 4, 1, 12, 2, 5, 1});
        auto engine1 = std::make_shared<engine::TrafficEngine>(lanes, config);

        ble::BLERegistry registry;
        registry.authorize("BUS-001");

        ble::BLEConfig bleConfig;
        bleConfig.cooldownWindow = std::chrono::seconds(30);

        ble::BLEPriorityManager bleMgr(bleConfig, registry);

        // Bus approaching from approach 1 (EAST in 4-way)
        ble::BLEEvent busEvent;
        busEvent.deviceId = "BUS-001";
        busEvent.direction = model::Direction(1, 4);
        busEvent.weight = 3.0;

        bool accepted = bleMgr.processEvent(busEvent);
        std::cout << "BLE event from BUS-001 (approach 1): "
                  << (accepted ? "ACCEPTED" : "REJECTED") << "\n";

        for (auto& lane : engine1->lanes()) {
            if (lane.direction.index == 1) {
                lane.bleBoost = bleMgr.getBoost(model::Direction(1, 4));
                lane.priorityReason = model::PriorityReason::BLE;
            }
        }

        for (int step = 0; step < 10; ++step) {
            auto d = engine1->step();
            printDecision("BLE " + std::to_string(step), d);
        }
    }
    std::cout << "\n";

    std::cout << "Demo 6: Corridor Coordination (two 4-way) \n";
    {
        auto lanes1 = createNWayIntersection(4);
        setQueues(lanes1, {15, 3, 4, 1, 12, 2, 5, 1});
        auto engine1 = std::make_shared<engine::TrafficEngine>(lanes1, config);

        auto lanes2 = createNWayIntersection(4);
        setQueues(lanes2, {10, 2, 8, 3, 6, 2, 7, 1});
        auto engine2 = std::make_shared<engine::TrafficEngine>(lanes2, config);

        coordination::CorridorCoordinator corridor;
        corridor.addIntersection(engine1, 0);
        corridor.addIntersection(engine2, 5);

        for (uint32_t t = 0; t < 12; ++t) {
            corridor.tick(t);
            const auto& decisions = corridor.lastDecisions();
            std::cout << "  t=" << std::setw(2) << t << " | ";
            for (std::size_t i = 0; i < decisions.size(); ++i) {
                std::cout << "I" << i << ":" << decisions[i].phaseName
                          << "(" << model::to_string(decisions[i].signalState) << ") ";
            }
            std::cout << "\n";
        }
    }
    std::cout << "\n";


    std::cout << "Demo 7: RL Parameter Tuning \n";
    {
        auto lanes = createNWayIntersection(4);
        setQueues(lanes, {15, 3, 4, 1, 12, 2, 5, 1});
        auto engine1 = std::make_shared<engine::TrafficEngine>(lanes, config);

        rl::RLAgent agent;
        std::cout << "  Before: alpha=" << engine1->config().alpha
                  << " beta=" << engine1->config().beta
                  << " greenPerVeh=" << engine1->config().greenPerVehicle << "\n";

        agent.tune(*engine1);

        std::cout << "  After:  alpha=" << engine1->config().alpha
                  << " beta=" << engine1->config().beta
                  << " greenPerVeh=" << engine1->config().greenPerVehicle << "\n";
    }

    std::cout << "\n\n";
    std::cout << "   Demo complete. N-way intersections supported.\n";
    std::cout << "\n";

    return 0;
}
