#pragma once
#include "../include/model/IntersectionState.h"

class SignalController {
public:
    static void apply(intersection_state& state, int laneId);
};

