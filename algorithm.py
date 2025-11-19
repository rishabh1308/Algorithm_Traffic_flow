import time
import random
from typing import List, Dict, Any

class DynamicTrafficController:
    """
    Dynamic traffic signal controller that decides which lane to open
    based on:
      - Number of emergency and normal vehicles
      - Lane waiting time (aging/fairness)
      - Dynamic green time proportional to traffic volume
    """

    def __init__(
        self,
        N: int,
        yellow_time: float = 2.0,
        min_green: float = 3.0,
        max_green: float = 15.0,
        wait_boost: float = 0.4,
        starvation_limit: int = 8,
        clearance_rate: float = 3.0,
        debug: bool = True
    ):
        # Initialize lane data
        self.lane_ids = [f"Lane_{i+1}" for i in range(N)]
        self.lanes = {
            lane: {"normal": 0, "emergency": 0, "wait": 0, "state": [1, 0, 0]}
            for lane in self.lane_ids
        }

        # State tracking
        self.current_green = None
        self.green_started_at = None
        self.current_green_time = min_green
        self.last_emergency_lane = None

        # Parameters
        self.yellow_time = yellow_time
        self.min_green = min_green
        self.max_green = max_green
        self.wait_boost = wait_boost
        self.starvation_limit = starvation_limit
        self.clearance_rate = clearance_rate
        self.debug = debug

    # Emergency lane chooser (highest priority)
    def _choose_emergency_lane(self, data: Dict[str, Any]):
        emergencies = [l for l in data if data[l]["emergency"] > 0]
        if not emergencies:
            return None

        # Select lane with highest emergency count
        max_count = max(data[l]["emergency"] for l in emergencies)
        tied = [l for l in emergencies if data[l]["emergency"] == max_count]

        # Round-robin tie breaker
        if len(tied) == 1:
            chosen = tied[0]
        else:
            start = 0
            if self.last_emergency_lane in self.lane_ids:
                start = (self.lane_ids.index(self.last_emergency_lane) + 1) % len(self.lane_ids)
            for i in range(len(self.lane_ids)):
                lane = self.lane_ids[(start + i) % len(self.lane_ids)]
                if lane in tied:
                    chosen = lane
                    break
            else:
                chosen = tied[0]
        self.last_emergency_lane = chosen
        return chosen

    #   Normal lane chooser (based on vehicles + fairness)
    def _choose_normal_lane(self, data: Dict[str, Any]):
        scores = {}
        for lane in self.lane_ids:
            n = data[lane]["normal"]
            w = data[lane]["wait"]

            # Base score proportional to number of vehicles and wait boost
            score = n * (1 + w * self.wait_boost)

            # Starvation prevention: if waited too long, give a large boost
            if w >= self.starvation_limit:
                score += 1000

            scores[lane] = score

        # Choose lane with highest effective score
        chosen = max(scores, key=scores.get)
        return chosen

    #  Dynamic green time calculator
    def _calculate_green_time(self, lane_data):
        """
        Green time proportional to vehicle count, capped between min & max.
        Emergency vehicles add extra time.
        """
        base_time = lane_data["normal"] * 0.8 + lane_data["emergency"] * 2.0
        green_time = max(self.min_green, min(base_time, self.max_green))
        return green_time

    #  Transition phase (yellow to green)
    def _apply_yellow(self, to_green: str):
        """
        Simulates yellow transition instantly (non-blocking).
        Updates lane states accordingly.
        """
        for lane in self.lane_ids:
            if lane == to_green:
                self.lanes[lane]["state"] = [0, 1, 0]  # yellow
            else:
                self.lanes[lane]["state"] = [1, 0, 0]  # red

        # Transition to green immediately (no sleep)
        self.lanes[to_green]["state"] = [0, 0, 1]
        self.current_green = to_green
        self.green_started_at = time.time()

    # Update waiting counters (aging)
    def _update_waits(self, chosen: str):
        for lane in self.lane_ids:
            if lane == chosen:
                self.lanes[lane]["wait"] = 0
            else:
                self.lanes[lane]["wait"] += 1

    #  Simulate traffic flow
    def _simulate_flow(self, data: Dict[str, Any], chosen: str, green_time: float):
        """
        Clears vehicles from chosen lane proportional to green time.
        Adds random arrivals to others.
        """
        normal = data[chosen]["normal"]
        cleared = min(normal, int(self.clearance_rate * green_time))
        data[chosen]["normal"] -= cleared

        for lane in self.lane_ids:
            if lane != chosen:
                # Random new arrivals
                data[lane]["normal"] += random.randint(0, 3)

    #   Main update function
    def update(self, lanes_data: List[Dict[str, Any]]) -> Dict[str, List[int]]:
        """
        Update controller state given current sensor data.
        Returns lane signal states as {lane: [R,Y,G]}.
        """

        # Prepare data dict for easy access
        data = {
            d["lane_id"]: {
                "normal": d["normal"],
                "emergency": d["emergency"],
                "wait": self.lanes[d["lane_id"]]["wait"]
            }
            for d in lanes_data
        }

        # Step 1: Check for emergencies first
        chosen = self._choose_emergency_lane(data)

        # Step 2: If no emergency, handle normal lanes
        if not chosen:
            if self.current_green and self.green_started_at:
                elapsed = time.time() - self.green_started_at
                # Keep current green until its time expires
                if elapsed < self.current_green_time:
                    chosen = self.current_green
                else:
                    chosen = self._choose_normal_lane(data)
            else:
                chosen = self._choose_normal_lane(data)

        # Step 3: Fairness update
        self._update_waits(chosen)

        # Step 4: Dynamic green time calculation
        self.current_green_time = self._calculate_green_time(data[chosen])

        # Step 5: Transition + activate chosen lane
        self._apply_yellow(chosen)

        # Step 6: Simulate flow (traffic clearing)
        self._simulate_flow(data, chosen, self.current_green_time)

        # Step 7: Update internal lane queues
        for lane in self.lane_ids:
            self.lanes[lane]["normal"] = data[lane]["normal"]

        # Step 8: Debug output
        if self.debug:
            print("\n==============================")
            print(f" Active Green: {chosen}")
            print(f" Green Time  : {self.current_green_time:.1f}s")
            print(f" Wait Times  : {[self.lanes[l]['wait'] for l in self.lane_ids]}")
            print(" Lane States :", {l: self.lanes[l]["state"] for l in self.lane_ids})
            print("==============================\n")

        # Return signal states
        return {l: self.lanes[l]["state"] for l in self.lane_ids}



# Create a controller for 4 lanes
controller = DynamicTrafficController(
    N=4,
    min_green=3.0,
    max_green=12.0,
    yellow_time=2.0,
    clearance_rate=2.5,
    debug=True
)

# Initialize random vehicle counts
lanes_data = [
    {"lane_id": "Lane_1", "normal": 5, "emergency": 0},
    {"lane_id": "Lane_2", "normal": 3, "emergency": 0},
    {"lane_id": "Lane_3", "normal": 6, "emergency": 0},
    {"lane_id": "Lane_4", "normal": 4, "emergency": 0},
]

# Run simulation for 10 update cycles
for cycle in range(10):
    print(f"\n======= Cycle {cycle + 1} =======")

    # Randomly inject some new vehicles (and occasionally an emergency)
    for lane in lanes_data:
        lane["normal"] += random.randint(0, 3)
        # 10% chance of an emergency vehicle appearing
        lane["emergency"] = 1 if random.random() < 0.1 else 0

    # Feed current sensor data to controller
    controller.update(lanes_data)

    # Small pause just to separate console outputs
    time.sleep(1)
