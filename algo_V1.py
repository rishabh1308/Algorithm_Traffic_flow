import time
import random
from typing import List, Dict, Any

class DynamicTrafficController:
    def __init__(
        self,
        N: int,
        yellow_time: float = 1.0,
        min_green: float = 3.0,
        wait_boost: float = 0.5,
        starvation_limit: int = 8,
        clearance_rate: int = 3,
        debug: bool = True
    ):
        self.lane_ids = [f"Lane_{i+1}" for i in range(N)]
        self.lanes = {
            lane: {"normal": 0, "emergency": 0, "wait": 0, "state": [1, 0, 0]}
            for lane in self.lane_ids
        }
        self.current_green = None
        self.green_started_at = None
        self.last_emergency_lane = None
        self.yellow_time = yellow_time
        self.min_green = min_green
        self.wait_boost = wait_boost
        self.starvation_limit = starvation_limit
        self.clearance_rate = clearance_rate
        self.debug = debug

    def _choose_emergency_lane(self, data: Dict[str, Any]):
        emergencies = [l for l in data if data[l]["emergency"] > 0]
        if not emergencies:
            return None
        max_count = max(data[l]["emergency"] for l in emergencies)
        tied = [l for l in emergencies if data[l]["emergency"] == max_count]
        if len(tied) == 1:
            self.last_emergency_lane = tied[0]
            return tied[0]
        start = 0
        if self.last_emergency_lane in self.lane_ids:
            start = (self.lane_ids.index(self.last_emergency_lane) + 1) % len(self.lane_ids)
        for i in range(len(self.lane_ids)):
            lane = self.lane_ids[(start + i) % len(self.lane_ids)]
            if lane in tied:
                self.last_emergency_lane = lane
                return lane
        return tied[0]

    def _choose_normal_lane(self, data: Dict[str, Any]):
        for lane in self.lane_ids:
            if data[lane]["wait"] >= self.starvation_limit:
                return lane
        scores = {}
        for lane in self.lane_ids:
            n = data[lane]["normal"]
            w = data[lane]["wait"]
            scores[lane] = n * (1 + w * self.wait_boost)
        return max(scores, key=scores.get)

    def _apply_yellow(self, to_green: str):
        if self.current_green == to_green and self.lanes[to_green]["state"] == [0, 0, 1]:
            return
        for lane in self.lane_ids:
            self.lanes[lane]["state"] = [0, 1, 0] if lane == to_green else [1, 0, 0]
        time.sleep(self.yellow_time)
        self.lanes[to_green]["state"] = [0, 0, 1]
        self.current_green = to_green
        self.green_started_at = time.time()

    def _update_waits(self, chosen: str):
        for lane in self.lane_ids:
            if lane == chosen:
                self.lanes[lane]["wait"] = 0
            else:
                self.lanes[lane]["wait"] += 1

    def _simulate_flow(self, data: Dict[str, Any], chosen: str):
        normal = data[chosen]["normal"]
        cleared = min(normal, self.clearance_rate)
        data[chosen]["normal"] -= cleared
        for lane in self.lane_ids:
            if lane != chosen:
                data[lane]["normal"] += random.randint(0, 2)

    def update(self, lanes_data: List[Dict[str, Any]]) -> Dict[str, List[int]]:
        data = {d["lane_id"]: {"normal": d["normal"], "emergency": d["emergency"], "wait": self.lanes[d["lane_id"]]["wait"]} for d in lanes_data}
        chosen_em = self._choose_emergency_lane(data)
        if chosen_em is not None:
            chosen = chosen_em
        else:
            if self.current_green is not None and self.green_started_at is not None:
                elapsed = time.time() - self.green_started_at
                if elapsed < self.min_green:
                    chosen = self.current_green
                else:
                    chosen = self._choose_normal_lane(data)
            else:
                chosen = self._choose_normal_lane(data)
        self._update_waits(chosen)
        self._apply_yellow(chosen)
        self._simulate_flow(data, chosen)
        for lane in self.lane_ids:
            self.lanes[lane]["normal"] = data[lane]["normal"]
        if self.debug:
            print(f"\nActive: {chosen}")
            print({l: self.lanes[l]["state"] for l in self.lane_ids})
        return {l: self.lanes[l]["state"] for l in self.lane_ids}
