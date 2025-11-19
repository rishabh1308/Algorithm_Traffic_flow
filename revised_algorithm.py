import time
import random
from typing import Dict, Any, List, Tuple

class DynamicMultiLaneTrafficController:
    def __init__(
        self,
        min_green: float = 3.0,
        max_green: float = 15.0,
        clearance_rate: float = 3.0,
        exit_capacity_default: int = 20,
        wait_boost: float = 0.4,
        starvation_limit: int = 8,
        ambulance_safety_margin: float = 1.5,
        reaction_margin: float = 0.5,
        debug: bool = False
    ):
        self.movements = ["straight", "left", "right"]
        self.lanes: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.exit_capacity: Dict[str, int] = {}
        self.turn_map: Dict[Tuple[str, str], str] = {}
        self.conflicts: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        self.min_green = min_green
        self.max_green = max_green
        self.clearance_rate = clearance_rate
        self.exit_capacity_default = exit_capacity_default
        self.wait_boost = wait_boost
        self.starvation_limit = starvation_limit
        self.ambulance_safety_margin = ambulance_safety_margin
        self.reaction_margin = reaction_margin
        self.ambulances: List[Dict[str, Any]] = []
        self.planned_ambulance_jobs: List[Dict[str, Any]] = []
        self.current_green: Any = None
        self.green_started_at: float = 0.0
        self.current_green_time: float = min_green
        self.debug = debug

    def ensure_lane(self, lane_id: str):
        if lane_id not in self.lanes:
            self.lanes[lane_id] = {
                m: {"normal": 0, "emergency": 0, "wait": 0, "state": [1, 0, 0]}
                for m in self.movements
            }
            self.exit_capacity[lane_id] = self.exit_capacity_default

    def rebuild_turn_map(self):
        lane_ids = list(self.lanes.keys())
        n = len(lane_ids)
        for i, lane in enumerate(lane_ids):
            opposite = lane_ids[(i + n // 2) % n] if n > 1 else lane
            left = lane_ids[(i - 1) % n] if n > 1 else lane
            right = lane_ids[(i + 1) % n] if n > 1 else lane
            self.turn_map[(lane, "straight")] = opposite
            self.turn_map[(lane, "left")] = left
            self.turn_map[(lane, "right")] = right

    def rebuild_conflicts(self):
        self.conflicts = {}
        lane_ids = list(self.lanes.keys())
        for lane in lane_ids:
            for mov in self.movements:
                key = (lane, mov)
                self.conflicts[key] = []
                for mov2 in self.movements:
                    if mov2 != mov:
                        self.conflicts[key].append((lane, mov2))
                dest = self.turn_map.get(key)
                if dest:
                    for lane2 in lane_ids:
                        for mov2 in self.movements:
                            if (lane2, mov2) != key and self.turn_map.get((lane2, mov2)) == dest:
                                self.conflicts[key].append((lane2, mov2))

    def register_ambulance(self, amb_id: str, lane: str, mov: str, eta_seconds: float):
        now = time.time()
        self.ambulances = [a for a in self.ambulances if a.get("id") != amb_id]
        self.ambulances.append({
            "id": amb_id,
            "lane": lane,
            "mov": mov,
            "eta": now + eta_seconds,
            "detected_at": now
        })
        if self.debug:
            print("AMB REGISTERED", amb_id, lane, mov, "ETA", eta_seconds)

    def exit_blocked(self, lane: str, margin: int = 2) -> bool:
        Q = sum(self.lanes[lane][m]["normal"] + self.lanes[lane][m]["emergency"] for m in self.movements)
        return Q >= self.exit_capacity[lane] - margin

    def movements_compatible(self, m1: Tuple[str, str], m2: Tuple[str, str]) -> bool:
        if m1 == m2:
            return False
        if m2 in self.conflicts.get(m1, []):
            return False
        if m1 in self.conflicts.get(m2, []):
            return False
        dest1 = self.turn_map.get(m1)
        dest2 = self.turn_map.get(m2)
        if dest1 is None or dest2 is None:
            return False
        if self.exit_blocked(dest1) or self.exit_blocked(dest2):
            return False
        return True

    def compute_preclear_time(self, dest_lane: str) -> float:
        Q = sum(self.lanes[dest_lane][m]["normal"] + self.lanes[dest_lane][m]["emergency"] for m in self.movements)
        base = Q / max(1e-6, self.clearance_rate)
        G_req = max(self.min_green, min(self.max_green, base + self.ambulance_safety_margin))
        return G_req

    def plan_ambulances(self):
        now = time.time()
        jobs = []
        for amb in self.ambulances:
            lane = amb["lane"]
            mov = amb["mov"]
            t_a = amb["eta"]
            dest = self.turn_map.get((lane, mov))
            if dest is None:
                continue
            G_req = self.compute_preclear_time(dest)
            t_s = t_a - G_req
            jobs.append({"amb": amb, "mov": (lane, mov), "dest": dest, "t_a": t_a, "t_s": t_s, "G": G_req})
        jobs.sort(key=lambda j: j["t_a"])
        scheduled = []
        for job in jobs:
            if job["t_s"] <= now + self.reaction_margin:
                job["start"] = now
                scheduled.append(job)
                continue
            conflict = False
            for sj in scheduled:
                sj_end = sj["start"] + sj["G"]
                job_end = job["t_s"] + job["G"]
                if not (job_end <= sj["start"] or sj_end <= job["t_s"]):
                    if not self.movements_compatible(job["mov"], sj["mov"]):
                        conflict = True
                        break
            if not conflict:
                job["start"] = job["t_s"]
                scheduled.append(job)
            else:
                job["start"] = now
                scheduled.append(job)
        self.planned_ambulance_jobs = scheduled
        return scheduled

    def apply_ambulance_policy(self) -> bool:
        now = time.time()
        jobs = getattr(self, "planned_ambulance_jobs", []) or []
        running = [j for j in jobs if j["start"] <= now <= j["start"] + j["G"]]
        if not running:
            imminents = [j for j in jobs if j["start"] <= now + self.reaction_margin]
            if imminents:
                running = imminents
        if not running:
            return False
        running.sort(key=lambda j: j["t_a"])
        greens: List[Tuple[str, str]] = []
        for j in running:
            mv = j["mov"]
            if all(self.movements_compatible(mv, g) for g in greens):
                greens.append(mv)
        if not greens:
            greens = [running[0]["mov"]]
        for L in self.lanes:
            for M in self.movements:
                self.lanes[L][M]["state"] = [1, 0, 0]
        for (lane, mov) in greens:
            self.lanes[lane][mov]["state"] = [0, 0, 1]
        self.current_green = ("ambulance_multi", greens)
        self.green_started_at = now
        selected_jobs = [j for j in running if j["mov"] in greens]
        self.current_green_time = max((j["G"] for j in selected_jobs), default=self.min_green)
        if self.debug:
            print("AMB POLICY APPLIED", greens, "for", f"{self.current_green_time:.1f}s")
        return True

    def choose_emergency_movement(self, flat: Dict[Tuple[str, str], Dict[str, int]]):
        emergencies = [k for k, v in flat.items() if v["emergency"] > 0]
        if not emergencies:
            return None
        max_count = max(flat[k]["emergency"] for k in emergencies)
        tied = [k for k in emergencies if flat[k]["emergency"] == max_count]
        if len(tied) == 1:
            return tied[0]
        start = 0
        lane_order = list(self.lanes.keys())
        last = getattr(self, "last_emergency_lane", None)
        if last in lane_order:
            start = (lane_order.index(last) + 1) % len(lane_order)
        chosen = None
        for i in range(len(lane_order)):
            lane = lane_order[(start + i) % len(lane_order)]
            for mov in self.movements:
                if (lane, mov) in tied:
                    chosen = (lane, mov)
                    break
            if chosen:
                break
        if chosen is None:
            chosen = tied[0]
        self.last_emergency_lane = chosen[0]
        return chosen

    def choose_normal_movement(self, flat: Dict[Tuple[str, str], Dict[str, int]]):
        scores: Dict[Tuple[str, str], float] = {}
        for lane in self.lanes:
            for mov in self.movements:
                n = flat.get((lane, mov), {}).get("normal", 0)
                w = flat.get((lane, mov), {}).get("wait", 0)
                if self.exit_blocked(self.turn_map.get((lane, mov), lane)):
                    scores[(lane, mov)] = -1e9
                    continue
                score = n * (1 + w * self.wait_boost)
                if w >= self.starvation_limit:
                    score += 10000
                scores[(lane, mov)] = score
        chosen = max(scores, key=scores.get)
        return chosen

    def calculate_green_for_movement(self, lane: str, mov: str) -> float:
        lane_data = self.lanes[lane][mov]
        base_time = lane_data["normal"] * 0.8 + lane_data["emergency"] * 2.0
        return max(self.min_green, min(base_time, self.max_green))

    def simulate_flow(self, data: Dict[Tuple[str, str], Dict[str, int]], active_movements: List[Tuple[str, str]], green_time: float):
        for (lane, mov) in active_movements:
            queue = data[(lane, mov)]["normal"]
            cleared = min(queue, int(self.clearance_rate * green_time))
            data[(lane, mov)]["normal"] = max(0, data[(lane, mov)]["normal"] - cleared)
            dest = self.turn_map.get((lane, mov))
            if dest:
                dest_Q = sum(data[(dest, m)]["normal"] + data[(dest, m)]["emergency"] for m in self.movements)
                space = max(0, self.exit_capacity[dest] - dest_Q)
                pushed = min(cleared, space)
                data[(dest, "straight")]["normal"] += pushed
        for lane in self.lanes:
            for mov in self.movements:
                if (lane, mov) not in active_movements:
                    data[(lane, mov)]["normal"] += random.randint(0, 3)

    def update_waits(self, chosen: List[Tuple[str, str]]):
        chosen_set = set(chosen)
        for lane in self.lanes:
            for mov in self.movements:
                if (lane, mov) in chosen_set:
                    self.lanes[lane][mov]["wait"] = 0
                else:
                    self.lanes[lane][mov]["wait"] += 1

    def update(self, camera_input: List[Dict[str, Any]]):
        for entry in camera_input:
            self.ensure_lane(entry["lane_id"])
        self.rebuild_turn_map()
        self.rebuild_conflicts()
        flat: Dict[Tuple[str, str], Dict[str, int]] = {}
        for entry in camera_input:
            lane = entry["lane_id"]
            for mov in self.movements:
                self.lanes[lane][mov]["normal"] = int(entry.get("movements", {}).get(mov, self.lanes[lane][mov]["normal"]))
                self.lanes[lane][mov]["emergency"] = int(entry.get("emergency", {}).get(mov, 0))
                flat[(lane, mov)] = {"normal": self.lanes[lane][mov]["normal"], "emergency": self.lanes[lane][mov]["emergency"], "wait": self.lanes[lane][mov]["wait"]}
        self.plan_ambulances()
        if self.apply_ambulance_policy():
            kind, greens = self.current_green
            chosen_movs = greens
            self.simulate_flow(flat, chosen_movs, self.current_green_time)
            self.update_waits(chosen_movs)
            for (lane, mov) in flat.keys():
                self.lanes[lane][mov]["normal"] = flat[(lane, mov)]["normal"]
            if self.debug:
                print("AMBULANCE CYCLE COMPLETED")
            return {l: {m: self.lanes[l][m]["state"] for m in self.movements} for l in self.lanes}
        chosen_em = self.choose_emergency_movement(flat)
        if chosen_em:
            lane, mov = chosen_em
            G = self.calculate_green_for_movement(lane, mov)
            for L in self.lanes:
                for M in self.movements:
                    self.lanes[L][M]["state"] = [1, 0, 0]
            self.lanes[lane][mov]["state"] = [0, 0, 1]
            self.current_green = ("emergency", [chosen_em])
            self.current_green_time = G
            self.green_started_at = time.time()
            self.simulate_flow(flat, [chosen_em], self.current_green_time)
            self.update_waits([chosen_em])
            for (lane, mov) in flat.keys():
                self.lanes[lane][mov]["normal"] = flat[(lane, mov)]["normal"]
            if self.debug:
                print("EMERGENCY MOVEMENT APPLIED", chosen_em)
            return {l: {m: self.lanes[l][m]["state"] for m in self.movements} for l in self.lanes}
        chosen_norm = self.choose_normal_movement(flat)
        candidates = sorted(list(flat.keys()), key=lambda kv: - (flat[kv]["normal"] * (1 + flat[kv]["wait"] * self.wait_boost)))
        chosen_set = [chosen_norm]
        for lane_mov in candidates:
            if lane_mov == chosen_norm:
                continue
            if all(self.movements_compatible(lane_mov, c) for c in chosen_set):
                chosen_set.append(lane_mov)
            if len(chosen_set) >= max(1, len(self.lanes) // 2):
                break
        Gs = [self.calculate_green_for_movement(lane, mov) for (lane, mov) in chosen_set]
        G = max(Gs) if Gs else self.min_green
        for L in self.lanes:
            for M in self.movements:
                self.lanes[L][M]["state"] = [1, 0, 0]
        for (lane, mov) in chosen_set:
            self.lanes[lane][mov]["state"] = [0, 0, 1]
        self.current_green = ("normal_multi", chosen_set)
        self.current_green_time = G
        self.green_started_at = time.time()
        self.simulate_flow(flat, chosen_set, self.current_green_time)
        self.update_waits(chosen_set)
        for (lane, mov) in flat.keys():
            self.lanes[lane][mov]["normal"] = flat[(lane, mov)]["normal"]
        if self.debug:
            print("NORMAL CYCLE APPLIED", chosen_set, f"for {G:.1f}s")
        return {l: {m: self.lanes[l][m]["state"] for m in self.movements} for l in self.lanes}