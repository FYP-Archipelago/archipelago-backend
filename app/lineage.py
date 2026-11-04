"""Observational ancestry games. No optimiser replays or cluster-graph inference.

Kept independent of the dashboard and transport so a Python worker can reuse it.
"""

import random
from collections import defaultdict, deque
from functools import lru_cache
from math import comb, isfinite, sqrt


def shapley_game(labels, candidates, *, seed=20261103, samples=1024, work_limit=None):
    """Candidates are (required-player bit mask, nonnegative fitness improvement)."""
    n = len(labels)
    if n > 64 or samples < 2:
        raise ValueError("Support up to 64 players and at least two samples.")
    best = {}
    for mask, gain in candidates:
        if mask < 0 or mask >= 1 << n or not isfinite(gain) or gain < 0:
            raise ValueError("Invalid coalition candidate.")
        best[mask] = max(best.get(mask, 0.0), gain)
    exact = n <= 10
    if not exact and work_limit is not None and len(best) * min(samples * n, 1 << n) > work_limit:
        raise ValueError("Estimated permutation-sampling work exceeds the Shapley resource limit.")
    values = [best.get(mask, 0.0) for mask in range(1 << n)] if exact else None
    if exact:
        for i in range(n):
            for mask in range(1 << n):
                if mask & (1 << i):
                    values[mask] = max(values[mask], values[mask ^ (1 << i)])

    @lru_cache(maxsize=100000)
    def value(mask):
        return values[mask] if exact else max((gain for req, gain in best.items() if req & mask == req), default=0.0)

    empty, full = value(0), value((1 << n) - 1)
    credit, squared = [0.0] * n, [0.0] * n
    rng = random.Random(seed)
    examples = []

    def walk(order):
        mask, steps = 0, []
        for i in order:
            before = value(mask)
            mask |= 1 << i
            after = value(mask)
            steps.append(
                {
                    "player_id": str(i),
                    "label": labels[i],
                    "before": before,
                    "after": after,
                    "marginal": after - before,
                    "bar_percent": 100 * (after - before) / (full - empty) if full > empty else 0,
                }
            )
        return steps

    if exact:
        for i in range(n):
            for mask in range(1 << n):
                if not mask & (1 << i):
                    credit[i] += (value(mask | (1 << i)) - value(mask)) / (n * comb(n - 1, mask.bit_count()))
    else:
        for sample in range(samples):
            order = list(range(n))
            rng.shuffle(order)
            steps = walk(order)
            for step in steps:
                i, marginal = int(step["player_id"]), step["marginal"]
                credit[i] += marginal
                squared[i] += marginal * marginal
            if sample < 6:
                examples.append({"id": str(sample), "steps": steps})
        for i in range(n):
            credit[i] /= samples

    if exact and n:
        orders = [tuple(range(n)), tuple(reversed(range(n)))]
        for _ in range(12):
            order = list(range(n))
            rng.shuffle(order)
            if tuple(order) not in orders:
                orders.append(tuple(order))
        examples = [{"id": str(i), "steps": walk(order)} for i, order in enumerate(dict.fromkeys(orders))][:6]

    scale = max(credit, default=0)
    players = []
    for i, label in enumerate(labels):
        stderr = 0.0 if exact else sqrt(max(0, squared[i] - samples * credit[i] ** 2) / (samples - 1) / samples)
        players.append(
            {
                "id": str(i),
                "label": label,
                "value": credit[i],
                "standard_error": stderr,
                "share_percent": 100 * credit[i] / (full - empty) if full > empty else 0,
                "bar_percent": 100 * credit[i] / scale if scale else 0,
            }
        )
    players.sort(key=lambda player: (-player["value"], int(player["id"])))
    return {
        "method": "exact" if exact else "permutation-sampling",
        "evaluated_coalitions": (1 << n) if exact else value.cache_info().misses,
        "permutations": None if exact else samples,
        "seed": seed,
        "players": players,
        "empty_value": empty,
        "full_value": full,
        "allocated_gain": full - empty,
        "efficiency_error": sum(credit) - (full - empty),
        "examples": examples,
        "coalitions": [
            {
                "id": str(mask),
                "players": [str(i) for i in range(n) if mask & (1 << i)],
                "value": value(mask),
                "value_percent": 100 * value(mask) / full if full else 0,
            }
            for mask in range(1 << n)
        ]
        if exact
        else [],
    }


def explain(rows, *, maximising, run_id, work_limit=None):
    """Require all recorded parents; unknown ancestry cannot establish support."""
    if not isinstance(maximising, bool):
        raise ValueError("Run metadata must explicitly specify maximising.")  # noqa: TRY004 - preserve the reviewed core's data-error contract
    if not rows or len(rows) > 100000:
        raise ValueError("Expected between 1 and 100,000 evaluation records.")
    nodes, by_id = {}, defaultdict(list)
    for raw in rows:
        island = int(raw["island_id"])
        identity = str(raw["individual_id"]).strip()
        fitness = float(raw["fitness"])
        if island < 0 or not identity or not isfinite(fitness):
            raise ValueError("Invalid island, individual ID or finite fitness.")
        key = (island, identity)
        # Schema 2 individual IDs are globally unique, including migration copies.
        # Reject ambiguity instead of guessing a same-island parent.
        if identity in by_id:
            raise ValueError(f"Duplicate individual ID: {identity}")
        nodes[key] = {"raw": raw, "fitness": fitness}
        by_id[identity].append(key)
    islands = sorted({key[0] for key in nodes})
    if len(islands) > 64:
        raise ValueError("At most 64 islands are supported.")
    island_bit = {island: 1 << i for i, island in enumerate(islands)}
    routes, founders = set(), []
    children = defaultdict(list)
    direct_invalid, indegree = set(), {}
    for key, node in nodes.items():
        raw = node["raw"]
        parent_ids = raw.get("parent_ids") or ""
        if isinstance(parent_ids, str):
            parent_ids = [x.strip() for x in parent_ids.split(";") if x.strip()]
        origin = raw.get("origin_individual_id")
        arrival = raw.get("operator") == "migration_arrival"
        parents, own_routes = set(), set()
        # A migration copy also depends on the explicit source individual.
        # Other operators (notably PSO) may retain origin fields as provenance.
        # Only an actual arrival introduces a direct source dependency.
        if arrival and origin:
            try:
                source = (int(raw["origin_island"]), str(origin))
            except (ValueError, TypeError, KeyError):
                source = None
            if source not in nodes:
                direct_invalid.add(key)
            else:
                parents.add(source)
        for identity in parent_ids:
            local = (key[0], identity)
            if arrival and origin and identity == origin:
                continue
            resolved = local if local in nodes else by_id[identity][0] if len(by_id[identity]) == 1 else None
            if resolved is None:
                direct_invalid.add(key)
            else:
                parents.add(resolved)
        is_founder = raw.get("operator") == "init" and not parent_ids and not arrival and not origin
        if is_founder:
            founders.append(node["fitness"])
        elif not parents or (arrival and not origin):
            direct_invalid.add(key)
        for parent in parents:
            if parent[0] != key[0]:
                own_routes.add((parent[0], key[0]))
            children[parent].append(key)
        routes.update(own_routes)
        node.update(parents=parents, own_routes=own_routes)
        indegree[key] = len(parents)
    if not founders:
        raise ValueError("No explicit initial-population records; cannot define the baseline.")
    routes = sorted(routes)
    if len(routes) > 64:
        raise ValueError("At most 64 observed migration routes are supported.")
    route_bit = {route: 1 << i for i, route in enumerate(routes)}
    baseline = (max if maximising else min)(founders)
    queue = deque(sorted(key for key in nodes if not indegree[key]))
    supported, island_candidates, route_candidates = {}, [], []
    while queue:
        key = queue.popleft()
        node = nodes[key]
        if key not in direct_invalid and all(parent in supported for parent in node["parents"]):
            island_mask, route_mask = island_bit[key[0]], 0
            for parent in node["parents"]:
                ip, rp = supported[parent]
                island_mask |= ip
                route_mask |= rp
            for route in node["own_routes"]:
                route_mask |= route_bit[route]
            supported[key] = island_mask, route_mask
            gain = max(0.0, (node["fitness"] - baseline) * (1 if maximising else -1))
            island_candidates.append((island_mask, gain))
            route_candidates.append((route_mask, gain))
        for child in children[key]:
            indegree[child] -= 1
            if not indegree[child]:
                queue.append(child)
    best_fn = max if maximising else min
    excluded = len(nodes) - len(supported)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "maximising": maximising,
        "baseline_fitness": baseline,
        "observed_best_fitness": best_fn(node["fitness"] for node in nodes.values()),
        "supported_best_fitness": best_fn(nodes[key]["fitness"] for key in supported),
        "coverage": {
            "total": len(nodes),
            "supported": len(supported),
            "excluded": excluded,
            "percent": 100 * len(supported) / len(nodes),
            "direct_incomplete": len(direct_invalid),
            "cyclic_or_blocked": sum(bool(x) for x in indegree.values()),
        },
        "warnings": ([f"{excluded} evaluations lack fully resolved ancestry and are excluded."] if excluded else []),
        "games": {
            "islands": shapley_game([f"Island {x}" for x in islands], island_candidates, work_limit=work_limit),
            "routes": shapley_game([f"{a} → {b}" for a, b in routes], route_candidates, work_limit=work_limit),
        },
    }
