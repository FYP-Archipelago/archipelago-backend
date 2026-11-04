import json
import math
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.lineage import explain, shapley_game
from app.shapley import ShapleyAccumulator


def row(identity, island, fitness, parents=None, operator="trial", **extra):
    return dict(individual_id=identity, island_id=island, fitness=fitness, parent_ids=parents or [], operator=operator, **extra)


def metadata(maximising=False):
    return SimpleNamespace(maximising=maximising, algorithm="ga", benchmark="sphere", migration_topology="ring")


class AnalysisTests(unittest.TestCase):
    def test_raw_list_parent_ids_produce_complementary_credit_without_genomes(self):
        rows = [
            row("a", 0, 100, operator="init", genome_repr=[123]),
            row("b", 1, 100, operator="init"),
            row("m", 1, 80, ["a"], "migration_arrival", origin_island=0, origin_individual_id="a"),
            row("w", 1, 10, ["b", "m"]),
        ]
        accumulator = ShapleyAccumulator("run-test")
        accumulator.add(rows)
        result = accumulator.finish(metadata())
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["run_id"], "run-test")
        explanation = result["explanation"]
        self.assertEqual([p["value"] for p in explanation["games"]["islands"]["players"]], [45, 45])
        self.assertEqual(explanation["source"]["kind"], "backend-lineage")
        self.assertEqual(explanation["source"]["algorithm"], "ga")
        self.assertNotIn("genome", json.dumps(vars(accumulator)))
        self.assertNotIn("genome", json.dumps(result))

    def test_chunking_and_original_input_mutation_do_not_change_lineage(self):
        initial = row("a", 0, 100, operator="init")
        child = row("b", 0, 25, ["a"])
        accumulator = ShapleyAccumulator("run-test")
        accumulator.add([initial])
        accumulator.add([child])
        child["parent_ids"].append("not-recorded")
        self.assertEqual(accumulator.finish(metadata())["explanation"]["games"]["islands"]["full_value"], 75)

    def test_missing_operator_does_not_invent_initial_population(self):
        accumulator = ShapleyAccumulator("run-test")
        accumulator.add([{"individual_id": "a", "island_id": 0, "fitness": 10, "parent_ids": []}])
        self.assertEqual(accumulator.finish(metadata())["status"], "unavailable")

    def test_unresolved_ancestry_keeps_coverage_and_retained_pso_origin_is_only_provenance(self):
        accumulator = ShapleyAccumulator("run-test")
        accumulator.add(
            [
                row("a", 0, 100, operator="init"),
                row("b", 1, 100, operator="init"),
                row("p", 1, 1, [], "pso_move", origin_island=0, origin_individual_id="a"),
                row("w", 1, 5, ["b"], "pso_move", origin_island=0, origin_individual_id="a"),
            ]
        )
        result = accumulator.finish(metadata())["explanation"]
        self.assertEqual(result["coverage"]["supported"], 3)
        self.assertEqual(result["coverage"]["excluded"], 1)
        self.assertEqual(result["games"]["routes"]["players"], [])
        self.assertEqual(result["games"]["routes"]["empty_value"], 95)

    def test_metadata_controls_direction_and_requires_explicit_boolean(self):
        accumulator = ShapleyAccumulator("run-test")
        accumulator.add([row("a", 0, 10, operator="init"), row("b", 0, 20, ["a"])])
        self.assertEqual(accumulator.finish(metadata(True))["explanation"]["games"]["islands"]["full_value"], 10)
        self.assertEqual(accumulator.finish(metadata(None))["status"], "unavailable")

    def test_row_and_player_limits_release_retained_lineage_and_remain_unavailable(self):
        for rows, limit in [
            ([row(str(i), 0, 10, operator="init") for i in range(4)], 3),
            ([row(str(i), i, 10, operator="init") for i in range(65)], 100000),
        ]:
            with self.subTest(limit=limit), patch("app.shapley.MAX_ROWS", limit):
                accumulator = ShapleyAccumulator("run-test")
                accumulator.add(rows)
                self.assertEqual(accumulator.finish(metadata())["status"], "unavailable")
                self.assertNotIn('"individual_id"', json.dumps(vars(accumulator)))
                accumulator.add([row("later", 0, 0)])
                self.assertEqual(accumulator.finish(metadata())["status"], "unavailable")

    def test_malformed_lineage_and_resource_failures_are_unavailable_but_programming_errors_propagate(self):
        for rows in [
            [],
            [row("a", 0, float("nan"), operator="init")],
            [row("a", 0, 1, operator="init")] * 2,
            [{"individual_id": "missing-fitness", "island_id": 0, "operator": "init"}],
            [row("a", 0, 1, [7])],
        ]:
            with self.subTest(rows=rows):
                accumulator = ShapleyAccumulator("run-test")
                accumulator.add(rows)
                self.assertEqual(accumulator.finish(metadata())["status"], "unavailable")
        accumulator = ShapleyAccumulator("run-test")
        accumulator.add([row("a", 0, 10, operator="init")])
        with patch("app.shapley.explain", side_effect=MemoryError):
            self.assertEqual(accumulator.finish(metadata())["status"], "unavailable")
        with patch("app.shapley.explain", side_effect=RuntimeError("bug")), self.assertRaises(RuntimeError):
            accumulator.finish(metadata())


class SamplingBudgetTests(unittest.TestCase):
    def test_sampling_budget_rejects_large_work_and_preserves_exact_games(self):
        with self.assertRaisesRegex(ValueError, "work"):
            shapley_game([str(i) for i in range(11)], [(1, 3), (3, 9)], work_limit=2)
        game = shapley_game(["A", "B"], [(3, 12)], work_limit=2)
        self.assertEqual([player["value"] for player in game["players"]], [6, 6])

    def test_worker_adapter_reports_sampling_resource_limit_as_unavailable(self):
        accumulator = ShapleyAccumulator("run-test")
        accumulator.add([row(str(i), i, 100, operator="init") for i in range(11)])
        with patch("app.shapley.MAX_SAMPLING_WORK", 2):
            result = accumulator.finish(metadata())
        self.assertEqual(result["status"], "unavailable")
        self.assertIn("work", result["detail"])

    def test_large_finite_fitness_cannot_emit_nonfinite_json(self):
        accumulator = ShapleyAccumulator("run-test")
        accumulator.add([row("a", 0, 0, operator="init"), row("b", 0, 1e308, ["a"])])
        result = accumulator.finish(metadata(True))
        self.assertEqual(result["status"], "unavailable")
        json.dumps(result, allow_nan=False)

    def test_route_player_limit_is_unavailable(self):
        accumulator = ShapleyAccumulator("run-test")
        rows = [row(f"f{i}", i, 100, operator="init") for i in range(9)]
        rows.extend(
            row(f"m{source}-{dest}", dest, 100, [f"f{source}"], "migration_arrival", origin_island=source, origin_individual_id=f"f{source}")
            for source in range(9)
            for dest in range(9)
            if source != dest
        )
        accumulator.add(rows)
        result = accumulator.finish(metadata())
        self.assertEqual(result["status"], "unavailable")
        self.assertIn("64 observed migration routes", result["detail"])


class LineageTests(unittest.TestCase):
    def test_complementary_islands_share_joint_gain_and_dummy_is_zero(self):
        game = shapley_game(["A", "B", "dummy"], [(3, 12)], seed=7)
        self.assertEqual([p["value"] for p in game["players"]], [6, 6, 0])
        self.assertEqual(game["empty_value"], 0)
        self.assertEqual(game["full_value"], 12)
        self.assertAlmostEqual(game["efficiency_error"], 0)

    def test_full_parent_lineage_not_any_parent_path(self):
        rows = [
            row("a", 0, 100, operator="init"),
            row("b", 1, 100, operator="init"),
            row("c", 1, 80, "a", "migration_arrival", origin_island=0, origin_individual_id="a"),
            row("d", 1, 10, "b;c"),
        ]
        result = explain(rows, maximising=False, run_id="test")
        self.assertEqual([p["value"] for p in result["games"]["islands"]["players"]], [45, 45])
        self.assertEqual(result["games"]["routes"]["full_value"], 90)
        self.assertEqual(result["games"]["routes"]["empty_value"], 0)
        self.assertEqual(result["coverage"]["supported"], 4)

    def test_migration_game_only_allocates_gain_beyond_local_only(self):
        rows = [
            row("a", 0, 100, operator="init"),
            row("b", 1, 100, operator="init"),
            row("l", 0, 60, "a"),
            row("m", 1, 60, "l", "migration_arrival", origin_island=0, origin_individual_id="l"),
            row("w", 1, 20, "b;m"),
        ]
        result = explain(rows, maximising=False, run_id="test")
        game = result["games"]["routes"]
        self.assertEqual(game["empty_value"], 40)
        self.assertEqual(game["full_value"], 80)
        self.assertEqual(game["players"][0]["value"], 40)

    def test_maximisation_and_input_order_independence(self):
        rows = [row("a", 0, -10, operator="init"), row("b", 0, -3, "a")]
        a = explain(rows, maximising=True, run_id="test")
        b = explain(list(reversed(rows)), maximising=True, run_id="test")
        self.assertEqual(a, b)
        self.assertEqual(a["baseline_fitness"], -10)
        self.assertEqual(a["games"]["islands"]["full_value"], 7)

    def test_missing_cycle_and_unlogged_parentage_are_excluded(self):
        rows = [
            row("a", 0, 100, operator="init"),
            row("b", 0, 1, "absent"),
            row("c", 0, 0, "b"),
            row("d", 0, -1),
            row("e", 0, -2, "f"),
            row("f", 0, -3, "e"),
        ]
        r = explain(rows, maximising=False, run_id="test")
        self.assertEqual(r["coverage"]["supported"], 1)
        self.assertEqual(r["coverage"]["excluded"], 5)
        self.assertEqual(r["games"]["islands"]["full_value"], 0)
        self.assertEqual(r["observed_best_fitness"], -3)
        self.assertEqual(r["supported_best_fitness"], 100)

    def test_retained_migrant_provenance_cannot_replace_pso_parentage(self):
        rows = [
            row("a", 0, 100, operator="init"),
            row("b", 1, 100, operator="init"),
            row("m", 1, 100, "a", "migration_arrival", origin_island=0, origin_individual_id="a"),
            row("p", 1, 0, operator="pso_move", origin_island=0, origin_individual_id="a"),
        ]
        result = explain(rows, maximising=False, run_id="test")
        self.assertEqual(result["coverage"]["supported"], 3)
        self.assertEqual(result["games"]["islands"]["full_value"], 0)
        self.assertEqual(result["coverage"]["excluded"], 1)

    def test_non_arrival_origin_fields_do_not_invent_migration_dependencies(self):
        rows = [
            row("a", 0, 100, operator="init"),
            row("b", 1, 100, operator="init"),
            row("p", 1, 0, "b", "pso_move", origin_island=0, origin_individual_id="a"),
        ]
        result = explain(rows, maximising=False, run_id="test")
        self.assertEqual(result["games"]["routes"]["players"], [])
        self.assertEqual(result["games"]["routes"]["empty_value"], 100)

    def test_duplicate_identity_nonfinite_and_missing_initialisation_fail(self):
        for rows in (
            [row("a", 0, 1, operator="init")] * 2,
            [row("a", 0, 100, operator="init"), row("a", 1, 100, operator="init"), row("b", 1, 1, "a")],
            [row("a", 0, math.nan, operator="init")],
            [row("a", 0, 1)],
        ):
            with self.assertRaises(ValueError):
                explain(rows, maximising=False, run_id="test")

    def test_sampled_game_reproducible_efficient_and_reports_standard_error(self):
        a = shapley_game([str(i) for i in range(11)], [(1, 3), (3, 9)], seed=23)
        b = shapley_game([str(i) for i in range(11)], [(1, 3), (3, 9)], seed=23)
        self.assertEqual(a, b)
        self.assertEqual(a["method"], "permutation-sampling")
        self.assertAlmostEqual(sum(p["value"] for p in a["players"]), 9)
        self.assertGreater(a["players"][0]["standard_error"], 0)
        self.assertTrue(all(p["value"] == 0 for p in a["players"][2:]))

    def test_no_routes_and_no_improvement_are_finite(self):
        result = explain([row("a", 0, 1, operator="init")], maximising=False, run_id="test")
        self.assertEqual(result["games"]["routes"]["players"], [])
        self.assertEqual(result["games"]["islands"]["players"][0]["share_percent"], 0)


if __name__ == "__main__":
    unittest.main()
