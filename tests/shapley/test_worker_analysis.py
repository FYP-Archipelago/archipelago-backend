"""Worker transport tests use an in-memory Redis stream and a deterministic cluster boundary."""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from archipelago import Config

from app.worker import jobs

RUN_ID = "run-worker-test"


def row(identity, island, fitness, parents=None, operator="trial", **extra):
    return dict(individual_id=identity, island_id=island, fitness=fitness, parent_ids=parents or [], operator=operator, **extra)


class FakeRedis:
    def __init__(self, records):
        self.records = [(f"{i + 1}-0".encode(), {b"kind": kind, b"data": json.dumps(data).encode()}) for i, (kind, data) in enumerate(records)]
        self.writes = []

    @staticmethod
    def number(entry_id):
        return int(entry_id.split(b"-")[0])

    def xrange(self, key, start, end, count):
        assert key == f"run:{RUN_ID}:runner_logs"
        lower = 0 if start == "-" else self.number(start.removeprefix(b"(")) + 1
        return [(identity, fields) for identity, fields in self.records if lower <= self.number(identity) <= self.number(end)][:count]

    def xread(self, streams, count, block):
        assert set(streams) == {f"run:{RUN_ID}:runner_logs"}
        after = self.number(next(iter(streams.values())))
        page = [(identity, fields) for identity, fields in self.records if self.number(identity) > after][:count]
        if not page:
            raise AssertionError("Worker read beyond the run terminator")
        return [(b"logs", page)]

    def xadd(self, key, fields):
        assert key == f"run:{RUN_ID}:clustered"
        self.writes.append(fields)


class FakeResult:
    run = SimpleNamespace(maximising=False, algorithm="ga", benchmark="sphere", migration_topology="ring")

    def __init__(self, rows):
        self.rows = rows

    def model_dump_json(self):
        return json.dumps({"fitness": [row["fitness"] for row in self.rows]})


class FakeClusterStream:
    def __init__(self, config):
        self.rows = []

    def cluster(self, rows, events):
        self.rows.extend(rows)
        return SimpleNamespace(model_dump_json=lambda: "{}")

    def finish(self):
        return FakeResult(self.rows)


class WorkerAnalysisTests(unittest.TestCase):
    def records(self, missing_operator=False):
        rows = [
            row("a", 0, 100, operator="init"),
            row("b", 1, 100, operator="init"),
            row("m", 1, 80, ["a"], "migration_arrival", origin_island=0, origin_individual_id="a"),
            row("w", 1, 10, ["b", "m"]),
        ]
        if missing_operator:
            for record in rows:
                record.pop("operator")
        return [(b"row", record) for record in rows] + [(b"event", {"type": "run_end", "termination_reason": "completed"})]

    def execute(self, streamed, covers=None, missing_operator=False):
        client = FakeRedis(self.records(missing_operator))
        with (
            patch.object(jobs, "ClusterStream", FakeClusterStream),
            patch.object(jobs, "READ_COUNT", 2),
            patch.object(jobs, "cluster_run", side_effect=lambda rows, events, config: FakeResult(rows)),
            patch.object(jobs.time, "sleep"),
        ):
            if streamed:
                jobs.run_streamed(client, RUN_ID, None, covers)
            else:
                jobs.run_batch(client, RUN_ID, None)
        return client.writes

    def test_streaming_batch_and_paged_recovery_share_final_explanation(self):
        final = self.execute(False)[-1]
        self.assertEqual(set(final), {b"final", b"shapley"})
        payload = json.loads(final[b"shapley"])
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["explanation"]["coverage"]["supported"], 4)
        self.assertEqual([p["value"] for p in payload["explanation"]["games"]["islands"]["players"]], [45, 45])
        for covers in (None, b"2-0", b"3-0", b"5-0"):
            with self.subTest(covers=covers):
                writes = self.execute(True, covers)
                self.assertEqual(writes[-1], final)
                self.assertTrue(all(b"covers" in entry and b"shapley" not in entry for entry in writes[:-1]))
                if covers is not None:
                    self.assertTrue(all(FakeRedis.number(entry[b"covers"]) > FakeRedis.number(covers) for entry in writes[:-1]))

    def test_missing_lineage_returns_unavailable_alongside_successful_graph(self):
        for streamed in (False, True):
            with self.subTest(streamed=streamed):
                final = self.execute(streamed, missing_operator=True)[-1]
                self.assertIn(b"final", final)
                self.assertEqual(json.loads(final[b"shapley"])["status"], "unavailable")

    def test_resource_limit_does_not_fail_graph_and_unrelated_analysis_errors_propagate(self):
        with patch("app.shapley.MAX_ROWS", 3):
            final = self.execute(True)[-1]
        self.assertIn(b"final", final)
        self.assertEqual(json.loads(final[b"shapley"])["status"], "unavailable")
        with patch("app.shapley.explain", side_effect=RuntimeError("unexpected bug")), self.assertRaises(RuntimeError):
            self.execute(False)

    def test_installed_module_result_metadata_works_in_both_worker_modes(self):
        records = [
            (
                b"event",
                {
                    "type": "run_start",
                    "run_id": RUN_ID,
                    "algorithm": "ga",
                    "benchmark": "sphere",
                    "num_islands": 1,
                    "evaluation_budget": 2,
                    "evaluation_budget_per_island": 2,
                    "datasets": {"maximising": False},
                    "migration": {"topology": "ring"},
                },
            ),
            (
                b"event",
                {
                    "type": "island_start",
                    "island_id": 0,
                    "genome_encoding": "real_vector",
                    "benchmark_params": {"dimension": 2, "lower": -5.0, "upper": 5.0},
                    "clock_offset_ns": 0,
                },
            ),
        ]
        for index, (identity, fitness) in enumerate([("a", 100), ("b", 25)]):
            record = row(identity, 0, fitness, [] if index == 0 else ["a"], "init" if index == 0 else "trial")
            record.update(
                run_id=RUN_ID,
                seq=index,
                eval_index=index,
                generation=index,
                t_wall=index * 0.5,
                genome_encoding="real_vector",
                genome_hash=identity,
                genome_repr=[float(index), float(index + 1)],
                genome_repr_mode="full",
            )
            records.append((b"row", record))
        records.extend(
            [
                (b"event", {"type": "generation_end", "island_id": 0, "maximising": False}),
                (b"event", {"type": "island_end", "island_id": 0}),
                (b"event", {"type": "run_end", "termination_reason": "completed"}),
            ]
        )
        payloads = []
        for streamed in (False, True):
            client = FakeRedis(records)
            with patch.object(jobs.time, "sleep"):
                if streamed:
                    jobs.run_streamed(client, RUN_ID, Config(lsh=True), None)
                else:
                    jobs.run_batch(client, RUN_ID, Config(lsh=True))
            final = client.writes[-1]
            self.assertEqual(json.loads(final[b"final"])["run"]["run_id"], RUN_ID)
            payload = json.loads(final[b"shapley"])
            self.assertEqual(payload["status"], "ready")
            self.assertEqual(payload["explanation"]["coverage"]["supported"], 2)
            self.assertEqual(payload["explanation"]["games"]["islands"]["full_value"], 75)
            self.assertEqual(payload["explanation"]["source"]["topology"], "ring")
            payloads.append(payload)
        self.assertEqual(payloads[0], payloads[1])


if __name__ == "__main__":
    unittest.main()
