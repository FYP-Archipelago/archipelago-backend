import gzip
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.db import queries
from app.redis_names import FINAL, SHAPLEY
from app.supervisor import _read_clustered


class Context:
    def __init__(self, value):
        self.value = value
        self.error = None

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, kind, error, traceback):
        self.error = error


class Connection:
    def __init__(self, rowcount=1, fail_explanation=False):
        self.calls = []
        self.rowcount = rowcount
        self.fail_explanation = fail_explanation
        self.transaction_context = Context(self)

    def transaction(self):
        return self.transaction_context

    async def execute(self, sql, values):
        self.calls.append((sql, values))
        if self.fail_explanation and sql.startswith("INSERT INTO explanations"):
            raise RuntimeError("database write interrupted")
        return SimpleNamespace(rowcount=self.rowcount)


class PersistenceTests(unittest.IsolatedAsyncioTestCase):
    def pool(self, conn):
        return SimpleNamespace(connection=lambda: Context(conn))

    async def test_graph_and_analysis_are_saved_in_one_transaction(self):
        conn = Connection()
        await queries.save_result(self.pool(conn), "run-test", b"graph", explanation_body=b"analysis")
        self.assertEqual(len(conn.calls), 3)
        self.assertIn("INSERT INTO results", conn.calls[1][0])
        self.assertIn("INSERT INTO explanations", conn.calls[2][0])
        self.assertEqual(conn.calls[2][1], ("run-test", b"analysis"))
        self.assertIsNone(conn.transaction_context.error)

    async def test_analysis_write_failure_exits_transaction_with_error(self):
        conn = Connection(fail_explanation=True)
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            await queries.save_result(self.pool(conn), "run-test", b"graph", explanation_body=b"analysis")
        self.assertIsInstance(conn.transaction_context.error, RuntimeError)

    async def test_stopped_or_completed_run_does_not_receive_an_orphan_artifact(self):
        conn = Connection(rowcount=0)
        await queries.save_result(self.pool(conn), "run-test", b"graph", explanation_body=b"analysis")
        self.assertEqual(len(conn.calls), 1)

    async def test_legacy_worker_result_remains_compatible(self):
        conn = Connection()
        await queries.save_result(self.pool(conn), "run-test", b"graph")
        self.assertEqual(len(conn.calls), 2)

    async def test_supervisor_persists_both_payloads_before_advancing_stream_cursor(self):
        for has_analysis in (False, True):
            fields = {FINAL: b'{"graph":{}}'}
            if has_analysis:
                fields[SHAPLEY] = b'{"status":"unavailable"}'
            redis = SimpleNamespace(xread=AsyncMock(return_value=[(b"stream", [(b"12-0", fields)])]))
            state = SimpleNamespace(redis=redis, run_id="run-test", last_id="0-0", pool=object(), worker_done=False)
            with patch("app.supervisor.queries.save_result", AsyncMock()) as save:
                await _read_clustered(state)
                self.assertEqual(gzip.decompress(save.await_args.args[2]), fields[FINAL])
                if has_analysis:
                    self.assertEqual(gzip.decompress(save.await_args.kwargs["explanation_body"]), fields[SHAPLEY])
                else:
                    self.assertEqual(save.await_args.kwargs, {})
                self.assertEqual(state.last_id, "12-0")
                self.assertTrue(state.worker_done)

    async def test_supervisor_retries_the_entry_if_persistence_fails(self):
        redis = SimpleNamespace(xread=AsyncMock(return_value=[(b"stream", [(b"12-0", {FINAL: b"graph", SHAPLEY: b"analysis"})])]))
        state = SimpleNamespace(redis=redis, run_id="run-test", last_id="0-0", pool=object(), worker_done=False)
        with patch("app.supervisor.queries.save_result", AsyncMock(side_effect=RuntimeError("store unavailable"))), self.assertRaises(RuntimeError):
            await _read_clustered(state)
        self.assertEqual(state.last_id, "0-0")
        self.assertFalse(state.worker_done)


if __name__ == "__main__":
    unittest.main()
