import gzip
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI

from app.routes import router

RUN_ID = "run-20261104T120000Z-11223344"


class ExplanationEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = FastAPI()
        self.app.state.pool = object()
        self.app.include_router(router)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_active_runs_are_pending_not_missing(self):
        for status in ("queued", "running", "clustering"):
            with patch("app.routes.queries.get_run", AsyncMock(return_value=SimpleNamespace(status=status))):
                response = await self.client.get(f"/runs/{RUN_ID}/explanation")
                self.assertEqual(response.status_code, 202)
                self.assertEqual(response.json()["status"], "pending")
                self.assertEqual(response.json()["run_id"], RUN_ID)

    async def test_unknown_and_invalid_run_ids_return_404(self):
        with patch("app.routes.queries.get_run", AsyncMock(return_value=None)):
            for run_id in (RUN_ID, "invalid"):
                response = await self.client.get(f"/runs/{run_id}/explanation")
                self.assertEqual(response.status_code, 404)

    async def test_ready_artifact_survives_gzip_and_identity_requests(self):
        envelope = {"schema_version": 1, "run_id": RUN_ID, "status": "ready", "explanation": {"run_id": RUN_ID}}
        body = gzip.compress(json.dumps(envelope).encode())
        with (
            patch("app.routes.queries.get_run", AsyncMock(return_value=SimpleNamespace(status="done"))),
            patch("app.routes.queries.get_explanation_body", AsyncMock(return_value=body), create=True),
        ):
            for encoding in ("gzip", "identity", "gzip;q=0"):
                response = await self.client.get(f"/runs/{RUN_ID}/explanation", headers={"accept-encoding": encoding})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), envelope)
                self.assertEqual(response.headers.get("vary"), "Accept-Encoding")
                self.assertEqual(response.headers.get("content-encoding"), "gzip" if encoding == "gzip" else None)

    async def test_stopped_runs_never_expose_a_successful_artifact(self):
        for status in ("failed", "cancelled"):
            with (
                patch("app.routes.queries.get_run", AsyncMock(return_value=SimpleNamespace(status=status))),
                patch("app.routes.queries.get_explanation_body", AsyncMock(), create=True) as get_body,
            ):
                response = await self.client.get(f"/runs/{RUN_ID}/explanation")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["status"], "unavailable")
                get_body.assert_not_awaited()

    async def test_legacy_completed_run_without_artifact_is_explicitly_unavailable(self):
        with (
            patch("app.routes.queries.get_run", AsyncMock(return_value=SimpleNamespace(status="done"))),
            patch("app.routes.queries.get_explanation_body", AsyncMock(return_value=None), create=True),
        ):
            response = await self.client.get(f"/runs/{RUN_ID}/explanation")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["status"], "unavailable")

    async def test_corrupt_or_mismatched_persistence_is_not_returned_as_attribution(self):
        wrong = gzip.compress(json.dumps({"schema_version": 1, "run_id": "another-run", "status": "unavailable"}).encode())
        for body in (b"invalid gzip", wrong):
            with (
                patch("app.routes.queries.get_run", AsyncMock(return_value=SimpleNamespace(status="done"))),
                patch("app.routes.queries.get_explanation_body", AsyncMock(return_value=body)),
            ):
                response = await self.client.get(f"/runs/{RUN_ID}/explanation")
                self.assertEqual(response.status_code, 503)

    async def test_persisted_unavailability_reason_is_retained(self):
        envelope = {"schema_version": 1, "run_id": RUN_ID, "status": "unavailable", "detail": "Missing initial-population lineage."}
        with (
            patch("app.routes.queries.get_run", AsyncMock(return_value=SimpleNamespace(status="done"))),
            patch("app.routes.queries.get_explanation_body", AsyncMock(return_value=gzip.compress(json.dumps(envelope).encode()))),
        ):
            response = await self.client.get(f"/runs/{RUN_ID}/explanation")
            self.assertEqual(response.json(), envelope)
            self.assertEqual(response.headers["cache-control"], "no-store")


if __name__ == "__main__":
    unittest.main()
