# Shapley-only backend integration

User scope: add only APIs and Python analysis required for Shapley, submit a backend PR for teammate review. Do not add upload, convergence, migration timing, server history or unrelated backend features. Preserve the existing API/worker/result structure and dirty sibling checkout.

## Contract

`GET /runs/{run_id}/explanation` returns a versioned envelope:

- HTTP 200: `{schema_version:1, run_id, status:"ready", explanation:<existing v1 artifact>}`.
- HTTP 202 while queued/running/clustering: `{schema_version:1, run_id, status:"pending", detail:<text>}`.
- HTTP 200 when unavailable (legacy completed runs, missing/unusable lineage, failed/cancelled runs, explicit resource limits): `{schema_version:1, run_id, status:"unavailable", detail:<text>}`.
- HTTP 404 for unknown/invalid run IDs, using the existing API error style.

Compute final observational island/route games in Python from compact raw lineage before Redis expiry. Reuse the reviewed Python core; no cluster-graph ancestry inference, optimiser reruns, frontend scientific calculations, or genome exposure. Preserve incomplete-data coverage diagnostics. Bound retained rows and players and return unavailable without failing an otherwise valid graph run if analysis cannot be supported.

Worker finalization adds a `b"shapley"` field alongside FINAL in the same clustered-stream entry. Payload is the JSON envelope above. Streaming recovery must replay the lineage accumulator together with clustering. Supervisor compresses both JSON payloads and saves them in one existing result transaction. Add a separate FK-linked `explanations` table, keeping the public module Result schema unchanged. Cancelled/failed runs never expose a successful explanation. Existing results and old worker entries without the new field remain supported.

Frontend prefers the backend endpoint for backend run IDs, polls only while pending, aborts stale work on navigation/backend change, and exposes retry/errors/unavailability. The two published recordings remain explicitly selectable as sample data. No backend index/history endpoint is added. Upload is out of scope and absent from navigation.

## Verification

Python core invariants, streaming versus batch agreement, replay and resource limits, endpoint state/identity/gzip behavior, persistence atomicity and cancellation/legacy behavior; JS stale-response and pending/error/render tests, production builds, actual contract smoke flows. No Docker testing without a change to the user's prior constraint. Review backend PR and frontend before pushing `feat/backend-integration`; do not merge main.
