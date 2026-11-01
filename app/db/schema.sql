CREATE TABLE IF NOT EXISTS runs (
    run_id text PRIMARY KEY,
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'clustering', 'done', 'failed', 'cancelled')),
    stream boolean NOT NULL,
    preset text NOT NULL,
    overrides jsonb NOT NULL DEFAULT '{}',
    clustering jsonb NOT NULL,
    error_phase text CHECK (error_phase IN ('runner', 'clustering', 'internal')),
    error_message text,
    streams_expired boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'failed') = (error_phase IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS results (
    run_id text PRIMARY KEY REFERENCES runs (run_id) ON DELETE CASCADE,
    body bytea NOT NULL
);
