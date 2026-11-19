CREATE TABLE IF NOT EXISTS runs (
    run_id text PRIMARY KEY,
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'clustering', 'done', 'failed', 'cancelling', 'cancelled')),
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

-- Upgrade existing databases without taking the ALTER TABLE lock on every API start.
DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute WHERE attrelid = 'runs'::regclass AND attname = 'runner_key' AND NOT attisdropped
    ) THEN
        ALTER TABLE runs ADD COLUMN IF NOT EXISTS runner_key text NOT NULL DEFAULT 'dea';
    END IF;
END;
$migration$;

CREATE TABLE IF NOT EXISTS results (
    run_id text PRIMARY KEY REFERENCES runs (run_id) ON DELETE CASCADE,
    body bytea NOT NULL
);

CREATE INDEX IF NOT EXISTS runs_history_order_idx
    ON runs (created_at DESC, run_id DESC);
