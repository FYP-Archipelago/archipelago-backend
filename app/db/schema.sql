CREATE TABLE IF NOT EXISTS users (
    id uuid PRIMARY KEY,
    email text NOT NULL UNIQUE CHECK (email = lower(email)),
    password_hash text NOT NULL,
    full_name text NOT NULL CHECK (length(btrim(full_name)) > 0),
    institution text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS auth_sessions (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    access_token_hash bytea NOT NULL UNIQUE CHECK (octet_length(access_token_hash) = 32),
    refresh_token_hash bytea NOT NULL UNIQUE CHECK (octet_length(refresh_token_hash) = 32),
    csrf_token text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    access_expires_at timestamptz NOT NULL,
    refresh_expires_at timestamptz NOT NULL,
    CHECK (access_expires_at > created_at AND access_expires_at <= refresh_expires_at)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id text PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id),
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

CREATE TABLE IF NOT EXISTS results (
    run_id text PRIMARY KEY REFERENCES runs (run_id) ON DELETE CASCADE,
    body bytea NOT NULL
);


CREATE INDEX IF NOT EXISTS auth_sessions_user_idx ON auth_sessions (user_id);
CREATE INDEX IF NOT EXISTS auth_sessions_expiry_idx ON auth_sessions (refresh_expires_at);
CREATE INDEX IF NOT EXISTS runs_owner_history_idx ON runs (user_id, created_at DESC, run_id DESC);
