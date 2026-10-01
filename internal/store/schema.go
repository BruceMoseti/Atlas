package store

// schema is applied on every open. Every statement is IF NOT EXISTS, so opening an
// existing database is a no-op. Atlas is young enough not to need migrations; when it
// does, this constant becomes migration 1.
const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    job_id                TEXT PRIMARY KEY,
    idempotency_key       TEXT NOT NULL UNIQUE,
    client_id             TEXT NOT NULL DEFAULT '',
    state                 TEXT NOT NULL,
    priority              INTEGER NOT NULL DEFAULT 0,
    cpu_millis            INTEGER NOT NULL,
    memory_bytes          INTEGER NOT NULL,
    image                 TEXT NOT NULL DEFAULT '',
    command               TEXT NOT NULL DEFAULT '[]',
    env                   TEXT NOT NULL DEFAULT '{}',
    max_attempts          INTEGER NOT NULL,
    attempt_count         INTEGER NOT NULL DEFAULT 0,
    timeout_ns            INTEGER NOT NULL DEFAULT 0,
    retry_on_process_exit INTEGER NOT NULL DEFAULT 0,
    retry_on_timeout      INTEGER NOT NULL DEFAULT 0,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    enqueued_at           INTEGER NOT NULL DEFAULT 0,
    eligible_at           INTEGER NOT NULL DEFAULT 0,
    deadline_at           INTEGER,
    current_attempt_id    TEXT NOT NULL DEFAULT '',
    exit_code             INTEGER,
    failure_class         TEXT NOT NULL DEFAULT '',
    message               TEXT NOT NULL DEFAULT '',
    spec_hash             TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS jobs_state_idx  ON jobs(state);
CREATE INDEX IF NOT EXISTS jobs_queue_idx  ON jobs(state, priority DESC, enqueued_at);
CREATE INDEX IF NOT EXISTS jobs_client_idx ON jobs(client_id, state);

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id       TEXT PRIMARY KEY,
    job_id           TEXT NOT NULL REFERENCES jobs(job_id),
    attempt_number   INTEGER NOT NULL,
    worker_id        TEXT NOT NULL,
    state            TEXT NOT NULL,
    lease_id         TEXT NOT NULL,
    lease_expires_at INTEGER NOT NULL,
    cpu_millis       INTEGER NOT NULL,
    memory_bytes     INTEGER NOT NULL,
    created_at       INTEGER NOT NULL,
    started_at       INTEGER,
    finished_at      INTEGER,
    exit_code        INTEGER,
    failure_class    TEXT NOT NULL DEFAULT '',
    message          TEXT NOT NULL DEFAULT '',
    stdout_tail      TEXT NOT NULL DEFAULT '',
    stderr_tail      TEXT NOT NULL DEFAULT '',
    UNIQUE(job_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS attempts_job_idx    ON attempts(job_id);
CREATE INDEX IF NOT EXISTS attempts_live_idx   ON attempts(state, lease_expires_at);
CREATE INDEX IF NOT EXISTS attempts_worker_idx ON attempts(worker_id, state);

CREATE TABLE IF NOT EXISTS workers (
    worker_id         TEXT PRIMARY KEY,
    hostname          TEXT NOT NULL DEFAULT '',
    version           TEXT NOT NULL DEFAULT '',
    labels            TEXT NOT NULL DEFAULT '{}',
    cpu_capacity      INTEGER NOT NULL,
    memory_capacity   INTEGER NOT NULL,
    cpu_allocated     INTEGER NOT NULL DEFAULT 0,
    memory_allocated  INTEGER NOT NULL DEFAULT 0,
    state             TEXT NOT NULL,
    registered_at     INTEGER NOT NULL,
    last_heartbeat_at INTEGER NOT NULL,
    generation        INTEGER NOT NULL DEFAULT 1
);

-- Append-only audit log. The invariant checker reads this to prove that terminal
-- states were absorbing across the whole run, not merely at the end of it.
CREATE TABLE IF NOT EXISTS transitions (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL,
    job_id     TEXT NOT NULL,
    attempt_id TEXT NOT NULL DEFAULT '',
    from_state TEXT NOT NULL,
    to_state   TEXT NOT NULL,
    reason     TEXT NOT NULL DEFAULT '',
    at         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS transitions_job_idx ON transitions(job_id, seq);
`
