CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at REAL NOT NULL
);

CREATE TABLE queue_sequence (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    value INTEGER NOT NULL CHECK (value >= 0)
);
INSERT INTO queue_sequence VALUES (1, 0);

CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER NOT NULL CHECK (owner_id > 0),
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    topic_id INTEGER,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('telegram', 'youtube', 'mailru', 'direct')),
    source_ref TEXT NOT NULL,
    title TEXT,
    size_bytes INTEGER CHECK (size_bytes >= 0),
    duration_seconds REAL CHECK (duration_seconds >= 0),
    model TEXT NOT NULL CHECK (model = 'whisper-large-v3-turbo'),
    queue_no INTEGER NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN
        ('queued', 'active', 'retry_wait', 'quota_wait', 'failed', 'cancelled', 'completed')),
    phase TEXT NOT NULL CHECK (phase IN ('download', 'prepare', 'transcribe', 'assemble', 'deliver')),
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    quota_attempts INTEGER NOT NULL DEFAULT 0 CHECK (quota_attempts >= 0),
    next_attempt_at REAL,
    error_code TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    finished_at REAL,
    retention_until REAL,
    resumable INTEGER NOT NULL DEFAULT 1 CHECK (resumable IN (0, 1)),
    cleanup_pending INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_pending IN (0, 1)),
    status_message_id INTEGER,
    CHECK ((status IN ('retry_wait', 'quota_wait')) = (next_attempt_at IS NOT NULL)),
    CHECK ((status IN ('failed', 'cancelled', 'completed')) = (finished_at IS NOT NULL)),
    CHECK ((status = 'failed') = (retention_until IS NOT NULL))
);
CREATE INDEX jobs_owner_history ON jobs(owner_id, id DESC);
CREATE INDEX jobs_queue ON jobs(status, queue_no);
CREATE UNIQUE INDEX jobs_single_active ON jobs((1))
    WHERE status IN ('active', 'retry_wait', 'quota_wait');

CREATE TABLE processed_updates (
    update_id INTEGER PRIMARY KEY CHECK (update_id >= 0),
    outcome TEXT NOT NULL,
    job_id INTEGER REFERENCES jobs(id),
    queue_position INTEGER,
    processed_at REAL NOT NULL
);
CREATE TABLE polling_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    next_offset INTEGER NOT NULL CHECK (next_offset >= 0)
);
INSERT INTO polling_state VALUES (1, 0);

CREATE TABLE chunks (
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    number INTEGER NOT NULL CHECK (number >= 0),
    start_sample INTEGER NOT NULL CHECK (start_sample >= 0),
    end_sample INTEGER NOT NULL CHECK (end_sample > start_sample),
    response_path TEXT,
    response_sha256 TEXT,
    completed_at REAL,
    PRIMARY KEY (job_id, number),
    CHECK ((response_path IS NOT NULL) = (response_sha256 IS NOT NULL)),
    CHECK ((response_path IS NOT NULL) = (completed_at IS NOT NULL))
);

CREATE TABLE assets (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('workspace', 'telegram_cache')),
    path TEXT NOT NULL UNIQUE,
    sha256 TEXT,
    size_bytes INTEGER CHECK (size_bytes >= 0),
    cleanup_pending INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_pending IN (0, 1))
);
CREATE TABLE job_assets (
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    asset_id INTEGER NOT NULL REFERENCES assets(id),
    PRIMARY KEY (job_id, asset_id)
);

-- Content stays in files; preview uses the same TXT path as the text attachment.
CREATE TABLE outbox (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    operation TEXT NOT NULL CHECK (operation IN ('status', 'preview', 'txt', 'json')),
    artifact_path TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at REAL,
    telegram_message_id INTEGER,
    UNIQUE (job_id, generation, operation),
    CHECK ((status = 'sent') = (telegram_message_id IS NOT NULL))
);
