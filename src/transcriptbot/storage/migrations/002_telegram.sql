ALTER TABLE jobs ADD COLUMN source_message_id INTEGER;
UPDATE jobs SET source_message_id = message_id;

ALTER TABLE outbox RENAME TO old_outbox;
CREATE TABLE outbox (
    id INTEGER PRIMARY KEY,
    job_id INTEGER REFERENCES jobs(id),
    generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    operation TEXT NOT NULL CHECK (operation IN ('status', 'preview', 'txt', 'json', 'reply', 'callback')),
    artifact_path TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at REAL,
    telegram_message_id INTEGER,
    chat_id INTEGER,
    topic_id INTEGER,
    payload TEXT,
    callback_id TEXT,
    update_id INTEGER REFERENCES processed_updates(update_id),
    last_sent_at REAL,
    status_revision INTEGER,
    UNIQUE (job_id, generation, operation),
    UNIQUE (update_id, operation),
    CHECK ((status = 'sent') = (telegram_message_id IS NOT NULL))
);
INSERT INTO outbox
    (id, job_id, generation, operation, artifact_path, status, attempts,
     next_attempt_at, telegram_message_id, chat_id, topic_id)
SELECT o.id, o.job_id, o.generation, o.operation, o.artifact_path, o.status,
       o.attempts, o.next_attempt_at, o.telegram_message_id, j.chat_id, j.topic_id
FROM old_outbox o JOIN jobs j ON j.id = o.job_id;
DROP TABLE old_outbox;
CREATE INDEX outbox_pending ON outbox(status, next_attempt_at, id);
CREATE TABLE chat_send_state (
    chat_id INTEGER PRIMARY KEY,
    next_send_at REAL NOT NULL
);
