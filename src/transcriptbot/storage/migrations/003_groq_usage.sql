-- Reserve before the request. Crashes and unknown outcomes consume quota conservatively.
-- No audio, transcripts, keys or API bodies are stored in this journal.
CREATE TABLE groq_usage (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    chunk_number INTEGER NOT NULL,
    started_at REAL NOT NULL,
    seconds REAL NOT NULL CHECK (seconds > 0),
    outcome TEXT NOT NULL DEFAULT 'uncertain'
        CHECK (outcome IN ('uncertain', 'succeeded', 'rejected'))
);
CREATE INDEX groq_usage_window ON groq_usage(started_at);
