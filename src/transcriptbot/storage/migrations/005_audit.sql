-- Track changed statuses rather than scan the unlimited history four times/s.
CREATE TABLE dirty_status (
    job_id INTEGER PRIMARY KEY REFERENCES jobs(id)
);
INSERT INTO dirty_status SELECT id FROM jobs;
CREATE TRIGGER jobs_status_insert AFTER INSERT ON jobs BEGIN
    INSERT OR IGNORE INTO dirty_status VALUES (NEW.id);
END;
CREATE TRIGGER jobs_status_update AFTER UPDATE OF revision ON jobs
WHEN OLD.revision != NEW.revision BEGIN
    INSERT OR IGNORE INTO dirty_status VALUES (NEW.id);
END;
CREATE INDEX jobs_cleanup_pending ON jobs(id) WHERE cleanup_pending = 1;
CREATE INDEX jobs_failed_retention ON jobs(retention_until, id)
    WHERE status = 'failed' AND resumable = 1;
CREATE INDEX job_assets_reverse ON job_assets(asset_id, job_id);
CREATE INDEX outbox_sent_delivery ON outbox(job_id, operation) WHERE status = 'sent';
CREATE INDEX processed_updates_age ON processed_updates(processed_at, update_id);
CREATE INDEX outbox_pending_replies ON outbox(chat_id, operation)
    WHERE status = 'pending' AND job_id IS NULL;
