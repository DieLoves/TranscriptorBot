CREATE TABLE runtime_health (
    role TEXT PRIMARY KEY CHECK (role IN ('bot', 'worker')),
    heartbeat_at REAL NOT NULL
);

CREATE TABLE maintenance_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    checked_at REAL,
    error_count INTEGER NOT NULL DEFAULT 0,
    error_code TEXT
);
INSERT INTO maintenance_state (singleton) VALUES (1);
