CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL,
    updated_at REAL NOT NULL DEFAULT (julianday('now'))
);

CREATE TABLE IF NOT EXISTS log_entries (
    log_index INTEGER PRIMARY KEY,
    term INTEGER NOT NULL,
    entry_data BLOB NOT NULL,
    created_at REAL NOT NULL DEFAULT (julianday('now'))
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY,
    last_included_index INTEGER NOT NULL,
    last_included_term INTEGER NOT NULL,
    state_machine_data BLOB NOT NULL,
    configuration TEXT NOT NULL,
    timestamp REAL NOT NULL,
    checksum TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_at REAL NOT NULL DEFAULT (julianday('now'))
);

CREATE INDEX IF NOT EXISTS idx_log_entries_term 
    ON log_entries(term);

CREATE INDEX IF NOT EXISTS idx_snapshots_created_at 
    ON snapshots(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_last_included 
    ON snapshots(last_included_index, last_included_term);