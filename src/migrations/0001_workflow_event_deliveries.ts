export default `
  -- Terminal workflow outcomes are delivered at least once. The workflow event
  -- remains append-only; mutable acknowledgement and retry state lives here.
  CREATE TABLE workflow_event_deliveries (
    event_id INTEGER NOT NULL PRIMARY KEY
      REFERENCES workflow_events(id) ON UPDATE RESTRICT ON DELETE RESTRICT,

    attempts INTEGER NOT NULL DEFAULT 0
      CHECK (attempts >= 0),

    next_attempt_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (next_attempt_at >= 0),

    delivered_at INTEGER
      CHECK (delivered_at IS NULL OR delivered_at >= 0),

    last_error TEXT
  ) STRICT;
`;
