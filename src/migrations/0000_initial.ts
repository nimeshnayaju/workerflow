export default `
  -- All timestamps are INTEGER milliseconds since Unix epoch.

  CREATE TABLE workflow_metadata (
    id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),

    status TEXT NOT NULL CHECK (
      status IN ('pending', 'running', 'paused', 'completed', 'failed', 'cancelled')
    ),

    created_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (created_at >= 0),

    updated_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER)),

    definition_version TEXT
      CHECK (definition_version IS NULL OR length(definition_version) > 0),
    definition_input TEXT
      CHECK (definition_input IS NULL OR json_valid(definition_input)),

    CHECK (updated_at >= created_at),

    -- definition_version and definition_input must be set together or not set at all
    CHECK (definition_version IS NOT NULL OR definition_input IS NULL),

    -- definition must be pinned before running/paused/completing/failing; cancelled is always allowed
    CHECK (status IN ('pending', 'cancelled') OR definition_version IS NOT NULL)
  ) STRICT;

  CREATE TABLE steps (
    id TEXT NOT NULL PRIMARY KEY CHECK (length(id) > 0),
    type TEXT NOT NULL CHECK (type IN ('run', 'sleep', 'wait')),
    state TEXT NOT NULL CHECK (state IN (
      'pending',
      'running',
      'succeeded',
      'failed',
      'waiting',
      'elapsed',
      'satisfied',
      'timed_out'
    )),
    created_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (created_at >= 0),

    -- run-step fields
    attempt_count INTEGER,
    max_attempts INTEGER,
    next_attempt_at INTEGER,
    result TEXT,
    error_message TEXT,
    error_name TEXT,

    -- sleep-step fields
    wake_at INTEGER,

    -- wait-step fields
    event_name TEXT,
    timeout_at INTEGER,
    payload TEXT,

    -- terminal timestamp
    resolved_at INTEGER,

    -- innermost enclosing run step when this row was created (nested run / sleep / wait under a run callback)
    parent_step_id TEXT REFERENCES steps(id) ON UPDATE RESTRICT ON DELETE RESTRICT,

    CHECK (attempt_count IS NULL OR attempt_count >= 0),
    CHECK (max_attempts IS NULL OR max_attempts >= 1),
    CHECK (next_attempt_at IS NULL OR next_attempt_at >= 0),
    CHECK (wake_at IS NULL OR wake_at >= 0),
    CHECK (timeout_at IS NULL OR timeout_at >= 0),
    CHECK (resolved_at IS NULL OR resolved_at >= created_at),
    CHECK (error_name IS NULL OR length(error_name) > 0),
    CHECK (event_name IS NULL OR length(event_name) > 0),

    -- run steps may never exceed max_attempts
    CHECK (
      attempt_count IS NULL OR
      max_attempts IS NULL OR
      attempt_count <= max_attempts
    ),

    CHECK (
      (
        type = 'run' AND
        state = 'pending' AND
        attempt_count IS NOT NULL AND attempt_count >= 0 AND
        (max_attempts IS NULL OR max_attempts >= 1) AND
        (max_attempts IS NULL OR attempt_count < max_attempts) AND
        next_attempt_at IS NOT NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL AND
        resolved_at IS NULL
      )
      OR
      (
        type = 'run' AND
        state = 'running' AND
        attempt_count IS NOT NULL AND attempt_count >= 1 AND
        (max_attempts IS NULL OR max_attempts >= 1) AND
        (max_attempts IS NULL OR attempt_count <= max_attempts) AND
        next_attempt_at IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL AND
        resolved_at IS NULL
      )
      OR
      (
        type = 'run' AND
        state = 'succeeded' AND
        attempt_count IS NOT NULL AND attempt_count >= 1 AND
        (max_attempts IS NULL OR max_attempts >= 1) AND
        (max_attempts IS NULL OR attempt_count <= max_attempts) AND
        next_attempt_at IS NULL AND
        result IS NOT NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL AND
        resolved_at IS NOT NULL
      )
      OR
      (
        type = 'run' AND
        state = 'failed' AND
        attempt_count IS NOT NULL AND attempt_count >= 1 AND
        (max_attempts IS NULL OR max_attempts >= 1) AND
        (max_attempts IS NULL OR attempt_count <= max_attempts) AND
        next_attempt_at IS NULL AND
        result IS NULL AND
        error_message IS NOT NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL AND
        resolved_at IS NOT NULL
      )
      OR
      (
        type = 'sleep' AND
        state = 'waiting' AND
        attempt_count IS NULL AND
        max_attempts IS NULL AND
        next_attempt_at IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        wake_at IS NOT NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL AND
        resolved_at IS NULL
      )
      OR
      (
        type = 'sleep' AND
        state = 'elapsed' AND
        attempt_count IS NULL AND
        max_attempts IS NULL AND
        next_attempt_at IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL AND
        resolved_at IS NOT NULL
      )
      OR
      (
        type = 'wait' AND
        state = 'waiting' AND
        attempt_count IS NULL AND
        max_attempts IS NULL AND
        next_attempt_at IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        wake_at IS NULL AND
        event_name IS NOT NULL AND
        payload IS NULL AND
        resolved_at IS NULL
      )
      OR
      (
        type = 'wait' AND
        state = 'satisfied' AND
        attempt_count IS NULL AND
        max_attempts IS NULL AND
        next_attempt_at IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        wake_at IS NULL AND
        event_name IS NOT NULL AND
        timeout_at IS NULL AND
        payload IS NOT NULL AND
        resolved_at IS NOT NULL
      )
      OR
      (
        type = 'wait' AND
        state = 'timed_out' AND
        attempt_count IS NULL AND
        max_attempts IS NULL AND
        next_attempt_at IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        wake_at IS NULL AND
        event_name IS NOT NULL AND
        timeout_at IS NULL AND
        payload IS NULL AND
        resolved_at IS NOT NULL
      )
    ),
    CHECK (parent_step_id IS NULL OR parent_step_id <> id)
  ) STRICT;

  CREATE TABLE step_events (
    id TEXT NOT NULL PRIMARY KEY
      DEFAULT (lower(hex(randomblob(16))))
      CHECK (length(id) > 0),

    step_id TEXT NOT NULL,
    recorded_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (recorded_at >= 0),

    type TEXT NOT NULL CHECK (type IN (
      'attempt_started',
      'attempt_succeeded',
      'attempt_failed',
      'sleep_waiting',
      'sleep_elapsed',
      'wait_waiting',
      'wait_satisfied',
      'wait_timed_out'
    )),

    attempt_number INTEGER,
    result TEXT,
    error_message TEXT,
    error_name TEXT,
    next_attempt_at INTEGER,
    wake_at INTEGER,
    event_name TEXT,
    timeout_at INTEGER,
    payload TEXT,

    FOREIGN KEY (step_id) REFERENCES steps(id) ON UPDATE RESTRICT ON DELETE RESTRICT,

    CHECK (attempt_number IS NULL OR attempt_number >= 1),
    CHECK (next_attempt_at IS NULL OR next_attempt_at >= 0),
    CHECK (wake_at IS NULL OR wake_at >= 0),
    CHECK (timeout_at IS NULL OR timeout_at >= 0),
    CHECK (error_name IS NULL OR length(error_name) > 0),
    CHECK (event_name IS NULL OR length(event_name) > 0),

    CHECK (
      (
        type = 'attempt_started' AND
        attempt_number IS NOT NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL
      )
      OR
      (
        type = 'attempt_succeeded' AND
        attempt_number IS NOT NULL AND
        result IS NOT NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL
      )
      OR
      (
        type = 'attempt_failed' AND
        attempt_number IS NOT NULL AND
        result IS NULL AND
        error_message IS NOT NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL
      )
      OR
      (
        type = 'sleep_waiting' AND
        attempt_number IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL AND
        wake_at IS NOT NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL
      )
      OR
      (
        type = 'sleep_elapsed' AND
        attempt_number IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL
      )
      OR
      (
        type = 'wait_waiting' AND
        attempt_number IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL AND
        wake_at IS NULL AND
        event_name IS NOT NULL AND
        payload IS NULL
      )
      OR
      (
        type = 'wait_satisfied' AND
        attempt_number IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NOT NULL
      )
      OR
      (
        type = 'wait_timed_out' AND
        attempt_number IS NULL AND
        result IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL AND
        wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        payload IS NULL
      )
    )
  ) STRICT;

  CREATE TABLE workflow_events (
    id INTEGER NOT NULL PRIMARY KEY,

    recorded_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (recorded_at >= 0),

    type TEXT NOT NULL CHECK (type IN (
      'created',
      'started',
      'paused',
      'resumed',
      'completed',
      'failed',
      'cancelled'
    )),

    cancellation_reason TEXT,

    CHECK (cancellation_reason IS NULL OR type = 'cancelled')
  ) STRICT;

  CREATE TABLE inbound_events (
    id TEXT NOT NULL PRIMARY KEY
      DEFAULT (lower(hex(randomblob(16))))
      CHECK (length(id) > 0),
    event_name TEXT NOT NULL CHECK (length(event_name) > 0),
    payload TEXT CHECK (payload IS NULL OR json_valid(payload)),
    created_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (created_at >= 0),
    claimed_by TEXT,
    claimed_at INTEGER,

    FOREIGN KEY (claimed_by) REFERENCES steps(id) ON UPDATE RESTRICT ON DELETE RESTRICT,

    -- claimed_by and claimed_at must be set together or not set at all
    CHECK (
      (claimed_by IS NULL AND claimed_at IS NULL) OR
      (claimed_by IS NOT NULL AND claimed_at IS NOT NULL)
    )
  ) STRICT;

  -- scheduler/query indexes
  CREATE INDEX steps_run_pending_by_time_idx
    ON steps(next_attempt_at, id)
    WHERE type = 'run' AND state = 'pending';

  CREATE INDEX steps_sleep_waiting_by_time_idx
    ON steps(wake_at, id)
    WHERE type = 'sleep' AND state = 'waiting';

  CREATE INDEX steps_wait_waiting_by_event_idx
    ON steps(event_name, id)
    WHERE type = 'wait' AND state = 'waiting';

  CREATE INDEX steps_wait_waiting_by_timeout_idx
    ON steps(timeout_at, id)
    WHERE type = 'wait' AND state = 'waiting' AND timeout_at IS NOT NULL;

  CREATE INDEX steps_by_parent_step_id_idx
    ON steps(parent_step_id, id)
    WHERE parent_step_id IS NOT NULL;

  CREATE INDEX step_events_by_step_and_time_idx
    ON step_events(step_id, recorded_at, id);

  CREATE INDEX workflow_events_by_time_idx
    ON workflow_events(recorded_at, id);

  CREATE INDEX inbound_events_by_name_and_time_idx
    ON inbound_events(event_name, created_at, id);

  CREATE INDEX inbound_events_by_claimed_by_idx
    ON inbound_events(claimed_by, id)
    WHERE claimed_by IS NOT NULL;

  -- immutable identity fields
  CREATE TRIGGER workflow_metadata_immutable_fields
  BEFORE UPDATE ON workflow_metadata
  FOR EACH ROW
  WHEN NEW.id <> OLD.id OR NEW.created_at <> OLD.created_at
  BEGIN
    SELECT RAISE(ABORT, 'workflow_metadata.id and workflow_metadata.created_at are immutable');
  END;

  -- valid status transitions
  CREATE TRIGGER workflow_metadata_valid_transition
  BEFORE UPDATE ON workflow_metadata
  FOR EACH ROW
  WHEN NEW.status <> OLD.status
  BEGIN
    SELECT CASE
      WHEN OLD.status = 'pending' AND NEW.status NOT IN ('running', 'cancelled') THEN
        RAISE(ABORT, 'pending can only transition to running or cancelled')
      WHEN OLD.status = 'running' AND NEW.status NOT IN ('paused', 'completed', 'failed', 'cancelled') THEN
        RAISE(ABORT, 'running can only transition to paused, completed, failed, or cancelled')
      WHEN OLD.status = 'paused' AND NEW.status NOT IN ('running', 'cancelled') THEN
        RAISE(ABORT, 'paused can only transition to running or cancelled')
      WHEN OLD.status IN ('completed', 'failed', 'cancelled') THEN
        RAISE(ABORT, 'terminal status cannot transition')
    END;
  END;

  CREATE TRIGGER steps_immutable_identity_fields
  BEFORE UPDATE ON steps
  FOR EACH ROW
  WHEN NEW.id <> OLD.id OR NEW.type <> OLD.type OR NEW.created_at <> OLD.created_at
  BEGIN
    SELECT RAISE(ABORT, 'steps.id, steps.type, and steps.created_at are immutable');
  END;

  CREATE TRIGGER steps_parent_step_id_immutable
  BEFORE UPDATE ON steps
  FOR EACH ROW
  WHEN NEW.parent_step_id IS NOT OLD.parent_step_id
  BEGIN
    SELECT RAISE(ABORT, 'steps.parent_step_id is immutable');
  END;

  CREATE TRIGGER steps_parent_must_be_run
  BEFORE INSERT ON steps
  FOR EACH ROW
  WHEN NEW.parent_step_id IS NOT NULL
    AND (SELECT type FROM steps WHERE id = NEW.parent_step_id) IS NOT 'run'
  BEGIN
    SELECT RAISE(ABORT, 'steps.parent_step_id must reference a run step');
  END;

  -- append-only step events
  CREATE TRIGGER step_events_append_only_update
  BEFORE UPDATE ON step_events
  FOR EACH ROW
  BEGIN
    SELECT RAISE(ABORT, 'step_events is append-only');
  END;

  CREATE TRIGGER step_events_append_only_delete
  BEFORE DELETE ON step_events
  FOR EACH ROW
  BEGIN
    SELECT RAISE(ABORT, 'step_events is append-only');
  END;

  -- append-only workflow events
  CREATE TRIGGER workflow_events_append_only_update
  BEFORE UPDATE ON workflow_events
  FOR EACH ROW
  BEGIN
    SELECT RAISE(ABORT, 'workflow_events is append-only');
  END;

  CREATE TRIGGER workflow_events_append_only_delete
  BEFORE DELETE ON workflow_events
  FOR EACH ROW
  BEGIN
    SELECT RAISE(ABORT, 'workflow_events is append-only');
  END;

  -- step_events.type must match the referenced row in steps.type
  CREATE TRIGGER step_events_parent_type_match
  BEFORE INSERT ON step_events
  FOR EACH ROW
  BEGIN
    SELECT CASE
      WHEN NOT EXISTS (SELECT 1 FROM steps WHERE id = NEW.step_id) THEN
        RAISE(ABORT, 'step_events.step_id does not reference an existing steps row')
      WHEN NEW.type IN ('attempt_started', 'attempt_succeeded', 'attempt_failed')
           AND (SELECT type FROM steps WHERE id = NEW.step_id) <> 'run' THEN
        RAISE(ABORT, 'run attempt events require a run step')
      WHEN NEW.type IN ('sleep_waiting', 'sleep_elapsed')
           AND (SELECT type FROM steps WHERE id = NEW.step_id) <> 'sleep' THEN
        RAISE(ABORT, 'sleep events require a sleep step')
      WHEN NEW.type IN ('wait_waiting', 'wait_satisfied', 'wait_timed_out')
           AND (SELECT type FROM steps WHERE id = NEW.step_id) <> 'wait' THEN
        RAISE(ABORT, 'wait events require a wait step')
    END;
  END;
`;
