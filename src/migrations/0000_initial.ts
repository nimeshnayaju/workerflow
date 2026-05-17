export default `
  -- All timestamps are INTEGER milliseconds since Unix epoch.

  CREATE TABLE workflow_metadata (
    id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),

    status TEXT NOT NULL CHECK (
      status IN ('pending', 'initialized', 'running', 'paused', 'completed', 'failed', 'cancelled')
    ),

    created_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (created_at >= 0),

    updated_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER)),

    definition_input TEXT
      CHECK (definition_input IS NULL OR json_valid(definition_input)),

    CHECK (updated_at >= created_at),

    -- definition_input must be NULL until create() initializes the workflow.
    CHECK (status <> 'pending' OR definition_input IS NULL)
  ) STRICT;

  CREATE TABLE steps (
    id TEXT NOT NULL PRIMARY KEY CHECK (length(id) > 0),
    type TEXT NOT NULL CHECK (type IN ('run', 'sleep', 'wait')),
    created_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (created_at >= 0),

    -- sleep / wait only: run rows use run_step_attempts for lifecycle
    state TEXT CHECK (state IN (
      'waiting',
      'elapsed',
      'satisfied',
      'timed_out'
    )),

    max_attempts INTEGER,

    target_wake_at INTEGER,

    event_name TEXT,
    timeout_at INTEGER,

    resolved_at INTEGER,

    parent_step_id TEXT REFERENCES steps(id) ON UPDATE RESTRICT ON DELETE RESTRICT,

    CHECK (max_attempts IS NULL OR max_attempts >= 1),
    CHECK (target_wake_at IS NULL OR target_wake_at >= 0),
    CHECK (timeout_at IS NULL OR timeout_at >= 0),
    CHECK (resolved_at IS NULL OR resolved_at >= created_at),
    CHECK (event_name IS NULL OR length(event_name) > 0),

    CHECK (
      (
        type = 'run' AND
        state IS NULL AND
        (max_attempts IS NULL OR max_attempts >= 1) AND
        target_wake_at IS NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        resolved_at IS NULL
      )
      OR
      (
        type = 'sleep' AND
        state = 'waiting' AND
        max_attempts IS NULL AND
        target_wake_at IS NOT NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        resolved_at IS NULL
      )
      OR
      (
        type = 'sleep' AND
        state = 'elapsed' AND
        max_attempts IS NULL AND
        target_wake_at IS NOT NULL AND
        event_name IS NULL AND
        timeout_at IS NULL AND
        resolved_at IS NOT NULL
      )
      OR
      (
        type = 'wait' AND
        state = 'waiting' AND
        max_attempts IS NULL AND
        target_wake_at IS NULL AND
        event_name IS NOT NULL AND
        resolved_at IS NULL
      )
      OR
      (
        type = 'wait' AND
        state = 'satisfied' AND
        max_attempts IS NULL AND
        target_wake_at IS NULL AND
        event_name IS NOT NULL AND
        resolved_at IS NOT NULL
      )
      OR
      (
        type = 'wait' AND
        state = 'timed_out' AND
        max_attempts IS NULL AND
        target_wake_at IS NULL AND
        event_name IS NOT NULL AND
        timeout_at IS NOT NULL AND
        resolved_at IS NOT NULL
      )
    ),
    CHECK (parent_step_id IS NULL OR parent_step_id <> id)
  ) STRICT;

  CREATE TABLE run_step_attempts (
    id TEXT NOT NULL PRIMARY KEY
      DEFAULT (lower(hex(randomblob(16))))
      CHECK (length(id) > 0),

    step_id TEXT NOT NULL REFERENCES steps(id) ON UPDATE RESTRICT ON DELETE RESTRICT,

    started_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (started_at >= 0),

    state TEXT NOT NULL CHECK (state IN ('started', 'succeeded', 'failed')),

    ended_at INTEGER CHECK (ended_at IS NULL OR ended_at >= started_at),

    -- Discriminator for the shape of a succeeded result.
    --   'json'   → result_json holds the raw JSON value (never NULL)
    --   'none'   → callback returned undefined/void, result_json IS NULL
    result_type TEXT CHECK (result_type IN ('json', 'none')),

    -- Raw JSON value. No wrapper objects.
    -- NULL when result_type is 'none', or when the attempt hasn't succeeded yet.
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),

    error_message TEXT,
    error_name TEXT,
    next_attempt_at INTEGER CHECK (next_attempt_at IS NULL OR next_attempt_at >= 0),

    CHECK (error_name IS NULL OR length(error_name) > 0),

    CHECK (
      (
        state = 'started' AND
        ended_at IS NULL AND
        result_type IS NULL AND
        result_json IS NULL AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL
      )
      OR
      (
        state = 'succeeded' AND
        ended_at IS NOT NULL AND
        result_type IS NOT NULL AND
        (
          (result_type = 'none' AND result_json IS NULL) OR
          (result_type = 'json' AND result_json IS NOT NULL)
        ) AND
        error_message IS NULL AND
        error_name IS NULL AND
        next_attempt_at IS NULL
      )
      OR
      (
        state = 'failed' AND
        ended_at IS NOT NULL AND
        error_message IS NOT NULL AND
        result_type IS NULL AND
        result_json IS NULL
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
    -- Raw JSON value, or SQL NULL when no payload was provided (undefined).
    -- JSON null is stored as the TEXT literal 'null', distinct from SQL NULL.
    payload TEXT CHECK (payload IS NULL OR json_valid(payload)),
    created_at INTEGER NOT NULL
      DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
      CHECK (created_at >= 0),
    claimed_by TEXT,
    claimed_at INTEGER,

    FOREIGN KEY (claimed_by) REFERENCES steps(id) ON UPDATE RESTRICT ON DELETE RESTRICT,

    CHECK (
      (claimed_by IS NULL AND claimed_at IS NULL) OR
      (claimed_by IS NOT NULL AND claimed_at IS NOT NULL)
    )
  ) STRICT;

  CREATE INDEX run_step_attempts_by_step_time_idx
    ON run_step_attempts(step_id, started_at, id);

  CREATE INDEX run_step_attempts_started_one_idx
    ON run_step_attempts(step_id)
    WHERE state = 'started';

  CREATE INDEX steps_sleep_waiting_by_time_idx
    ON steps(target_wake_at, id)
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

  CREATE INDEX workflow_events_by_time_idx
    ON workflow_events(recorded_at, id);

  CREATE INDEX inbound_events_by_name_and_time_idx
    ON inbound_events(event_name, created_at, id);

  CREATE UNIQUE INDEX inbound_events_claimed_by_unique
    ON inbound_events(claimed_by)
    WHERE claimed_by IS NOT NULL;

  CREATE TRIGGER workflow_metadata_immutable_fields
  BEFORE UPDATE ON workflow_metadata
  FOR EACH ROW
  WHEN NEW.id <> OLD.id OR NEW.created_at <> OLD.created_at
  BEGIN
    SELECT RAISE(ABORT, 'workflow_metadata.id and workflow_metadata.created_at are immutable');
  END;

  CREATE TRIGGER workflow_metadata_valid_transition
  BEFORE UPDATE ON workflow_metadata
  FOR EACH ROW
  WHEN NEW.status <> OLD.status
  BEGIN
    SELECT CASE
      WHEN OLD.status = 'pending' AND NEW.status NOT IN ('initialized', 'cancelled') THEN
        RAISE(ABORT, 'pending can only transition to initialized or cancelled')
      WHEN OLD.status = 'initialized' AND NEW.status NOT IN ('running', 'cancelled') THEN
        RAISE(ABORT, 'initialized can only transition to running or cancelled')
      WHEN OLD.status = 'running' AND NEW.status NOT IN ('paused', 'completed', 'failed', 'cancelled') THEN
        RAISE(ABORT, 'running can only transition to paused, completed, failed, or cancelled')
      WHEN OLD.status = 'paused' AND NEW.status NOT IN ('running', 'cancelled') THEN
        RAISE(ABORT, 'paused can only transition to running or cancelled')
      WHEN OLD.status IN ('completed', 'failed', 'cancelled') THEN
        RAISE(ABORT, 'terminal status cannot transition')
    END;
  END;

  CREATE TRIGGER workflow_metadata_definition_input_immutable_after_init
  BEFORE UPDATE ON workflow_metadata
  FOR EACH ROW
  WHEN OLD.status <> 'pending' AND NEW.definition_input IS NOT OLD.definition_input
  BEGIN
    SELECT RAISE(ABORT, 'workflow_metadata.definition_input is immutable after initialization');
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

  CREATE TRIGGER run_step_attempts_step_must_be_run
  BEFORE INSERT ON run_step_attempts
  WHEN (SELECT type FROM steps WHERE id = NEW.step_id) IS NOT 'run'
  BEGIN
    SELECT RAISE(ABORT, 'run_step_attempts.step_id must reference a run step');
  END;

  CREATE TRIGGER run_step_attempts_at_most_one_started_ins
  BEFORE INSERT ON run_step_attempts
  WHEN NEW.state = 'started'
    AND EXISTS (SELECT 1 FROM run_step_attempts WHERE step_id = NEW.step_id AND state = 'started')
  BEGIN
    SELECT RAISE(ABORT, 'run step already has an in-flight attempt');
  END;

  CREATE TRIGGER run_step_attempts_valid_transition
  BEFORE UPDATE ON run_step_attempts
  FOR EACH ROW
  WHEN NEW.state <> OLD.state
  BEGIN
    SELECT CASE
      WHEN OLD.state = 'started' AND NEW.state NOT IN ('succeeded', 'failed') THEN
        RAISE(ABORT, 'started can only transition to succeeded or failed')
      WHEN OLD.state IN ('succeeded', 'failed') THEN
        RAISE(ABORT, 'terminal attempt state cannot transition')
    END;
  END;

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
`;
