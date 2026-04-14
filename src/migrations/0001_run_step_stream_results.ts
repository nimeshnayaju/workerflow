export default `
  -- Add 'stream' as a result_type for run step attempts and create the stream_chunks table.
  --
  -- SQLite does not support ALTER TABLE to modify CHECK constraints, so we recreate
  -- run_step_attempts with the updated constraints via the rename-copy-drop pattern.

  -- 1. Drop dependent indexes and triggers
  DROP INDEX IF EXISTS run_step_attempts_by_step_time_idx;
  DROP INDEX IF EXISTS run_step_attempts_started_one_idx;
  DROP TRIGGER IF EXISTS run_step_attempts_step_must_be_run;
  DROP TRIGGER IF EXISTS run_step_attempts_at_most_one_started_ins;
  DROP TRIGGER IF EXISTS run_step_attempts_valid_transition;

  -- 2. Rename the old table
  ALTER TABLE run_step_attempts RENAME TO _run_step_attempts_old;

  -- 3. Create the new table with 'stream' added to result_type
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
    --   'stream' → byte chunks live in stream_chunks, result_json IS NULL
    result_type TEXT CHECK (result_type IN ('json', 'none', 'stream')),

    -- Raw JSON value. No wrapper objects.
    -- NULL when result_type is 'none' or 'stream', or when the attempt hasn't succeeded yet.
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
          (result_type = 'none'   AND result_json IS NULL) OR
          (result_type = 'json'   AND result_json IS NOT NULL) OR
          (result_type = 'stream' AND result_json IS NULL)
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

  -- 4. Copy existing rows
  INSERT INTO run_step_attempts (id, step_id, started_at, state, ended_at, result_type, result_json, error_message, error_name, next_attempt_at)
    SELECT id, step_id, started_at, state, ended_at, result_type, result_json, error_message, error_name, next_attempt_at
      FROM _run_step_attempts_old;

  -- 5. Drop the old table
  DROP TABLE _run_step_attempts_old;

  -- 6. Recreate indexes
  CREATE INDEX run_step_attempts_by_step_time_idx
    ON run_step_attempts(step_id, started_at, id);

  CREATE INDEX run_step_attempts_started_one_idx
    ON run_step_attempts(step_id)
    WHERE state = 'started';

  -- 7. Recreate triggers
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

  -- 8. Create the stream_chunks table
  CREATE TABLE stream_chunks (
    attempt_id TEXT NOT NULL
      REFERENCES run_step_attempts(id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    seq INTEGER NOT NULL CHECK (seq >= 0),
    data BLOB NOT NULL CHECK (length(data) > 0),
    PRIMARY KEY (attempt_id, seq)
  ) STRICT;
`;
