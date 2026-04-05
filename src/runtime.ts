import { DurableObject, RpcTarget } from "cloudflare:workers";
import type { WorkflowDefinition } from "./definition";
import type { Json } from "./json";
import mig000 from "./migrations/0000_initial";
import type { Brand } from "./brand";

export abstract class WorkflowRuntime<
  TInput extends Json | undefined = Json | undefined,
  TVersion extends string = string
> extends DurableObject {
  private static readonly MIGRATIONS = [mig000];
  private readonly sql: SqlStorage;
  #status: WorkflowStatus;
  #isRunLoopActive: boolean = false;
  #definitionVersion: TVersion | undefined;
  #definitionInput: TInput | undefined;

  /**
   * @param status - The new status of the workflow; one of "running", "paused", "completed", "failed", or "cancelled".
   * @internal
   * A callback that is called when the status of the workflow changes.
   */
  async onStatusChange_experimental?(
    status: "running" | "paused" | "completed" | "failed" | "cancelled"
  ): Promise<void>;
  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    this.sql = this.ctx.storage.sql;

    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS migrations (
      	version     INTEGER NOT NULL PRIMARY KEY,
      	migrated_at REAL NOT NULL DEFAULT (CAST(unixepoch('subsecond') * 1000 AS INTEGER))
    	) STRICT`
    );

    const currentVersion =
      this.sql.exec<{ version: number | null }>("SELECT MAX(version) AS version FROM migrations").one().version ?? 0;
    // Apply any pending migrations if the current version is less than the number of migrations defined
    if (currentVersion < WorkflowRuntime.MIGRATIONS.length) {
      for (let version = currentVersion + 1; version <= WorkflowRuntime.MIGRATIONS.length; version++) {
        this.sql.exec(WorkflowRuntime.MIGRATIONS[version - 1] as string);
        this.sql.exec("INSERT INTO migrations (version) VALUES (?)", version);
      }
    } else if (currentVersion > WorkflowRuntime.MIGRATIONS.length) {
      console.error("Database migration version is ahead of the codebase. Please check your migrations.");
    }

    const [metadata] = this.sql
      .exec<WorkflowMetadata_Row<TVersion>>("SELECT * FROM workflow_metadata WHERE id = 1")
      .toArray();
    if (metadata === undefined) {
      this.sql.exec("INSERT INTO workflow_metadata (id, status) VALUES (1, ?)", "pending");
      this.sql.exec("INSERT INTO workflow_events (type) VALUES (?)", "created");
      this.#status = "pending";
    } else {
      this.#status = metadata.status;
      this.#definitionVersion = metadata.definition_version === null ? undefined : metadata.definition_version;
      this.#definitionInput =
        metadata.definition_input === null ? undefined : (JSON.parse(metadata.definition_input) as TInput);
    }
  }

  protected abstract getDefinition(
    version: TVersion
  ): (options: {
    props: { requestId: string; runtimeInstanceId: string; input: TInput };
  }) => Fetcher<WorkflowDefinition<TInput>>;

  public getStatus(): WorkflowStatus {
    return this.#status;
  }

  #setStatus(
    data:
      | { type: "running" }
      | { type: "paused" }
      | { type: "completed" }
      | { type: "failed" }
      | { type: "cancelled"; reason?: string }
  ): void {
    if (this.#status === data.type) return;

    let eventType: "started" | "resumed" | "paused" | "completed" | "failed" | "cancelled";
    switch (data.type) {
      case "running":
        eventType = this.#status === "paused" ? "resumed" : "started";
        break;
      case "paused":
      case "completed":
      case "failed":
      case "cancelled":
        eventType = data.type;
        break;
    }

    this.sql.exec(
      `UPDATE workflow_metadata
       SET status = ?,
           updated_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER)
       WHERE id = 1`,
      data.type
    );

    this.sql.exec(
      `INSERT INTO workflow_events (type, cancellation_reason) VALUES (?, ?)`,
      eventType,
      data.type === "cancelled" ? (data.reason ?? null) : null
    );

    this.#status = data.type;
  }

  /**
   * Retrieves the current state of all steps in the workflow, ordered by when each step was created.
   *
   * @returns An array containing the formatted steps for all steps in the
   * workflow.
   */
  getSteps_experimental(): Step[] {
    const steps = this.sql.exec<Step_Row>("SELECT * FROM steps ORDER BY created_at ASC").toArray();
    return steps.map((step) => formatStep(step));
  }

  /**
   * Retrieves all durable step events in the workflow, ordered by recording time.
   *
   * @returns Formatted `step_events` rows for the workflow instance.
   */
  getStepEvents_experimental(): StepEvent[] {
    const events = this.sql.exec<StepEventRow>("SELECT * FROM step_events ORDER BY recorded_at ASC");
    return events.toArray().map((row) => formatStepEvent(row));
  }

  /**
   * Retrieves all durable workflow status events, ordered by id (insertion order).
   *
   * @returns Formatted `workflow_events` rows for the workflow instance.
   */
  getWorkflowEvents_experimental(): WorkflowEvent[] {
    const events = this.sql.exec<WorkflowEventRow>("SELECT * FROM workflow_events ORDER BY id ASC");
    return events.toArray().map((row) => formatWorkflowEvent(row));
  }

  /**
   * Handles an inbound event by satisfying the first waiting wait-step for the given event name, ordered by creation
   * time. If a step is found, we mark it as satisfied and resume the workflow. Otherwise, we record the event and wait
   * for it to be satisfied. If the workflow is in a terminal state, we do not need to process the inbound event.
   *
   * @param event - The name of the event that a wait step is expected to be waiting for.
   * @param payload - The payload of the event that will be associated with the wait step if it is satisfied.
   */
  async handleInboundEvent(event: string, payload?: Json): Promise<void> {
    // If the workflow is in a terminal state, we do not need to process the inbound event.
    if (this.isTerminalStatus(this.#status)) {
      console.info(`An inbound event was received for a workflow in a terminal state: ${this.#status}`);
      return;
    }

    const serializedPayload = payload !== undefined ? JSON.stringify(payload) : null;

    // If the workflow is paused, queue the event but do not satisfy any wait step or call run().
    // The event will be picked up when the workflow is resumed and execution hits getOrCreateWaitStep.
    if (this.#status === "paused") {
      this.sql.exec(`INSERT INTO inbound_events (event_name, payload) VALUES (?, ?)`, event, serializedPayload);
      return;
    }

    /**
     * Find the first waiting wait-step for the given event name, ordered by creation time. If a step is found, we mark
     * it as satisfied and resume the workflow. Otherwise, we record the event and wait for it to be satisfied.
     */
    const [step] = this.sql
      .exec<Pick<WaitStep_Row, "id">>(
        `SELECT id
						 FROM steps
						 WHERE type = 'wait'
							 AND state = 'waiting'
							 AND event_name = ?
						 ORDER BY created_at ASC, id ASC
						 LIMIT 1`,
        event
      )
      .toArray();

    if (step !== undefined) {
      this.sql.exec(
        `UPDATE steps
							 SET state = 'satisfied',
									 payload = ?,
									 resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER),
									 timeout_at = NULL
							 WHERE id = ?
								 AND type = 'wait'
								 AND state = 'waiting'`,
        serializedPayload,
        step.id
      );

      this.sql.exec(
        `INSERT INTO step_events (step_id, type, payload)
							 VALUES (?, ?, ?)`,
        step.id,
        "wait_satisfied",
        serializedPayload
      );

      this.sql.exec(
        `INSERT INTO inbound_events (event_name, payload, claimed_by, claimed_at)
							 VALUES (?, ?, ?, CAST(unixepoch('subsecond') * 1000 AS INTEGER))`,
        event,
        serializedPayload,
        step.id
      );

      await this.run();
    } else {
      this.sql.exec(`INSERT INTO inbound_events (event_name, payload) VALUES (?, ?)`, event, serializedPayload);
    }
  }

  /**
   * Cancels the workflow. If the workflow is in a terminal state (completed, failed, or cancelled), it will return
   * early.
   *
   * @param reason - The reason for the cancellation.
   */
  async cancel(reason?: string): Promise<void> {
    if (this.isTerminalStatus(this.#status)) return;

    this.#setStatus({ type: "cancelled", reason });
    await this.ctx.storage.deleteAlarm();

    if (this.onStatusChange_experimental !== undefined) {
      await this.onStatusChange_experimental("cancelled");
    }
  }

  /**
   * Pauses the workflow. Only transitions from `running` to `paused`. If the workflow is already paused, terminal, or
   * not running (e.g. `pending`), this method is a no-op.
   */
  async pause(): Promise<void> {
    if (this.#status !== "running") return;

    await this.ctx.storage.transaction(async (transaction) => {
      this.#setStatus({ type: "paused" });
      await transaction.deleteAlarm();
    });

    if (this.onStatusChange_experimental !== undefined) {
      await this.onStatusChange_experimental("paused");
    }
  }

  /**
   * Resumes a paused workflow. Only transitions from `paused` to `running`. If the workflow is not paused, an error is
   * thrown.
   */
  async resume(): Promise<void> {
    if (this.#status !== "paused") {
      throw new Error(`Cannot resume workflow: expected status 'paused' but got '${this.#status}'.`);
    }

    this.#setStatus({ type: "running" });

    if (this.onStatusChange_experimental !== undefined) {
      await this.onStatusChange_experimental("running");
    }

    await this.run();
  }

  async alarm(_info?: AlarmInvocationInfo): Promise<void> {
    // If the workflow is in a terminal state (completed, failed, or cancelled), we do not need to continue the execution.
    if (this.isTerminalStatus(this.#status)) return;

    // If the workflow is paused, do not continue execution.
    if (this.#status === "paused") return;

    // Schedule another safety alarm if the run loop is still active.
    if (this.#isRunLoopActive) {
      await this.ctx.storage.setAlarm(Date.now() + 30_000 * 60); // 30 minutes
    } else {
      await this.run();
    }
  }

  /**
   * Creates a new workflow instance and pins the definition version. If the workflow is in a terminal state, it will
   * return early. Otherwise, it will pin the definition version and set the input. If the definition version is already
   * pinned to a different version, it will throw an error.
   *
   * @param options.definitionVersion - The version of the definition to pin to the workflow instance. This will be used
   *   to resolve the workflow definition from the `getDefinition` hook.
   * @param options.input - The input to the workflow instance. This will be passed to the workflow definition as the
   *   `input` property.
   */
  public async create(options: { definitionVersion: TVersion; input?: TInput }): Promise<void> {
    if (this.isTerminalStatus(this.#status)) return;
    if (this.#status === "paused") return;

    const version = options.definitionVersion;
    let metadata = this.sql
      .exec<Pick<WorkflowMetadata_Row<TVersion>, "definition_version" | "definition_input">>(
        "SELECT definition_version, definition_input FROM workflow_metadata WHERE id = 1"
      )
      .one();

    if (metadata.definition_version !== null && metadata.definition_version !== version) {
      throw new Error(
        `Workflow definition version is already pinned to '${metadata.definition_version}' and cannot be changed to '${version}'.`
      );
    }

    // If the workflow is not yet pinned to a definition version, we pin it to the new version and set the input.
    if (metadata.definition_version === null) {
      metadata = this.sql
        .exec<Pick<WorkflowMetadata_Row<TVersion>, "definition_version" | "definition_input">>(
          `UPDATE workflow_metadata
						SET definition_version = ?,
								definition_input = ?,
								updated_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER)
						WHERE id = 1 RETURNING definition_version, definition_input`,
          version,
          options.input ? JSON.stringify(options.input) : null
        )
        .one();
    }

    this.#definitionVersion = version;
    this.#definitionInput = metadata.definition_input ? (JSON.parse(metadata.definition_input) as TInput) : undefined;

    await this.run();
  }

  private async run(): Promise<void> {
    if (this.isTerminalStatus(this.#status)) return;
    if (this.#status === "paused") return;

    if (this.#definitionVersion === undefined) return;

    if (this.#status !== "running") {
      this.#setStatus({ type: "running" });
      this.#status = "running";

      if (this.onStatusChange_experimental !== undefined) {
        await this.onStatusChange_experimental("running");
      }
    }

    if (this.#isRunLoopActive) return;

    const requestId = crypto.randomUUID();
    const context = new WorkflowRuntimeContext(this.ctx.storage, { requestId });

    this.#isRunLoopActive = true;

    (async () => {
      try {
        /**
         * Drives the workflow forward by repeatedly calling next() on the executor.
         *
         * Each call to next() re-executes the workflow function from the top. Completed steps return their cached
         * results immediately. Per nesting level, a sibling `run()` that follows another successful sibling in the same
         * `next()` sees a full sibling budget and throws `ResumeImmediatelyError` until the next `next()`; the budget
         * increments only after a `run()` step records `succeeded`. Nested `run()` callbacks use a fresh frame.
         *
         * The loop exits when: - The workflow completes or aborts (done: true) - A step needs a delayed retry or sleep
         * (schedules an alarm and exits) - A step is waiting for an inbound event (exits with no alarm; an event
         * resumes the workflow)
         */
        while (true) {
          // If paused between iterations, exit the loop cleanly.
          if (this.#status === "paused") {
            await this.ctx.storage.deleteAlarm();
            break;
          }

          try {
            const version = this.#definitionVersion;
            if (version === undefined) {
              throw new Error(
                "Workflow definition version has not been initialized. Call 'start()' before running the workflow."
              );
            }

            const definition = this.getDefinition(version);
            const executor = definition({
              props: {
                runtimeInstanceId: this.ctx.id.toString(),
                requestId,
                input: this.#definitionInput as TInput
              }
            });

            // Schedule a watchdog alarm. A watchdog alarm is protection against loss of control around durable state transitions,
            // especially when a step has been durably marked as started but the engine has not durably recorded how to proceed next.
            await this.ctx.storage.setAlarm(Date.now() + 30_000 * 60); // 30 minutes

            const result = await executor.next(context);

            // If the workflow was cancelled while waiting for the executor to return a response, we exit the loop immediately.
            if (this.#status === "cancelled") {
              await this.ctx.storage.deleteAlarm();
              break;
            }

            // Pause can happen while `next()` is in flight. From `paused`, durable metadata may only move to `running` or
            // `cancelled`, so we must not apply terminal transitions here; `resume()` will run `next()` again.
            if (this.getStatus() === "paused") {
              await this.ctx.storage.deleteAlarm();
              break;
            }

            if (result.done) {
              await this.ctx.storage.transaction(async (transaction) => {
                this.#setStatus({ type: result.status });
                await transaction.deleteAlarm();
              });
              if (this.onStatusChange_experimental !== undefined) {
                await this.onStatusChange_experimental(result.status);
              }
              break;
            }

            // An 'immediate' resume hint indicates that the workflow should resume immediately.
            if (result.resume.type === "immediate") continue;

            // A 'suspended' resume hint indicates that the workflow should suspend itself and wait for the next alarm or inbound event to resume.
            if (result.resume.type === "suspended") break;

            break;
          } catch (error) {
            // An exception can be thrown when calling 'next()' on the executor worker.
            // The resulting exception will have a 'remote' property set to 'True' in this case.
            // In this case, the error is considered to be transient and the workflow should continue.
            if (error instanceof Error && "remote" in error && error.remote) {
              console.info(error, { requestId });
              continue;
            }

            console.error(error instanceof Error ? error : new Error(String(error)), { requestId });

            // If the workflow is in a terminal state, we do not need to process the error.
            if (this.isTerminalStatus(this.#status)) break;

            // Same as after `next()` returns: `paused` cannot transition to `failed` in the database.
            if (this.getStatus() === "paused") {
              await this.ctx.storage.deleteAlarm();
              break;
            }

            // All other errors are considered to be fatal and the workflow should be aborted.
            await this.ctx.storage.transaction(async (transaction) => {
              this.#setStatus({ type: "failed" });
              await transaction.deleteAlarm();
            });

            if (this.onStatusChange_experimental !== undefined) {
              await this.onStatusChange_experimental("failed");
            }
          }
        }
      } finally {
        this.#isRunLoopActive = false;
      }
    })();
  }

  private isTerminalStatus(status: WorkflowStatus): boolean {
    return status === "completed" || status === "failed" || status === "cancelled";
  }
}

export class WorkflowRuntimeContext extends RpcTarget {
  private readonly storage: DurableObjectStorage;
  private readonly sql: SqlStorage;
  private readonly requestId?: string;
  private static readonly BACKOFF_DELAYS = [250, 500, 1_000, 2_000, 4_000, 8_000, 10_000] as const;

  private static readonly DEFAULT_MAX_ATTEMPTS = 3;

  constructor(storage: DurableObjectStorage, options?: { requestId?: string }) {
    super();
    this.storage = storage;
    this.sql = storage.sql;
    this.requestId = options?.requestId;
  }

  public async getOrCreateStep(
    id: RunStepId,
    options: { type: "run"; maxAttempts?: number | null; parentStepId: RunStepId | null }
  ): Promise<RunStep>;
  public async getOrCreateStep(
    id: SleepStepId,
    options: { type: "sleep"; wakeAt: Date; parentStepId: RunStepId | null }
  ): Promise<SleepStep>;
  public async getOrCreateStep(
    id: WaitStepId,
    options: { type: "wait"; eventName: string; timeoutAt?: Date; parentStepId: RunStepId | null }
  ): Promise<WaitStep>;
  public async getOrCreateStep(
    id: RunStepId | SleepStepId | WaitStepId,
    options:
      | { type: "run"; maxAttempts?: number | null; parentStepId: RunStepId | null }
      | { type: "sleep"; wakeAt: Date; parentStepId: RunStepId | null }
      | { type: "wait"; eventName: string; timeoutAt?: Date; parentStepId: RunStepId | null }
  ): Promise<Step> {
    try {
      if (options.type === "run") {
        return await this.getOrCreateRunStep(id as RunStepId, {
          maxAttempts: options?.maxAttempts,
          parentStepId: options.parentStepId
        });
      } else if (options.type === "sleep") {
        return await this.getOrCreateSleepStep(id as SleepStepId, {
          wakeAt: options.wakeAt,
          parentStepId: options.parentStepId
        });
      } else {
        return await this.getOrCreateWaitStep(id as WaitStepId, {
          eventName: options.eventName,
          timeoutAt: options.timeoutAt,
          parentStepId: options.parentStepId
        });
      }
    } catch (error) {
      console.error(error instanceof Error ? error : new Error(String(error)), { requestId: this.requestId });
      if (error instanceof Error && isSqliteInvariantViolation(error.message)) {
        throw new WorkflowInvariantError(error.message);
      }

      // All other errors are considered to be infrastructure/critical errors and may cause the DO to be reset
      // The following are examples of such errors:
      // 'SQLITE_FULL',      // Database or disk is full
      // 'SQLITE_IOERR',     // I/O error
      // 'SQLITE_BUSY',      // Database is locked
      // 'SQLITE_NOMEM',     // Out of memory
      // 'SQLITE_INTERRUPT', // Operation interrupted
      // 'SQLITE_CORRUPT',   // Database file is corrupted
      // 'SQLITE_CANTOPEN',  // Cannot open database file
      throw error;
    }
  }

  /**
   * True if this run step has at least one **direct** child that still **explains** a parent left in `running`:
   * typically `run` in `running` or `pending`, `sleep`/`wait` in `waiting`, or a successful-but-not-failed child row
   * (`succeeded` run, `satisfied` wait, `elapsed` sleep) while the parent has not yet recorded its own outcome.
   *
   * Excludes only terminal **failure** child states (`failed`, `timed_out`).
   */
  public async hasRunningOrWaitingChildSteps(stepId: RunStepId): Promise<boolean> {
    const rows = this.sql
      .exec<{ x: number }>(
        `SELECT 1 AS x FROM steps
         WHERE parent_step_id = ?
           AND state NOT IN ('failed', 'timed_out')
         LIMIT 1`,
        stepId
      )
      .toArray();
    return rows.length > 0;
  }

  private async getOrCreateRunStep(
    id: RunStepId,
    options: { maxAttempts?: number | null; parentStepId: RunStepId | null }
  ): Promise<RunStep> {
    const maxAttempts =
      options.maxAttempts === undefined ? WorkflowRuntimeContext.DEFAULT_MAX_ATTEMPTS : options.maxAttempts;
    return await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql.exec<RunStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'run'", id).toArray();
      // If the step does not exist, we create it and mark the attempt as 'pending'.
      if (existing === undefined) {
        const inserted = this.sql
          .exec<RunStep_Row>(
            `INSERT INTO steps (id, type, state, attempt_count, max_attempts, next_attempt_at, parent_step_id) VALUES (?, 'run', 'pending', 0, ?, CAST(unixepoch('subsecond') * 1000 AS INTEGER), ?) RETURNING *`,
            id,
            maxAttempts,
            options.parentStepId
          )
          .one();

        return formatStep(inserted);
      } else {
        // If the step exists and is in 'pending' state, we update the alarm to wake at the correct time.
        if (existing.state === "pending" && Date.now() < existing.next_attempt_at) {
          await transaction.setAlarm(existing.next_attempt_at);
        }
        return formatStep(existing);
      }
    });
  }

  private async getOrCreateSleepStep(
    id: SleepStepId,
    options: { wakeAt: Date; parentStepId: RunStepId | null }
  ): Promise<SleepStep> {
    return await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql
        .exec<SleepStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'sleep'", id)
        .toArray();
      if (existing !== undefined) {
        // If the step exists and is in 'waiting' state, we update the alarm to wake at the correct time.
        if (existing.state === "waiting" && Date.now() < existing.wake_at) {
          await transaction.setAlarm(existing.wake_at);
        }
        return formatStep(existing);
      }

      // If the step does not exist, we create it, set it to 'waiting' state and set the alarm to wake at the correct time.
      const wakeAt = options.wakeAt.getTime();
      const inserted = this.sql
        .exec<SleepStep_Row>(
          `INSERT INTO steps (id, type, state, wake_at, parent_step_id) VALUES (?, 'sleep', 'waiting', ?, ?) RETURNING *`,
          id,
          wakeAt,
          options.parentStepId
        )
        .one();
      this.sql.exec("INSERT INTO step_events (step_id, type, wake_at) VALUES (?, ?, ?)", id, "sleep_waiting", wakeAt);
      await transaction.setAlarm(wakeAt);
      return formatStep(inserted);
    });
  }

  private async getOrCreateWaitStep(
    id: WaitStepId,
    options: { eventName: string; timeoutAt?: Date; parentStepId: RunStepId | null }
  ): Promise<WaitStep> {
    return await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql
        .exec<WaitStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'wait'", id)
        .toArray();
      // If the step exists and isn't in 'waiting' state (i.e. in terminal state of 'satisfied' or 'timed_out'), we return the step as is as no further action is needed.
      if (existing !== undefined && existing.state !== "waiting") {
        return formatStep(existing);
      }

      let waiting: Extract<WaitStep_Row, { state: "waiting" }>;
      if (existing !== undefined) {
        waiting = existing;
      } else {
        waiting = this.sql
          .exec<Extract<WaitStep_Row, { state: "waiting" }>>(
            `
						INSERT INTO steps (id, type, state, event_name, timeout_at, parent_step_id)
						VALUES (?, 'wait', 'waiting', ?, ?, ?)
						RETURNING *
						`,
            id,
            options.eventName,
            options.timeoutAt !== undefined ? options.timeoutAt.getTime() : null,
            options.parentStepId
          )
          .one();
        this.sql.exec(
          `INSERT INTO step_events (step_id, type, event_name, timeout_at) VALUES (?, ?, ?, ?)`,
          id,
          "wait_waiting",
          options.eventName,
          options.timeoutAt !== undefined ? options.timeoutAt.getTime() : null
        );
      }

      const timeoutAt = waiting.timeout_at;

      // Attempt to claim any inbound event that is not claimed yet for the given event name.
      const [event] = this.sql
        .exec<{ id: string; payload: string }>(
          `
	UPDATE inbound_events
		 SET claimed_by = ?,
				 claimed_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER)
	 WHERE id = (
		 SELECT id
			 FROM inbound_events
			WHERE event_name = ?
				AND claimed_by IS NULL
			ORDER BY created_at ASC, id ASC
			LIMIT 1
	 )
		 AND claimed_by IS NULL
	RETURNING id, payload
	`,
          id,
          options.eventName
        )
        .toArray();

      // If a queued inbound event was found, we mark the step as 'satisfied' and return the satisfied step.
      if (event !== undefined) {
        const satisfied = this.sql
          .exec<WaitStep_Row>(
            `
			UPDATE steps
				 SET state = 'satisfied',
						 payload = ?,
						 resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER),
						 timeout_at = NULL
			 WHERE id = ?
				 AND type = 'wait'
				 AND state = 'waiting'
			RETURNING *
			`,
            event.payload,
            id
          )
          .one();

        this.sql.exec(
          `
		INSERT INTO step_events (step_id, type, payload)
		VALUES (?, ?, ?)
		`,
          id,
          "wait_satisfied",
          event.payload
        );

        return formatStep(satisfied);
      }
      // If no queued inbound event was found, we return the step as is.
      else {
        if (timeoutAt !== null && Date.now() < timeoutAt) {
          await transaction.setAlarm(timeoutAt);
        } else {
          await transaction.deleteAlarm();
        }
        return formatStep(waiting);
      }
    });
  }
  async handleRunAttemptEvent(
    id: RunStepId,
    event:
      | { type: "running"; attemptCount: number }
      | { type: "succeeded"; attemptCount: number; result: string }
      | {
          type: "failed";
          attemptCount: number;
          errorMessage: string;
          errorName?: string;
          isNonRetryableStepError?: boolean;
        }
  ): Promise<void> {
    try {
      await this.storage.transaction(async (transaction) => {
        const [existing] = this.sql
          .exec<RunStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'run'", id)
          .toArray();
        if (existing === undefined) {
          throw new WorkflowInvariantError(`Step '${id}' of type 'run' not found.`);
        }

        const attemptCount = event.attemptCount;

        if (event.type === "running") {
          if (existing.state !== "pending") {
            throw new WorkflowInvariantError(
              `Unexpected state for run step '${id}'. Expected 'pending' but got ${existing.state}.`
            );
          }
          if (existing.next_attempt_at !== null && existing.next_attempt_at > Date.now()) {
            throw new WorkflowInvariantError(
              `Unexpected next attempt at for run step '${id}'. Expected a NULL value or a value that is in the past but got ${new Date(existing.next_attempt_at).toISOString()}.`
            );
          }
          if (existing.attempt_count !== attemptCount - 1) {
            throw new WorkflowInvariantError(
              `Unexpected attempt count for run step '${id}'. Expected ${attemptCount - 1} but got ${existing.attempt_count}.`
            );
          }
          // Update the step to the 'running' state and insert an `attempt_started` step_events row.
          this.sql.exec(
            `UPDATE steps SET state = 'running', attempt_count = ?, next_attempt_at = NULL WHERE id = ?`,
            attemptCount,
            id
          );
          this.sql.exec(
            "INSERT INTO step_events (step_id, type, attempt_number) VALUES (?, ?, ?)",
            id,
            "attempt_started",
            attemptCount
          );
        } else if (event.type === "succeeded") {
          if (existing.state !== "running") {
            throw new WorkflowInvariantError(
              `Unexpected state for run step '${id}'. Expected 'running' but got ${existing.state}.`
            );
          }
          if (existing.attempt_count !== attemptCount) {
            throw new WorkflowInvariantError(
              `Unexpected attempt count for run step '${id}'. Expected ${attemptCount} but got ${existing.attempt_count}.`
            );
          }
          // Update the step to the 'succeeded' state and insert an `attempt_succeeded` step_events row.
          this.sql.exec(
            `UPDATE steps SET state = 'succeeded', result = ?, resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER) WHERE id = ?`,
            event.result,
            id
          );
          this.sql.exec(
            "INSERT INTO step_events (step_id, type, attempt_number, result) VALUES (?, ?, ?, ?)",
            id,
            "attempt_succeeded",
            attemptCount,
            event.result
          );
        } else if (event.type === "failed") {
          if (existing.state !== "running") {
            throw new WorkflowInvariantError(
              `Unexpected state for run step '${id}'. Expected 'running' but got ${existing.state}.`
            );
          }

          if (existing.attempt_count !== attemptCount) {
            throw new WorkflowInvariantError(
              `Unexpected attempt count for run step '${id}'. Expected ${attemptCount} but got ${existing.attempt_count}.`
            );
          }

          // If the step has reached the maximum number of attempts, we mark the step as 'failed'
          if (
            (existing.max_attempts != null && existing.attempt_count >= existing.max_attempts) ||
            event.isNonRetryableStepError
          ) {
            this.sql.exec(
              `UPDATE steps SET state = 'failed', error_message = ?, error_name = ?, resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER) WHERE id = ?`,
              event.errorMessage,
              event.errorName ?? null,
              id
            );

            // Insert an `attempt_failed` step_events row
            this.sql.exec(
              "INSERT INTO step_events (step_id, type, attempt_number, error_message, error_name) VALUES (?, ?, ?, ?, ?)",
              id,
              "attempt_failed",
              attemptCount,
              event.errorMessage,
              event.errorName ?? null
            );
          }
          // Otherwise (if the step hasn't reached the maximum number of attempts), we mark the step as 'pending'
          // and update 'next_attempt_at' to the next backoff time and set the alarm to wake up at the same time.
          else {
            const backoff =
              WorkflowRuntimeContext.BACKOFF_DELAYS[attemptCount - 1] ??
              (WorkflowRuntimeContext.BACKOFF_DELAYS[WorkflowRuntimeContext.BACKOFF_DELAYS.length - 1] as number);
            const nextAttemptAt = Date.now() + backoff;
            this.sql.exec(`UPDATE steps SET state = 'pending', next_attempt_at = ? WHERE id = ?`, nextAttemptAt, id);
            this.sql.exec(
              "INSERT INTO step_events (step_id, type, attempt_number, error_message, error_name, next_attempt_at) VALUES (?, ?, ?, ?, ?, ?)",
              id,
              "attempt_failed",
              attemptCount,
              event.errorMessage,
              event.errorName ?? null,
              nextAttemptAt
            );
            await transaction.setAlarm(nextAttemptAt);
          }
        }
      });
    } catch (error) {
      console.error(error instanceof Error ? error : new Error(String(error)), { requestId: this.requestId });
      if (error instanceof Error && isSqliteInvariantViolation(error.message)) {
        throw new WorkflowInvariantError(error.message);
      }
      throw error;
    }
  }

  handleSleepStepEvent(id: SleepStepId, event: { type: "elapsed" }): void {
    try {
      const [existing] = this.sql
        .exec<SleepStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'sleep'", id)
        .toArray();
      if (existing === undefined) {
        throw new WorkflowInvariantError(`Step '${id}' of type 'sleep' not found.`);
      }

      if (event.type === "elapsed") {
        // Update the step to the 'elapsed' state and insert a `sleep_elapsed` step_events row.
        if (existing.state !== "waiting") {
          throw new WorkflowInvariantError(
            `Unexpected state for sleep step '${id}'. Expected 'waiting' but got ${existing.state}.`
          );
        }
        this.sql.exec(
          `UPDATE steps SET state = 'elapsed', resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER), wake_at = NULL WHERE id = ?`,
          id
        );
        this.sql.exec("INSERT INTO step_events (step_id, type) VALUES (?, ?)", id, "sleep_elapsed");
      }
    } catch (error) {
      console.error(error instanceof Error ? error : new Error(String(error)), { requestId: this.requestId });
      if (error instanceof Error && isSqliteInvariantViolation(error.message)) {
        throw new WorkflowInvariantError(error.message);
      }
      throw error;
    }
  }

  handleWaitStepEvent(id: WaitStepId, event: { type: "timed_out" }): void {
    try {
      this.storage.transactionSync(() => {
        const [existing] = this.sql
          .exec<WaitStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'wait'", id)
          .toArray();
        if (existing === undefined) {
          throw new WorkflowInvariantError(`Step '${id}' of type 'wait' not found.`);
        }
        if (existing.state !== "waiting") {
          throw new WorkflowInvariantError(
            `Unexpected state for wait step '${id}'. Expected 'waiting' but got ${existing.state}.`
          );
        }
        // If the step has a timeout and the timeout has not been reached, we throw an error to explain the state mismatch.
        if (existing.timeout_at !== null && existing.timeout_at > Date.now()) {
          throw new WorkflowInvariantError(
            `Unexpected timeout at for wait step '${id}'. Expected a NULL value or a value that is in the past but got ${new Date(existing.timeout_at).toISOString()}.`
          );
        }
        // If the step has timed out, we mark the step as 'timed_out' and insert a `wait_timed_out` step_events row.
        if (event.type === "timed_out") {
          this.sql.exec(
            "UPDATE steps SET state = 'timed_out', resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER), timeout_at = NULL WHERE id = ?",
            id
          );
          this.sql.exec("INSERT INTO step_events (step_id, type) VALUES (?, ?)", id, "wait_timed_out");
        }
      });
    } catch (error) {
      console.error(error instanceof Error ? error : new Error(String(error)), { requestId: this.requestId });
      if (error instanceof Error && isSqliteInvariantViolation(error.message)) {
        throw new WorkflowInvariantError(error.message);
      }
      throw error;
    }
  }
}

function isSqliteInvariantViolation(message: string): boolean {
  return (
    message.includes("SQLITE_CONSTRAINT") || // Constraint violation (FK, UNIQUE, CHECK, NOT NULL)
    message.includes("SQLITE_MISMATCH") || // Data type mismatch
    message.includes("SQLITE_ERROR") || // Generic SQL error (syntax, etc.)
    message.includes("SQLITE_RANGE") || // Parameter index out of range
    message.includes("SQLITE_AUTH") || // Authorization denied (e.g., accessing _cf_ tables)
    message.includes("SQLITE_TOOBIG") // String or BLOB too large
  );
}

class WorkflowInvariantError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowInvariantError";
  }
}

export type RunStepId = Brand<string, "RunStepId">;
export type SleepStepId = Brand<string, "SleepStepId">;
export type WaitStepId = Brand<string, "WaitStepId">;

type RunStep = {
  type: "run";
  id: RunStepId;
  createdAt: Date;
  attemptCount: number;
  maxAttempts: number | null;
  parentStepId: RunStepId | null;
} & (
  | {
      state: "pending";
      nextAttemptAt: Date;
    }
  | {
      state: "running";
    }
  | {
      state: "succeeded";
      result: string;
      resolvedAt: Date;
    }
  | {
      state: "failed";
      errorMessage: string;
      errorName?: string;
      resolvedAt: Date;
    }
);

type SleepStep = {
  type: "sleep";
  id: SleepStepId;
  createdAt: Date;
  parentStepId: RunStepId | null;
} & (
  | {
      state: "waiting";
      wakeAt: Date;
    }
  | {
      state: "elapsed";
      resolvedAt: Date;
    }
);

type WaitStep = {
  type: "wait";
  id: WaitStepId;
  createdAt: Date;
  eventName: string;
  parentStepId: RunStepId | null;
} & (
  | {
      state: "waiting";
      timeoutAt?: Date;
    }
  | {
      state: "satisfied";
      payload: string;
      resolvedAt: Date;
    }
  | {
      state: "timed_out";
      resolvedAt: Date;
    }
);

type Step = RunStep | SleepStep | WaitStep;

function formatStep(step: RunStep_Row): RunStep;
function formatStep(step: SleepStep_Row): SleepStep;
function formatStep(step: WaitStep_Row): WaitStep;
function formatStep(step: Step_Row): Step;
function formatStep(step: Step_Row): Step {
  switch (step.type) {
    case "run": {
      switch (step.state) {
        case "pending":
          return {
            type: "run",
            id: step.id,
            state: "pending",
            nextAttemptAt: new Date(step.next_attempt_at),
            createdAt: new Date(step.created_at),
            attemptCount: step.attempt_count,
            maxAttempts: step.max_attempts,
            parentStepId: step.parent_step_id
          } satisfies RunStep;
        case "running":
          return {
            type: "run",
            id: step.id,
            state: "running",
            createdAt: new Date(step.created_at),
            attemptCount: step.attempt_count,
            maxAttempts: step.max_attempts,
            parentStepId: step.parent_step_id
          } satisfies RunStep;
        case "succeeded":
          return {
            type: "run",
            id: step.id,
            state: "succeeded",
            result: step.result,
            resolvedAt: new Date(step.resolved_at),
            createdAt: new Date(step.created_at),
            attemptCount: step.attempt_count,
            maxAttempts: step.max_attempts,
            parentStepId: step.parent_step_id
          } satisfies RunStep;
        case "failed":
          return {
            type: "run",
            id: step.id,
            state: "failed",
            errorMessage: step.error_message,
            errorName: step.error_name ?? undefined,
            resolvedAt: new Date(step.resolved_at),
            createdAt: new Date(step.created_at),
            attemptCount: step.attempt_count,
            maxAttempts: step.max_attempts,
            parentStepId: step.parent_step_id
          } satisfies RunStep;
        default:
          throw new Error("Unexpected step state");
      }
    }
    case "sleep":
      switch (step.state) {
        case "waiting":
          return {
            type: "sleep",
            id: step.id,
            state: "waiting",
            wakeAt: new Date(step.wake_at),
            createdAt: new Date(step.created_at),
            parentStepId: step.parent_step_id
          } satisfies SleepStep;
        case "elapsed":
          return {
            type: "sleep",
            id: step.id,
            state: "elapsed",
            resolvedAt: new Date(step.resolved_at),
            createdAt: new Date(step.created_at),
            parentStepId: step.parent_step_id
          } satisfies SleepStep;
        default:
          throw new Error("Unexpected step state");
      }
    case "wait":
      switch (step.state) {
        case "waiting":
          return {
            type: "wait",
            id: step.id,
            state: "waiting",
            eventName: step.event_name,
            timeoutAt: step.timeout_at ? new Date(step.timeout_at) : undefined,
            createdAt: new Date(step.created_at),
            parentStepId: step.parent_step_id
          } satisfies WaitStep;
        case "satisfied":
          return {
            type: "wait",
            id: step.id,
            state: "satisfied",
            payload: step.payload,
            createdAt: new Date(step.created_at),
            eventName: step.event_name,
            resolvedAt: new Date(step.resolved_at),
            parentStepId: step.parent_step_id
          } satisfies WaitStep;
        case "timed_out":
          return {
            type: "wait",
            id: step.id,
            state: "timed_out",
            eventName: step.event_name,
            resolvedAt: new Date(step.resolved_at),
            createdAt: new Date(step.created_at),
            parentStepId: step.parent_step_id
          } satisfies WaitStep;
        default:
          throw new Error("Unexpected step state");
      }
    default:
      throw new Error("Unexpected step type");
  }
}

/**
 * Formatted `step_events` row for application use.
 */
type StepEvent = {
  id: string;
  stepId: string;
  recordedAt: Date;
} & (
  | {
      type: "attempt_started";
      attemptNumber: number;
    }
  | {
      type: "attempt_succeeded";
      attemptNumber: number;
      result?: string;
    }
  | {
      type: "attempt_failed";
      attemptNumber: number;
      errorMessage: string;
      errorName?: string;
      nextAttemptAt?: Date;
    }
  | {
      type: "sleep_waiting";
      wakeAt: Date;
    }
  | {
      type: "sleep_elapsed";
    }
  | {
      type: "wait_waiting";
      eventName: string;
      timeoutAt?: Date;
    }
  | {
      type: "wait_satisfied";
      payload?: string;
    }
  | {
      type: "wait_timed_out";
    }
);

function formatStepEvent(row: StepEventRow): StepEvent {
  switch (row.type) {
    case "attempt_started":
      return {
        id: row.id,
        type: "attempt_started",
        attemptNumber: row.attempt_number,
        stepId: row.step_id,
        recordedAt: new Date(row.recorded_at)
      };
    case "attempt_succeeded":
      return {
        id: row.id,
        type: "attempt_succeeded",
        attemptNumber: row.attempt_number,
        result: row.result,
        stepId: row.step_id,
        recordedAt: new Date(row.recorded_at)
      };
    case "attempt_failed":
      return {
        id: row.id,
        type: "attempt_failed",
        attemptNumber: row.attempt_number,
        errorMessage: row.error_message,
        errorName: row.error_name ?? undefined,
        nextAttemptAt: row.next_attempt_at ? new Date(row.next_attempt_at) : undefined,
        stepId: row.step_id,
        recordedAt: new Date(row.recorded_at)
      };
    case "sleep_waiting":
      return {
        id: row.id,
        type: "sleep_waiting",
        wakeAt: new Date(row.wake_at),
        stepId: row.step_id,
        recordedAt: new Date(row.recorded_at)
      };
    case "sleep_elapsed":
      return {
        id: row.id,
        type: "sleep_elapsed",
        stepId: row.step_id,
        recordedAt: new Date(row.recorded_at)
      };
    case "wait_waiting":
      return {
        id: row.id,
        type: "wait_waiting",
        eventName: row.event_name,
        timeoutAt: row.timeout_at ? new Date(row.timeout_at) : undefined,
        stepId: row.step_id,
        recordedAt: new Date(row.recorded_at)
      };
    case "wait_satisfied":
      return {
        id: row.id,
        type: "wait_satisfied",
        payload: row.payload,
        stepId: row.step_id,
        recordedAt: new Date(row.recorded_at)
      };
    case "wait_timed_out":
      return {
        id: row.id,
        type: "wait_timed_out",
        stepId: row.step_id,
        recordedAt: new Date(row.recorded_at)
      };
  }
}

/**
 * SQLite row shape for `workflow_events` (append-only workflow status transitions).
 */
type WorkflowEventRow = {
  id: number;
  recorded_at: number;
  type: "created" | "started" | "paused" | "resumed" | "completed" | "failed" | "cancelled";
  cancellation_reason: string | null;
};

/**
 * Formatted `workflow_events` row for application use.
 */
type WorkflowEvent = {
  id: number;
  recordedAt: Date;
} & (
  | { type: "created" }
  | { type: "started" }
  | { type: "paused" }
  | { type: "resumed" }
  | { type: "completed" }
  | { type: "failed" }
  | { type: "cancelled"; cancellationReason?: string }
);

function formatWorkflowEvent(row: WorkflowEventRow): WorkflowEvent {
  switch (row.type) {
    case "created":
    case "started":
    case "paused":
    case "resumed":
    case "completed":
    case "failed":
      return {
        id: row.id,
        recordedAt: new Date(row.recorded_at),
        type: row.type
      };
    case "cancelled":
      return {
        id: row.id,
        recordedAt: new Date(row.recorded_at),
        type: "cancelled",
        cancellationReason: row.cancellation_reason ?? undefined
      };
    default:
      throw new Error("Unexpected workflow event type");
  }
}

type RunStep_Row = {
  id: RunStepId;
  type: "run";
  created_at: number;
  /**
   * Enclosing run step id when this run step was created inside that run's callback; otherwise null.
   */
  parent_step_id: RunStepId | null;

  /**
   * Number of attempts that have been durably started for this step.
   *
   * Invariants:
   *
   * - `0` before the first attempt starts
   * - Incremented exactly when an attempt transitions into `running`
   * - Never decremented
   * - Unchanged while the step is pending between retries/backoff
   *
   * When greater than `0`, this is also the 1-based number of the most recently started attempt. The next attempt, if
   * one is started, will have number `attempts_started + 1`.
   */
  attempt_count: number;
  /**
   * Maximum number of attempts that can be made for this step. If not present, the step can be retried indefinitely. If
   * present, the step can be retried up to this number of times. If the step has reached the maximum number of
   * attempts, it will transition to the `failed` state.
   */
  max_attempts: number | null;
} & (
  | {
      /**
       * The step has been reached but is not yet resolved, and no attempt is currently in progress.
       *
       * This includes steps that: 1. have been reached but have not yet started their first attempt 2. are waiting
       * until a retry/backoff time 3. are eligible to run immediately.
       */
      state: "pending";

      /**
       * Earliest time at which the next attempt may start.
       *
       * Semantics: - On first creation, this is typically set to "now", meaning the step is immediately runnable. -
       * After a failed attempt with backoff, this is set to the retry time. - While the current time is before this
       * value, the step remains `pending` and no new attempt may start. - Once the current time reaches or passes this
       * value, the step becomes eligible to transition from `pending` to `running`.
       */
      next_attempt_at: number;
    }
  | {
      /**
       * An attempt was durably started, but its outcome has not yet been durably recorded.
       *
       * This does not guarantee that code is actively executing at this instant. After a restart or interruption,
       * `running` means only that a start was recorded and no durable success/failure was recorded afterward.
       */
      state: "running";
    }
  | {
      /**
       * The step completed successfully and its result was durably recorded.
       */
      state: "succeeded";

      /**
       * Serialized successful result for the step.
       */
      result: string;

      /**
       * Time at which the step became terminal by succeeding.
       */
      resolved_at: number;
    }
  | {
      /**
       * The step failed terminally and no further attempts will be made.
       */
      state: "failed";

      /**
       * Serialized failure message for the terminal failure.
       */
      error_message: string;

      /**
       * Optional serialized error class/name for the terminal failure.
       */
      error_name: string | null;

      /**
       * Time at which the step became terminal by failing.
       */
      resolved_at: number;
    }
);

type SleepStep_Row = {
  id: SleepStepId;
  type: "sleep";
  created_at: number;
  /**
   * Enclosing run step id when this sleep step was created inside that run's callback; otherwise null.
   */
  parent_step_id: RunStepId | null;
} & (
  | {
      /**
       * The sleep step has been reached and is currently in effect.
       *
       * `waiting` here means "started but not yet resolved", not "not yet started". The step remains waiting until its
       * wake time is reached.
       */
      state: "waiting";

      /**
       * Earliest time at which the sleep condition becomes satisfied.
       *
       * Before this moment, the step remains waiting. At or after this moment, the step may be marked elapsed.
       */
      wake_at: number;
    }
  | {
      /**
       * The sleep interval elapsed and that fact was durably recorded.
       */
      state: "elapsed";

      /**
       * Time at which the sleep step became terminal by completing.
       */
      resolved_at: number;
    }
);

type WaitStep_Row = {
  id: WaitStepId;
  type: "wait";
  created_at: number;
  /**
   * Enclosing run step id when this wait step was created inside that run's callback; otherwise null.
   */
  parent_step_id: RunStepId | null;

  /**
   * Name of the inbound event that can satisfy this step.
   */
  event_name: string;
} & (
  | {
      /**
       * The step has been reached, but the expected event has not yet been durably received, and no timeout failure has
       * been durably recorded.
       */
      state: "waiting";

      /**
       * Optional deadline after which the wait step may fail.
       *
       * - If omitted, the step may wait indefinitely.
       * - If present, reaching or passing this time allows the step to transition to `failed`.
       */
      timeout_at: number | null;
    }
  | {
      /**
       * The expected event was received and its payload was durably recorded.
       */
      state: "satisfied";

      /**
       * Serialized payload of the event that satisfied the wait.
       */
      payload: string;

      /**
       * Time at which the wait step became terminal by receiving the expected event.
       */
      resolved_at: number;
    }
  | {
      /**
       * The step failed because its timeout was reached before the expected event was durably recorded.
       */
      state: "timed_out";

      /**
       * Time at which the wait step became terminal by timing out.
       */
      resolved_at: number;
    }
);

/**
 * A step row is created once the workflow reaches that step. From that point on, `status` describes the step's durable
 * lifecycle state.
 */
type Step_Row = RunStep_Row | SleepStep_Row | WaitStep_Row;

/**
 * SQLite row shape for `step_events`: append-only durable transitions for steps.
 *
 * The `steps` table holds current state; `step_events` records how that state evolved.
 */
type StepEventRow = {
  id: string;

  /**
   * `steps.id` this event applies to.
   */
  step_id: RunStepId | SleepStepId | WaitStepId;

  /**
   * When this event row was persisted (`unixepoch` ms).
   */
  recorded_at: number;
} & (
  | {
      /**
       * A run step attempt was durably started.
       *
       * This corresponds to the step transitioning from `pending` → `running`.
       */
      type: "attempt_started";

      /**
       * 1-based attempt number.
       *
       * Equals the value of `attempts_started` after the transition.
       */
      attempt_number: number;
    }
  | {
      /**
       * A run step attempt completed successfully.
       *
       * The step transitioned from `running` to `succeeded`.
       */
      type: "attempt_succeeded";

      attempt_number: number;

      /**
       * Serialized result produced by the step.
       */
      result: string;
    }
  | {
      /**
       * A run step attempt failed.
       *
       * The step either: - scheduled a next attempt, or - transitioned to terminal failure.
       */
      type: "attempt_failed";

      attempt_number: number;

      error_message: string;
      error_name: string | null;

      /**
       * If present, the next attempt time.
       *
       * Absence indicates this failure was terminal.
       */
      next_attempt_at?: number;
    }
  | {
      /**
       * A sleep step began waiting.
       *
       * Corresponds to the step entering `waiting`.
       */
      type: "sleep_waiting";

      /**
       * Wake time for the sleep step.
       */
      wake_at: number;
    }
  | {
      /**
       * A sleep step completed because the wake condition became satisfied.
       */
      type: "sleep_elapsed";
    }
  | {
      /**
       * A wait step began waiting for the expected event.
       */
      type: "wait_waiting";

      event_name: string;
      timeout_at: number | null;
    }
  | {
      /**
       * A wait step was satisfied by receiving the expected event.
       */
      type: "wait_satisfied";

      payload: string;
    }
  | {
      /**
       * A wait step failed because its timeout deadline was reached.
       */
      type: "wait_timed_out";
    }
);

export type WorkflowStatus =
  | "pending" // The workflow has been created but 'run' hasn't been called yet
  | "running" // The workflow is currently executing; steps are being created/processed
  | "paused" // The workflow is paused and will not make progress until resumed
  | "completed" // The workflow completed successfully; ('Workflow.next' returned { done: true, status: "succeeded" })
  | "failed" // A step exhausted retries and the workflow aborted; ('Workflow.next' returned { done: true, status: "failed" })
  | "cancelled"; // The workflow was terminated explicitly by the user.

type WorkflowMetadata_Row<TVersion extends string> = {
  created_at: number;
  updated_at: number;
  status: WorkflowStatus;
  definition_version: TVersion | null;
  definition_input: string | null;
};

type WorkflowMetadata<TVersion extends string> = {
  createdAt: Date;
  updatedAt: Date;
  status: WorkflowStatus;
  definitionVersion?: TVersion;
  definitionInput?: Json;
};

export function formatWorkflowMetadata<TVersion extends string>(
  metadata: WorkflowMetadata_Row<TVersion>
): WorkflowMetadata<TVersion> {
  return {
    createdAt: new Date(metadata.created_at),
    updatedAt: new Date(metadata.updated_at),
    status: metadata.status,
    definitionVersion: metadata.definition_version ?? undefined,
    definitionInput: metadata.definition_input ? JSON.parse(metadata.definition_input) : undefined
  };
}
/**
 * Durable record of inbound events (`inbound_events`) that may satisfy wait steps.
 *
 * Persists delivered signals across restarts so step resolution is based on durable state rather than in-memory state.
 */
export type InboundEventRow = {
  /**
   * Unique identifier for the event.
   */
  id: string;
  /**
   * Name of the event.
   *
   * Used by wait steps to determine whether this event can satisfy them.
   */
  event_name: string;
  /**
   * Serialized payload delivered with the event.
   */
  payload: string;
  /**
   * Time the event was durably recorded.
   */
  created_at: number;
  /**
   * Step that claimed the event.
   */
  claimed_by?: RunStepId | SleepStepId | WaitStepId;
  /**
   * Time the event was claimed.
   */
  claimed_at?: number;
};
