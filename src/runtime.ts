import { DurableObject, RpcTarget } from "cloudflare:workers";
import type { WorkflowDefinition } from "./definition";
import type { Json } from "./json";
import mig000 from "./migrations/0000_initial";
import mig001 from "./migrations/0001_workflow_event_deliveries";
import type { Brand } from "./brand";

export abstract class WorkflowRuntime<TInput extends Json | undefined = Json | undefined> extends DurableObject {
  private static readonly MIGRATIONS = [mig000, mig001];
  private readonly sql: SqlStorage;
  #status: WorkflowStatus;
  #isRunLoopActive: boolean = false;
  #runRequested: boolean = false;
  #definitionInput: TInput | undefined;

  /**
   * A callback that is called when the status of the workflow changes.
   *
   * @param status - The new status of the workflow; one of "running", "paused", "completed", "failed", or "cancelled".
   * @internal
   */
  protected onStatusChange?(status: "running" | "paused" | "completed" | "failed" | "cancelled"): void;

  /**
   * Handles a durably delivered terminal workflow outcome.
   *
   * Defining this method opts the runtime into delivery for completed, failed, and cancelled workflows. Returning
   * acknowledges the event; throwing causes it to be retried. Delivery is at least once, so implementations must use
   * `event.id` to make side effects idempotent.
   */
  protected completion?(event: WorkflowCompletionEvent): Promise<void>;

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
      console.error(new Error("Database migration version is ahead of the codebase. Please check your migrations."));
    }

    const [metadata] = this.sql.exec<WorkflowMetadata_Row>("SELECT * FROM workflow_metadata WHERE id = 1").toArray();
    if (metadata === undefined) {
      this.sql.exec("INSERT INTO workflow_metadata (id, status) VALUES (1, ?)", "pending");
      this.sql.exec("INSERT INTO workflow_events (type) VALUES (?)", "created");
      this.#status = "pending";
    } else {
      this.#status = metadata.status;
      this.#definitionInput =
        metadata.definition_input === null ? undefined : (JSON.parse(metadata.definition_input) as TInput);
    }
  }

  protected abstract readonly definition: (options: {
    props: { input: TInput };
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
  ): WorkflowEventRow | undefined {
    if (this.#status === data.type) return undefined;

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

    const event = this.sql
      .exec<WorkflowEventRow>(
        `INSERT INTO workflow_events (type, cancellation_reason) VALUES (?, ?) RETURNING *`,
        eventType,
        data.type === "cancelled" ? (data.reason ?? null) : null
      )
      .one();

    return event;
  }

  /**
   * Retrieves the current state of all steps in the workflow, ordered by when each step was created.
   *
   * @returns An array containing the formatted steps for all steps in the
   * workflow.
   */
  getSteps_experimental(): Array<(RunStep & { attempts: RunStepAttempt[] }) | SleepStep | WaitStep> {
    const steps = this.sql.exec<Step_Row>("SELECT * FROM steps ORDER BY created_at ASC").toArray();
    return steps.map((step) => {
      if (step.type === "run") {
        const attempts = this.sql
          .exec<RunStepAttempt_Row>(
            `SELECT * FROM run_step_attempts WHERE step_id = ? ORDER BY started_at ASC, id ASC`,
            step.id
          )
          .toArray();
        return {
          ...formatRunStep(step),
          attempts: attempts.map((attempt) => formatRunStepAttempt(attempt))
        };
      }
      if (step.type === "sleep") {
        return formatSleepStep(step);
      } else if (step.type === "wait") {
        if (step.state === "satisfied") {
          return formatSatisfiedWaitStep(step, this.getInboundEventForWaitStep(step.id).payload);
        } else if (step.state === "timed_out") {
          return formatTimedOutWaitStep(step);
        } else {
          return formatWaitingWaitStep(step);
        }
      } else {
        throw new Error("Unexpected step type. Expected 'run', 'sleep', or 'wait'.");
      }
    });
  }

  /**
   * Loads the `inbound_events` row whose `claimed_by` is this wait step (`waitStepId`).
   *
   * @throws If no such row exists (storage invariant broken or the step is not in a satisfied state with a claim).
   */
  private getInboundEventForWaitStep<T extends Json | undefined = Json | undefined>(
    stepId: WaitStepId
  ): InboundEvent<T> {
    const [row] = this.sql
      .exec<ClaimedInboundEvent_Row>(`SELECT * FROM inbound_events WHERE claimed_by = ? LIMIT 1`, stepId)
      .toArray();
    if (row === undefined) {
      throw new Error(
        `Wait step '${stepId}' is satisfied in durable state but no inbound_events row is claimed by this step.`
      );
    }
    return {
      id: row.id,
      eventName: row.event_name,
      payload: (row.payload === null ? undefined : JSON.parse(row.payload)) as T,
      createdAt: new Date(row.created_at),
      claimedBy: row.claimed_by,
      claimedAt: new Date(row.claimed_at)
    };
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
   * Handles an inbound event by satisfying the first waiting, unexpired wait-step for the given event name, ordered by
   * creation time. If a step is found, we mark it as satisfied and resume the workflow. Otherwise, we record the event
   * and wait for it to be satisfied. If the workflow is in a terminal state, we do not need to process the inbound
   * event.
   *
   * @param event - The name of the event that a wait step is expected to be waiting for.
   * @param payload - The payload of the event that will be associated with the wait step if it is satisfied.
   */
  async handleInboundEvent(event: string, payload?: Json): Promise<void> {
    // If the workflow is in a terminal state, we do not need to process the inbound event.
    if (this.isTerminalStatus(this.#status)) {
      return;
    }

    // SQL NULL encodes `undefined` (no payload); raw JSON.stringify for everything else
    // (including JSON null, which becomes the TEXT literal 'null').
    const serializedPayload = payload === undefined ? null : JSON.stringify(payload);
    const receivedAt = Date.now();

    // If the workflow is paused, queue the event but do not satisfy any wait step or call run().
    // The event will be picked up when the workflow is resumed and execution hits getOrCreateWaitStep.
    if (this.#status === "paused") {
      this.sql.exec(
        `INSERT INTO inbound_events (event_name, payload, created_at) VALUES (?, ?, ?)`,
        event,
        serializedPayload,
        receivedAt
      );
      return;
    }

    const waitStepWasSatisfied = await this.ctx.storage.transaction(async (transaction) => {
      /**
       * Find the first waiting wait-step whose deadline had not elapsed when this event arrived, ordered by creation
       * time. If a step is found, mark it as satisfied and resume the workflow. Otherwise, record the event for a later
       * eligible wait step.
       */
      const [step] = this.sql
        .exec<Pick<WaitStep_Row, "id">>(
          `SELECT id
							 FROM steps
						 WHERE type = 'wait'
							 AND state = 'waiting'
							 AND event_name = ?
							 AND (timeout_at IS NULL OR ? < timeout_at)
						 ORDER BY created_at ASC, id ASC
						 LIMIT 1`,
          event,
          receivedAt
        )
        .toArray();

      if (step !== undefined) {
        this.sql.exec(
          `INSERT INTO inbound_events (event_name, payload, created_at, claimed_by, claimed_at)
							 VALUES (?, ?, ?, ?, CAST(unixepoch('subsecond') * 1000 AS INTEGER))`,
          event,
          serializedPayload,
          receivedAt,
          step.id
        );
        this.sql.exec(
          `UPDATE steps
							 SET state = 'satisfied',
									 resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER)
							 WHERE id = ?
								 AND type = 'wait'
								 AND state = 'waiting'`,
          step.id
        );
        // The workflow may have been paused between the wait step being satisfied and the transaction committing. If so, we do not need to set an alarm or run the workflow.
        const metadata = this.sql
          .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
          .one();
        if (metadata.status === "running") {
          await transaction.setAlarm(Date.now());
        }
        return true;
      }

      this.sql.exec(
        `INSERT INTO inbound_events (event_name, payload, created_at) VALUES (?, ?, ?)`,
        event,
        serializedPayload,
        receivedAt
      );
      return false;
    });

    if (waitStepWasSatisfied) {
      await this.run();
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

    await this.ctx.storage.transaction(async (transaction) => {
      const event = this.#setStatus({ type: "cancelled", reason });
      if (event?.type === "cancelled" && this.completion !== undefined) {
        const deliverAt = Date.now();
        this.sql.exec(
          `INSERT INTO workflow_event_deliveries (event_id, next_attempt_at) VALUES (?, ?)`,
          event.id,
          deliverAt
        );
        await transaction.setAlarm(deliverAt);
      } else {
        await transaction.deleteAlarm();
      }
    });
    this.#status = "cancelled";
    this.onStatusChange?.("cancelled");
    if (this.completion !== undefined) {
      await this.deliverCompletion();
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
    this.#status = "paused";
    this.onStatusChange?.("paused");
  }

  /**
   * Resumes a paused workflow. Only transitions from `paused` to `running`. If the workflow is not paused, an error is
   * thrown.
   */
  async resume(): Promise<void> {
    if (this.#status !== "paused") {
      throw new Error(`Cannot resume workflow: expected status 'paused' but got '${this.#status}'.`);
    }

    await this.ctx.storage.transaction(async (transaction) => {
      this.#setStatus({ type: "running" });
      await transaction.setAlarm(Date.now());
    });
    this.#status = "running";
    this.onStatusChange?.("running");

    await this.run();
  }

  private async deliverCompletion(): Promise<void> {
    const now = Date.now();

    const delivery = await this.ctx.storage.transaction(async (transaction) => {
      const [pending] = this.sql
        .exec<CompletionDelivery_Row>(
          `SELECT d.event_id, d.attempts, e.recorded_at, e.type
           FROM workflow_event_deliveries AS d
           JOIN workflow_events AS e ON e.id = d.event_id
           WHERE e.type IN ('completed', 'failed', 'cancelled')
             AND d.delivered_at IS NULL
             AND d.next_attempt_at <= ?
           ORDER BY d.event_id ASC
           LIMIT 1`,
          now
        )
        .toArray();

      if (pending === undefined) {
        const [next] = this.sql
          .exec<{ next_attempt_at: number }>(
            `SELECT next_attempt_at
             FROM workflow_event_deliveries
             WHERE delivered_at IS NULL
             ORDER BY next_attempt_at ASC
             LIMIT 1`
          )
          .toArray();

        if (next === undefined) {
          await transaction.deleteAlarm();
        } else {
          await transaction.setAlarm(next.next_attempt_at);
        }
        return undefined;
      }

      // Moving next_attempt_at into the future acts as a visibility lease. If the object is evicted after the user's
      // side effect but before acknowledgement, the alarm makes the same event eligible for another delivery.
      const leaseExpiresAt = now + 30 * 60 * 1000;
      const { attempts } = this.sql
        .exec<{ attempts: number }>(
          `UPDATE workflow_event_deliveries
           SET attempts = attempts + 1,
               next_attempt_at = ?,
               last_error = NULL
           WHERE event_id = ?
           RETURNING attempts`,
          leaseExpiresAt,
          pending.event_id
        )
        .one();
      await transaction.setAlarm(leaseExpiresAt);

      const workflowInstanceId = this.ctx.id.toString();
      return {
        eventId: pending.event_id,
        attempts,
        event: {
          id: `${workflowInstanceId}:${pending.event_id}`,
          status: pending.type,
          finishedAt: new Date(pending.recorded_at)
        }
      };
    });

    if (delivery === undefined) return;

    try {
      if (this.completion === undefined) {
        throw new Error(
          "A workflow completion delivery is pending, but the runtime no longer defines a completion handler."
        );
      }

      await this.completion(delivery.event);

      await this.ctx.storage.transaction(async (transaction) => {
        this.sql.exec(
          `UPDATE workflow_event_deliveries
           SET delivered_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER),
               last_error = NULL
           WHERE event_id = ?
             AND delivered_at IS NULL`,
          delivery.eventId
        );
        await transaction.deleteAlarm();
      });
    } catch (error) {
      const retryDelay = Math.min(60 * 60 * 1000, 1_000 * 2 ** Math.min(delivery.attempts - 1, 12));
      const retryAt = Date.now() + retryDelay;
      const message = error instanceof Error ? error : new Error(String(error));

      await this.ctx.storage.transaction(async (transaction) => {
        this.sql.exec(
          `UPDATE workflow_event_deliveries
           SET next_attempt_at = ?,
               last_error = ?
           WHERE event_id = ?
             AND delivered_at IS NULL`,
          retryAt,
          String(message),
          delivery.eventId
        );
        await transaction.setAlarm(retryAt);
      });

      console.warn(
        `Completion delivery ${delivery.event.id} attempt ${delivery.attempts} failed; retry scheduled for ${new Date(retryAt).toISOString()}: ${String(message)}`
      );
    }
  }

  async alarm(_info?: AlarmInvocationInfo): Promise<void> {
    if (this.isTerminalStatus(this.#status)) {
      await this.deliverCompletion();
      return;
    }

    // If the workflow is paused, do not continue execution.
    if (this.#status === "paused") return;

    // Firing consumes the current alarm. Install a durable fallback before handing the wake to the in-memory run loop;
    // the loop will replace it with the exact next deadline or delete it once the workflow becomes terminal.
    await this.ctx.storage.setAlarm(Date.now() + 30 * 60 * 1000); // 30 minutes
    await this.run();
  }

  /**
   * Creates a new workflow instance and pins the input. If the workflow is in a terminal state or paused, it will
   * return early. Otherwise, it will pin the input the first time the instance is initialized and start execution.
   *
   * @param input - The input to the workflow instance. This will be passed to the workflow definition as the `input`
   *   property.
   */
  public async create(...args: undefined extends TInput ? [input?: TInput] : [input: TInput]): Promise<void> {
    const input = args[0];
    if (this.isTerminalStatus(this.#status)) return;
    if (this.#status === "paused") return;

    let metadata = this.sql
      .exec<Pick<WorkflowMetadata_Row, "status" | "definition_input">>(
        "SELECT status, definition_input FROM workflow_metadata WHERE id = 1"
      )
      .one();

    // If the workflow is not yet initialized, pin the input. `undefined` is encoded as SQL NULL.
    if (metadata.status === "pending") {
      metadata = await this.ctx.storage.transaction(async (transaction) => {
        const metadata = this.sql
          .exec<Pick<WorkflowMetadata_Row, "status" | "definition_input">>(
            `UPDATE workflow_metadata
							SET status = 'initialized',
								definition_input = ?,
									updated_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER)
							WHERE id = 1
								AND status = 'pending'
							RETURNING status, definition_input`,
            input === undefined ? null : JSON.stringify(input)
          )
          .one();
        // Atomically hand newly initialized work to the alarm system. If this invocation disappears before run()
        // installs its watchdog, the due alarm starts the workflow from its persisted input.
        await transaction.setAlarm(Date.now());
        return metadata;
      });
    }

    this.#status = metadata.status;
    this.#definitionInput =
      metadata.definition_input === null ? undefined : (JSON.parse(metadata.definition_input) as TInput);

    await this.run();
  }

  private async run(): Promise<void> {
    if (this.isTerminalStatus(this.#status)) return;
    if (this.#status === "paused") return;

    if (this.#status === "pending") return;

    if (this.#status !== "running") {
      this.#setStatus({ type: "running" });
      this.#status = "running";
      this.onStatusChange?.("running");
    }

    // There is deliberately only one run-loop owner. A concurrent wake cannot start a second loop, but it must be
    // remembered so the owner replays the definition before it commits a stale terminal or suspension decision.
    if (this.#isRunLoopActive) {
      this.#runRequested = true;
      return;
    }

    let definitionRetryAttempt = 0;

    this.#runRequested = false;
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
         * The loop exits when: - The workflow completes or aborts (done: true) - Durable step state has scheduled its
         * next alarm and suspends execution - A step is waiting for an inbound event - A workflow-context transport
         * failure requests a retry.
         */
        while (true) {
          // If paused between iterations, exit the loop cleanly.
          if (this.#status === "paused") {
            break;
          }

          try {
            if (this.#status === "pending") {
              throw new Error("Workflow input has not been initialized. Call 'create()' before running the workflow.");
            }

            // Protect any durable work produced while the definition is in flight. Suspension below either installs
            // an earlier exact deadline or retains this watchdog as the eventual-progress fallback.
            await this.ctx.storage.setAlarm(Date.now() + 30 * 60 * 1000);

            const executor = this.definition({
              props: {
                input: this.#definitionInput as TInput
              }
            });

            const context = new WorkflowRuntimeContext(this.ctx.storage);

            let result: Awaited<ReturnType<WorkflowDefinition<TInput>["next"]>>;
            try {
              result = await executor.next(context);
            } catch (error) {
              // Do not actively retry an overloaded Durable Object. The watchdog set before the definition call remains
              // in place and provides a much later recovery attempt without amplifying current platform pressure.
              // `overloaded` takes precedence if the runtime supplies both `overloaded` and `retryable`.
              if (error instanceof Error && "overloaded" in error && error.overloaded === true) {
                console.warn(
                  `Workflow definition call was overloaded; deferring recovery to the watchdog: ${String(error)}`
                );
                break;
              }

              // Retry explicitly retryable Durable Object failures a few times in the current invocation. Each loop
              // creates a new executor because a stub can remain broken after an exception. If the invocation disappears
              // during the wait, or the short retry budget is exhausted, the existing watchdog remains the durable backup.
              if (error instanceof Error && "retryable" in error && error.retryable === true) {
                if (this.getStatus() !== "running") break;

                if (definitionRetryAttempt >= 3) {
                  console.warn(
                    `Workflow definition retries exhausted; deferring recovery to the watchdog: ${String(error)}`
                  );
                  break;
                }

                const retryDelay =
                  Math.min(20_000, 5_000 * 2 ** definitionRetryAttempt) + Math.floor(Math.random() * 1_000);
                definitionRetryAttempt++;
                console.warn(
                  `Workflow definition call failed; retrying in ${retryDelay}ms (attempt ${definitionRetryAttempt} of 3): ${String(error)}`
                );

                try {
                  await scheduler.wait(retryDelay);
                } catch (waitError) {
                  console.warn(
                    `Workflow definition retry wait was interrupted; deferring to the watchdog: ${String(waitError)}`
                  );
                  break;
                }

                if (this.getStatus() !== "running") break;
                continue;
              }

              throw error;
            }
            definitionRetryAttempt = 0;

            // If the workflow was cancelled while waiting for the executor to return a response, we exit the loop immediately.
            if (this.#status === "cancelled") {
              break;
            }

            // Pause can happen while `next()` is in flight. From `paused`, durable metadata may only move to `running` or
            // `cancelled`, so we must not apply terminal transitions here; `resume()` will run `next()` again.
            if (this.getStatus() === "paused") {
              break;
            }

            if (this.#runRequested) {
              this.#runRequested = false;
              continue;
            }

            if (result.done) {
              await this.ctx.storage.transaction(async (transaction) => {
                const event = this.#setStatus({ type: result.status });
                if (event !== undefined && this.completion !== undefined) {
                  const deliverAt = Date.now();
                  this.sql.exec(
                    `INSERT INTO workflow_event_deliveries (event_id, next_attempt_at) VALUES (?, ?)`,
                    event.id,
                    deliverAt
                  );
                  await transaction.setAlarm(deliverAt);
                } else {
                  await transaction.deleteAlarm();
                }
              });

              this.#status = result.status;
              this.onStatusChange?.(result.status);
              if (this.completion !== undefined) {
                await this.deliverCompletion();
              }
              break;
            }

            // An 'immediate' resume hint indicates that the workflow should resume immediately.
            if (result.resume.type === "immediate") continue;

            // A 'suspended' resume hint is control flow only. Context operations conservatively keep the earliest alarm
            // while next() is in flight. Once suspension is acknowledged, hand the watchdog off to the exact earliest
            // durable deadline unless an unexplained started attempt still needs its protection.
            if (result.resume.type === "suspended") {
              await this.ctx.storage.transaction(async (transaction) => {
                const metadata = this.sql
                  .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
                  .one();
                if (metadata.status !== "running") return;

                const schedule = this.sql
                  .exec<{
                    has_unexplained_started_attempt: number;
                    has_blocker: number;
                    recovery_at: number | null;
                  }>(
                    `WITH RECURSIVE
                       latest_attempt AS (
                         SELECT a.*
                         FROM run_step_attempts a
                         WHERE NOT EXISTS (
                           SELECT 1
                           FROM run_step_attempts a2
                           WHERE a2.step_id = a.step_id
                             AND (
                               a2.started_at > a.started_at
                               OR (a2.started_at = a.started_at AND a2.id > a.id)
                             )
                         )
                       ),
                       blocker(id, recovery_at) AS (
                         SELECT id, target_wake_at
                         FROM steps
                         WHERE type = 'sleep' AND state = 'waiting'

                         UNION ALL

                         SELECT id, timeout_at
                         FROM steps
                         WHERE type = 'wait' AND state = 'waiting'

                         UNION ALL

                         SELECT step_id, next_attempt_at
                         FROM latest_attempt
                         WHERE state = 'failed' AND next_attempt_at IS NOT NULL
                       ),
                       explained_started(id) AS (
                         SELECT p.id
                         FROM blocker b
                         JOIN steps s ON s.id = b.id
                         JOIN steps p ON p.id = s.parent_step_id
                         JOIN latest_attempt pa ON pa.step_id = p.id AND pa.state = 'started'

                         UNION

                         SELECT p.id
                         FROM explained_started e
                         JOIN steps s ON s.id = e.id
                         JOIN steps p ON p.id = s.parent_step_id
                         JOIN latest_attempt pa ON pa.step_id = p.id AND pa.state = 'started'
                       )
                     SELECT
                       EXISTS (
                         SELECT 1
                         FROM latest_attempt a
                         WHERE a.state = 'started'
                           AND a.step_id NOT IN (SELECT id FROM explained_started)
                       ) AS has_unexplained_started_attempt,
                       EXISTS (SELECT 1 FROM blocker) AS has_blocker,
                       MIN(recovery_at) AS recovery_at
                     FROM blocker`
                  )
                  .one();
                const currentAlarm = await transaction.getAlarm();
                if (currentAlarm !== null && currentAlarm <= Date.now()) return;

                if (schedule.has_unexplained_started_attempt !== 0 || schedule.has_blocker === 0) {
                  const watchdogAt = Date.now() + 30 * 60 * 1000;
                  if (currentAlarm === null || watchdogAt < currentAlarm) {
                    await transaction.setAlarm(watchdogAt);
                  }
                } else if (schedule.recovery_at === null) {
                  await transaction.deleteAlarm();
                } else {
                  await transaction.setAlarm(schedule.recovery_at);
                }
              });
              break;
            }

            // A workflow-context transport failure may have happened before or after a context transaction committed.
            // Schedule the transport retry only when it is earlier than the alarm already installed by durable state.
            if (result.resume.type === "retry") {
              const retryAt = result.resume.retryAt;
              await this.ctx.storage.transaction(async (transaction) => {
                const currentAlarm = await transaction.getAlarm();
                if (currentAlarm === null || retryAt < currentAlarm) {
                  await transaction.setAlarm(retryAt);
                }
              });
              break;
            }

            break;
          } catch (error) {
            console.error(error instanceof Error ? error : new Error(String(error)));

            // If the workflow is in a terminal state, we do not need to process the error.
            if (this.isTerminalStatus(this.#status)) break;

            // Same as after `next()` returns: `paused` cannot transition to `failed` in the database.
            if (this.getStatus() === "paused") {
              break;
            }

            // All other errors are considered to be fatal and the workflow should be aborted.
            await this.ctx.storage.transaction(async (transaction) => {
              const event = this.#setStatus({ type: "failed" });
              if (event?.type === "failed" && this.completion !== undefined) {
                const deliverAt = Date.now();
                this.sql.exec(
                  `INSERT INTO workflow_event_deliveries (event_id, next_attempt_at) VALUES (?, ?)`,
                  event.id,
                  deliverAt
                );
                await transaction.setAlarm(deliverAt);
              } else {
                await transaction.deleteAlarm();
              }
            });
            this.#status = "failed";
            this.onStatusChange?.("failed");
            if (this.completion !== undefined) {
              await this.deliverCompletion();
            }
            break;
          }
        }
      } finally {
        this.#isRunLoopActive = false;
        if (this.#runRequested) {
          this.#runRequested = false;
          void this.run();
        }
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
  private static readonly BACKOFF_DELAYS = [250, 500, 1_000, 2_000, 4_000, 8_000, 10_000] as const;

  private static readonly DEFAULT_MAX_ATTEMPTS = 3;

  constructor(storage: DurableObjectStorage) {
    super();
    this.storage = storage;
    this.sql = storage.sql;
  }

  /**
   * Loads the `inbound_events` row whose `claimed_by` is this wait step (`waitStepId`).
   *
   * @throws If no such row exists (storage invariant broken or the step is not in a satisfied state with a claim).
   */
  private getInboundEventForWaitStep<T extends Json | undefined>(stepId: WaitStepId): InboundEvent<T> {
    const [row] = this.sql
      .exec<ClaimedInboundEvent_Row>(`SELECT * FROM inbound_events WHERE claimed_by = ? LIMIT 1`, stepId)
      .toArray();
    if (row === undefined) {
      throw new Error(
        `Wait step '${stepId}' is satisfied in durable state but no inbound_events row is claimed by this step.`
      );
    }
    return {
      id: row.id,
      eventName: row.event_name,
      payload: (row.payload === null ? undefined : JSON.parse(row.payload)) as T,
      createdAt: new Date(row.created_at),
      claimedBy: row.claimed_by,
      claimedAt: new Date(row.claimed_at)
    };
  }

  async getOrCreateRunStep(
    id: RunStepId,
    options: {
      maxAttempts?: number | null;
      parentStepId: RunStepId | null;
    }
  ): Promise<RunStep & { attempts: RunStepAttempt[] }> {
    return await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql.exec<RunStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'run'", id).toArray();
      if (existing === undefined) {
        const inserted = this.sql
          .exec<RunStep_Row>(
            `INSERT INTO steps (id, type, parent_step_id, max_attempts) VALUES (?, 'run', ?, ?) RETURNING *`,
            id,
            options.parentStepId,
            options.maxAttempts ?? WorkflowRuntimeContext.DEFAULT_MAX_ATTEMPTS
          )
          .one();
        return { ...formatRunStep(inserted), attempts: [] };
      } else {
        const attempts = this.sql
          .exec<RunStepAttempt_Row>(
            `SELECT * FROM run_step_attempts WHERE step_id = ? ORDER BY started_at ASC, id ASC`,
            id
          )
          .toArray();
        const lastAttempt = attempts[attempts.length - 1];
        if (lastAttempt?.state === "failed" && lastAttempt.next_attempt_at !== null) {
          const recoveryAt = lastAttempt.next_attempt_at;
          const metadata = this.sql
            .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
            .one();
          if (metadata.status === "running") {
            const currentAlarm = await transaction.getAlarm();
            if (currentAlarm === null || recoveryAt < currentAlarm) {
              await transaction.setAlarm(recoveryAt);
            }
          }
        }

        return {
          ...formatRunStep(existing),
          attempts: attempts.map((attempt) => formatRunStepAttempt(attempt))
        };
      }
    });
  }

  async getOrCreateSleepStep(
    id: SleepStepId,
    options: { wakeAt: Date; parentStepId: RunStepId | null }
  ): Promise<SleepStep> {
    return await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql
        .exec<SleepStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'sleep'", id)
        .toArray();
      if (existing !== undefined) {
        if (existing.state === "waiting") {
          const recoveryAt = existing.target_wake_at;
          const metadata = this.sql
            .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
            .one();
          if (metadata.status === "running") {
            const currentAlarm = await transaction.getAlarm();
            if (currentAlarm === null || recoveryAt < currentAlarm) {
              await transaction.setAlarm(recoveryAt);
            }
          }
        }
        return formatSleepStep(existing);
      }

      const wakeAt = options.wakeAt.getTime();
      const inserted = this.sql
        .exec<SleepStep_Row>(
          `INSERT INTO steps (id, type, state, target_wake_at, parent_step_id) VALUES (?, 'sleep', 'waiting', ?, ?) RETURNING *`,
          id,
          wakeAt,
          options.parentStepId
        )
        .one();
      const metadata = this.sql
        .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
        .one();
      if (metadata.status === "running") {
        const currentAlarm = await transaction.getAlarm();
        if (currentAlarm === null || wakeAt < currentAlarm) {
          await transaction.setAlarm(wakeAt);
        }
      }
      return formatSleepStep(inserted);
    });
  }

  async getOrCreateWaitStep<T extends Json | undefined>(
    id: WaitStepId,
    options: { eventName: string; timeoutAt?: Date; parentStepId: RunStepId | null }
  ): Promise<WaitStep<T>> {
    return await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql
        .exec<WaitStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'wait'", id)
        .toArray();
      // If the step exists and isn't in 'waiting' state (i.e. in terminal state of 'satisfied' or 'timed_out'), we return the step as is as no further action is needed.
      if (existing !== undefined && existing.state !== "waiting") {
        if (existing.state === "satisfied") {
          return formatSatisfiedWaitStep<T>(existing, this.getInboundEventForWaitStep<T>(existing.id).payload);
        } else if (existing.state === "timed_out") {
          return formatTimedOutWaitStep(existing);
        }
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
      }

      // The durable row pins the event name and deadline across replays. Only claim an event that arrived before that
      // deadline; alarm delivery may occur after the deadline and must not let a later event win the race.
      const [claimed] = this.sql
        .exec<{ id: string; payload: string | null }>(
          `
	UPDATE inbound_events
		 SET claimed_by = ?,
				 claimed_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER)
	 WHERE id = (
		 SELECT id
			 FROM inbound_events
			WHERE event_name = ?
				AND claimed_by IS NULL
				AND (? IS NULL OR created_at < ?)
			ORDER BY created_at ASC, id ASC
			LIMIT 1
	 )
		 AND claimed_by IS NULL
	RETURNING id, payload
	`,
          id,
          waiting.event_name,
          waiting.timeout_at,
          waiting.timeout_at
        )
        .toArray();

      if (claimed !== undefined) {
        const satisfied = this.sql
          .exec<SatisfiedWaitStep_Row>(
            `
			UPDATE steps
				 SET state = 'satisfied',
						 resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER)
			 WHERE id = ?
				 AND type = 'wait'
				 AND state = 'waiting'
			RETURNING *
			`,
            id
          )
          .one();
        const recoveryAt = Date.now();
        const metadata = this.sql
          .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
          .one();
        if (metadata.status === "running") {
          const currentAlarm = await transaction.getAlarm();
          if (currentAlarm === null || recoveryAt < currentAlarm) {
            await transaction.setAlarm(recoveryAt);
          }
        }
        return formatSatisfiedWaitStep<T>(
          satisfied,
          claimed.payload === null ? undefined : JSON.parse(claimed.payload)
        );
      }

      if (waiting.timeout_at !== null) {
        // Alarm timestamps must be positive. A persisted deadline at the Unix epoch is already due, so use an
        // immediate positive timestamp for recovery while preserving the original durable timeout_at value.
        const recoveryAt = Math.max(waiting.timeout_at, Date.now(), 1);
        const metadata = this.sql
          .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
          .one();
        if (metadata.status === "running") {
          const currentAlarm = await transaction.getAlarm();
          if (currentAlarm === null || recoveryAt < currentAlarm) {
            await transaction.setAlarm(recoveryAt);
          }
        }
      }
      return formatWaitingWaitStep(waiting);
    });
  }

  /**
   * True if this run step has at least one **direct** child that is still in progress.
   */
  hasInProgressChildSteps(stepId: RunStepId): boolean {
    const rows = this.sql
      .exec<{ x: number }>(
        `SELECT 1 AS x FROM steps c
         WHERE c.parent_step_id = ?
           AND (
             (c.type = 'sleep' AND c.state IN ('waiting', 'elapsed'))
             OR (c.type = 'wait' AND c.state IN ('waiting', 'satisfied'))
             OR (
               c.type = 'run'
               AND NOT EXISTS (
                 SELECT 1
                 FROM run_step_attempts a
                 WHERE a.step_id = c.id
                   AND a.state = 'failed'
                   AND a.next_attempt_at IS NULL
                   AND a.id = (
                     SELECT a2.id
                     FROM run_step_attempts a2
                     WHERE a2.step_id = c.id
                     ORDER BY a2.started_at DESC, a2.id DESC
                     LIMIT 1
                   )
               )
             )
           )
         LIMIT 1`,
        stepId
      )
      .toArray();
    return rows.length > 0;
  }

  handleRunAttemptStarted(stepId: RunStepId): StartedRunStepAttempt {
    // If a run step with the given id does not exist, we throw a 'WorkflowInvariantError' indicating that the step was not found.
    const [existing] = this.sql
      .exec<RunStep_Row>(`SELECT * FROM steps WHERE id = ? AND type = 'run'`, stepId)
      .toArray();
    if (existing === undefined) {
      throw new Error(`Run step '${stepId}' not found.`);
    }

    // Get the last attempt for the step.
    const [lastAttempt] = this.sql
      .exec<RunStepAttempt_Row>(
        "SELECT * FROM run_step_attempts WHERE step_id = ? ORDER BY started_at DESC, id DESC",
        stepId
      )
      .toArray();

    // If the last attempt has been started, we throw a 'WorkflowInvariantError' indicating that the attempt is already in progress.
    if (lastAttempt !== undefined && lastAttempt.state === "started") {
      throw new Error(`Attempt '${lastAttempt.id}' for run step '${stepId}' is already in progress.`);
    }

    // Insert a new attempt for the step and return the new attempt
    const attempt = this.sql
      .exec<StartedRunStepAttempt_Row>(
        `INSERT INTO run_step_attempts (step_id, state) VALUES (?, 'started') RETURNING *`,
        stepId
      )
      .one();
    return formatRunStepAttempt(attempt);
  }

  /**
   * Marks the identified in-flight attempt as failed.
   *
   * @param attemptId - The attempt that produced this outcome. This fences a delayed response from mutating a newer
   *   attempt for the same step.
   */
  async handleRunAttemptFailed(
    stepId: RunStepId,
    attemptId: RunStepAttemptId,
    result: {
      errorMessage: string;
      errorName?: string;
      isNonRetryableStepError?: boolean;
    }
  ): Promise<FailedRunStepAttempt> {
    return await this.storage.transaction(async (transaction) => {
      // If a run step with the given id does not exist, we throw a 'WorkflowInvariantError' indicating that the step was not found.
      const [existing] = this.sql
        .exec<RunStep_Row>(`SELECT * FROM steps WHERE id = ? AND type = 'run'`, stepId)
        .toArray();
      if (existing === undefined) {
        throw new Error(`Run step '${stepId}' not found.`);
      }

      this.assertRunAttemptIsInProgress(stepId, attemptId);

      const attempts = this.sql
        .exec<RunStepAttempt_Row>("SELECT * FROM run_step_attempts WHERE step_id = ?", stepId)
        .toArray();

      if (
        result.isNonRetryableStepError ||
        (existing.max_attempts !== null && attempts.length >= existing.max_attempts)
      ) {
        const updated = this.sql
          .exec<Extract<RunStepAttempt_Row, { state: "failed" }>>(
            `UPDATE run_step_attempts SET state = 'failed', ended_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER), error_message = ?, error_name = ?, next_attempt_at = NULL WHERE id = ? AND step_id = ? AND state = 'started' RETURNING *`,
            result.errorMessage,
            result.errorName ?? null,
            attemptId,
            stepId
          )
          .one();
        const recoveryAt = Date.now();
        const metadata = this.sql
          .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
          .one();
        if (metadata.status === "running") {
          const currentAlarm = await transaction.getAlarm();
          if (currentAlarm === null || recoveryAt < currentAlarm) {
            await transaction.setAlarm(recoveryAt);
          }
        }
        return formatRunStepAttempt(updated);
      } else {
        const backoff =
          WorkflowRuntimeContext.BACKOFF_DELAYS[attempts.length - 1] ??
          (WorkflowRuntimeContext.BACKOFF_DELAYS[WorkflowRuntimeContext.BACKOFF_DELAYS.length - 1] as number);
        const nextAttemptAt = Date.now() + backoff;

        const updated = this.sql
          .exec<Extract<RunStepAttempt_Row, { state: "failed" }>>(
            `UPDATE run_step_attempts SET state = 'failed', ended_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER), error_message = ?, error_name = ?, next_attempt_at = ? WHERE id = ? AND step_id = ? AND state = 'started' RETURNING *`,
            result.errorMessage,
            result.errorName ?? null,
            nextAttemptAt,
            attemptId,
            stepId
          )
          .one();
        const metadata = this.sql
          .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
          .one();
        if (metadata.status === "running") {
          const currentAlarm = await transaction.getAlarm();
          if (currentAlarm === null || nextAttemptAt < currentAlarm) {
            await transaction.setAlarm(nextAttemptAt);
          }
        }
        return formatRunStepAttempt(updated);
      }
    });
  }

  /**
   * Marks the identified in-flight attempt as succeeded.
   *
   * @param attemptId - The attempt that produced this outcome. This fences a delayed response from mutating a newer
   *   attempt for the same step.
   * @param resultJson - Raw JSON string for the result value (`null` when the callback returned `undefined`). The
   *   `result_type` discriminator is derived: `null` → `'none'`, non-null → `'json'`.
   */
  async handleRunAttemptSucceeded(
    stepId: RunStepId,
    attemptId: RunStepAttemptId,
    resultJson: string | null
  ): Promise<SucceededRunStepAttempt> {
    return await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql
        .exec<RunStep_Row>(`SELECT * FROM steps WHERE id = ? AND type = 'run'`, stepId)
        .toArray();
      if (existing === undefined) {
        throw new Error(`Run step '${stepId}' not found.`);
      }

      this.assertRunAttemptIsInProgress(stepId, attemptId);

      const resultType = resultJson === null ? "none" : "json";
      const updated = this.sql
        .exec<SucceededRunStepAttempt_Row>(
          `UPDATE run_step_attempts SET state = 'succeeded', ended_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER), result_type = ?, result_json = ? WHERE id = ? AND step_id = ? AND state = 'started' RETURNING *`,
          resultType,
          resultJson,
          attemptId,
          stepId
        )
        .one();
      const recoveryAt = Date.now();
      const metadata = this.sql
        .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
        .one();
      if (metadata.status === "running") {
        const currentAlarm = await transaction.getAlarm();
        if (currentAlarm === null || recoveryAt < currentAlarm) {
          await transaction.setAlarm(recoveryAt);
        }
      }
      return formatRunStepAttempt(updated);
    });
  }

  /**
   * Validates the attempt token before applying an outcome. Selecting by both ids prevents a token from one step from
   * being used for another; requiring `started` rejects delayed outcomes after that attempt has already ended.
   */
  private assertRunAttemptIsInProgress(stepId: RunStepId, attemptId: RunStepAttemptId): void {
    const [attempt] = this.sql
      .exec<RunStepAttempt_Row>("SELECT * FROM run_step_attempts WHERE id = ? AND step_id = ?", attemptId, stepId)
      .toArray();
    if (attempt === undefined) {
      throw new Error(`Attempt '${attemptId}' for run step '${stepId}' not found.`);
    }
    if (attempt.state !== "started") {
      throw new Error(
        `Attempt '${attemptId}' for run step '${stepId}' is not in progress; its state is '${attempt.state}'.`
      );
    }
  }

  async handleSleepStepElapsed(id: SleepStepId): Promise<void> {
    await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql
        .exec<SleepStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'sleep'", id)
        .toArray();
      if (existing === undefined) {
        throw new Error(`Step '${id}' of type 'sleep' not found.`);
      }

      if (existing.state !== "waiting") {
        throw new Error(`Unexpected state for sleep step '${id}'. Expected 'waiting' but got ${existing.state}.`);
      }
      this.sql.exec(
        `UPDATE steps SET state = 'elapsed', resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER) WHERE id = ?`,
        id
      );
      const recoveryAt = Date.now();
      const metadata = this.sql
        .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
        .one();
      if (metadata.status === "running") {
        const currentAlarm = await transaction.getAlarm();
        if (currentAlarm === null || recoveryAt < currentAlarm) {
          await transaction.setAlarm(recoveryAt);
        }
      }
    });
  }

  async handleWaitStepTimedOut(id: WaitStepId): Promise<void> {
    await this.storage.transaction(async (transaction) => {
      const [existing] = this.sql
        .exec<WaitStep_Row>("SELECT * FROM steps WHERE id = ? AND type = 'wait'", id)
        .toArray();
      if (existing === undefined) {
        throw new Error(`Step '${id}' of type 'wait' not found.`);
      }
      if (existing.state !== "waiting") {
        throw new Error(`Unexpected state for wait step '${id}'. Expected 'waiting' but got ${existing.state}.`);
      }
      if (existing.timeout_at !== null && existing.timeout_at > Date.now()) {
        throw new Error(
          `Unexpected timeout at for wait step '${id}'. Expected a NULL value or a value that is in the past but got ${new Date(existing.timeout_at).toISOString()}.`
        );
      }
      this.sql.exec(
        "UPDATE steps SET state = 'timed_out', resolved_at = CAST(unixepoch('subsecond') * 1000 AS INTEGER) WHERE id = ?",
        id
      );
      const recoveryAt = Date.now();
      const metadata = this.sql
        .exec<Pick<WorkflowMetadata_Row, "status">>("SELECT status FROM workflow_metadata WHERE id = 1")
        .one();
      if (metadata.status === "running") {
        const currentAlarm = await transaction.getAlarm();
        if (currentAlarm === null || recoveryAt < currentAlarm) {
          await transaction.setAlarm(recoveryAt);
        }
      }
    });
  }
}

export type RunStepId = Brand<string, "RunStepId">;
export type RunStepAttemptId = Brand<string, "RunStepAttemptId">;
export type SleepStepId = Brand<string, "SleepStepId">;
export type WaitStepId = Brand<string, "WaitStepId">;

export type RunStepAttempt = {
  id: RunStepAttemptId;
  stepId: RunStepId;
  startedAt: Date;
} & (
  | { state: "started" }
  | ({ state: "succeeded"; endedAt: Date } & ({ resultType: "json"; resultJson: string } | { resultType: "none" }))
  | { state: "failed"; errorMessage: string; errorName?: string; endedAt: Date; nextAttemptAt?: Date }
);

export type StartedRunStepAttempt = Extract<RunStepAttempt, { state: "started" }>;
export type SucceededRunStepAttempt = Extract<RunStepAttempt, { state: "succeeded" }>;
export type FailedRunStepAttempt = Extract<RunStepAttempt, { state: "failed" }>;

export type RunStep = {
  type: "run";
  id: RunStepId;
  createdAt: Date;
  maxAttempts: number | null;
  parentStepId: RunStepId | null;
};

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

type WaitStep<T extends Json | undefined = Json | undefined> = {
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
      payload: T;
      resolvedAt: Date;
      timeoutAt?: Date;
    }
  | {
      state: "timed_out";
      resolvedAt: Date;
      timeoutAt: Date;
    }
);

type WaitingWaitStep = Extract<WaitStep, { state: "waiting" }>;
type SatisfiedWaitStep<T extends Json | undefined> = Extract<WaitStep<T>, { state: "satisfied" }>;
type TimedOutWaitStep = Extract<WaitStep, { state: "timed_out" }>;

/**
 * SQLite row shape for `run_step_attempts`.
 *
 * Succeeded attempts use a `result_type` discriminator:
 *
 * - `'json'` → `result_json` holds the raw JSON value (never NULL)
 * - `'none'` → callback returned `undefined`/`void`; no result data
 */
type RunStepAttempt_Row = {
  id: RunStepAttemptId;
  step_id: RunStepId;
  started_at: number;
} & (
  | {
      state: "started";
    }
  | ({
      state: "succeeded";
      ended_at: number;
    } & ({ result_type: "json"; result_json: string } | { result_type: "none" }))
  | {
      state: "failed";
      error_message: string;
      error_name: string | null;
      ended_at: number;
      next_attempt_at: number | null;
    }
);

type StartedRunStepAttempt_Row = Extract<RunStepAttempt_Row, { state: "started" }>;
type SucceededRunStepAttempt_Row = Extract<RunStepAttempt_Row, { state: "succeeded" }>;
type FailedRunStepAttempt_Row = Extract<RunStepAttempt_Row, { state: "failed" }>;

function formatRunStepAttempt(attempt: StartedRunStepAttempt_Row): StartedRunStepAttempt;
function formatRunStepAttempt(attempt: SucceededRunStepAttempt_Row): SucceededRunStepAttempt;
function formatRunStepAttempt(attempt: FailedRunStepAttempt_Row): FailedRunStepAttempt;
function formatRunStepAttempt(attempt: RunStepAttempt_Row): RunStepAttempt;
function formatRunStepAttempt(attempt: RunStepAttempt_Row): RunStepAttempt {
  switch (attempt.state) {
    case "started":
      return {
        id: attempt.id,
        stepId: attempt.step_id,
        startedAt: new Date(attempt.started_at),
        state: "started"
      };
    case "succeeded": {
      const base = {
        id: attempt.id,
        stepId: attempt.step_id,
        startedAt: new Date(attempt.started_at),
        state: "succeeded" as const,
        endedAt: new Date(attempt.ended_at)
      };
      if (attempt.result_type === "json") {
        return { ...base, resultType: "json" as const, resultJson: attempt.result_json };
      }
      return { ...base, resultType: attempt.result_type };
    }
    case "failed":
      return {
        id: attempt.id,
        stepId: attempt.step_id,
        startedAt: new Date(attempt.started_at),
        state: "failed",
        errorMessage: attempt.error_message,
        errorName: attempt.error_name ?? undefined,
        endedAt: new Date(attempt.ended_at),
        nextAttemptAt: attempt.next_attempt_at != null ? new Date(attempt.next_attempt_at) : undefined
      };
  }
}

function formatRunStep(step: RunStep_Row): RunStep {
  return {
    type: "run",
    id: step.id,
    createdAt: new Date(step.created_at),
    maxAttempts: step.max_attempts,
    parentStepId: step.parent_step_id
  };
}

function formatSleepStep(step: SleepStep_Row): SleepStep {
  switch (step.state) {
    case "waiting":
      return {
        type: "sleep",
        id: step.id,
        state: "waiting",
        wakeAt: new Date(step.target_wake_at),
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
      throw new Error("Unexpected sleep step state");
  }
}

function formatWaitingWaitStep(step: WaitingWaitStep_Row): WaitingWaitStep {
  return {
    type: "wait",
    id: step.id,
    state: "waiting",
    eventName: step.event_name,
    timeoutAt: step.timeout_at != null ? new Date(step.timeout_at) : undefined,
    createdAt: new Date(step.created_at),
    parentStepId: step.parent_step_id
  };
}

function formatTimedOutWaitStep(step: TimedOutWaitStep_Row): TimedOutWaitStep {
  return {
    type: "wait",
    id: step.id,
    state: "timed_out",
    eventName: step.event_name,
    resolvedAt: new Date(step.resolved_at),
    createdAt: new Date(step.created_at),
    parentStepId: step.parent_step_id,
    timeoutAt: new Date(step.timeout_at)
  };
}

function formatSatisfiedWaitStep<T extends Json | undefined>(
  step: SatisfiedWaitStep_Row,
  payload: T
): SatisfiedWaitStep<T> {
  return {
    type: "wait",
    id: step.id,
    state: "satisfied",
    payload: payload,
    createdAt: new Date(step.created_at),
    eventName: step.event_name,
    resolvedAt: new Date(step.resolved_at),
    parentStepId: step.parent_step_id,
    timeoutAt: step.timeout_at != null ? new Date(step.timeout_at) : undefined
  };
}

/**
 * A durably delivered notification that a workflow reached a terminal status.
 *
 * The same event can be delivered more than once. `id` is stable across attempts and should be used as the idempotency
 * key for side effects performed by the completion handler.
 */
export type WorkflowCompletionEvent = {
  id: string;
  status: "completed" | "failed" | "cancelled";
  finishedAt: Date;
};

type CompletionDelivery_Row = {
  event_id: number;
  attempts: number;
  recorded_at: number;
  type: WorkflowCompletionEvent["status"];
};

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
   * Maximum number of attempts that can be made for this step. If not present, the step can be retried indefinitely. If
   * present, the step can be retried up to this number of times. If the step has reached the maximum number of
   * attempts, it will transition to the `failed` state.
   */
  max_attempts: number | null;
};

type SleepStep_Row = {
  id: SleepStepId;
  type: "sleep";
  created_at: number;
  /**
   * Id of the enclosing run step if this sleep step was created from within that run step's callback; otherwise null.
   */
  parent_step_id: RunStepId | null;
} & (
  | {
      /**
       * The sleep step has started and is not yet terminal.
       *
       * In this state, the step is considered active until its target wake time is reached and that transition is
       * durably recorded.
       */
      state: "waiting";

      /**
       * Target time at or after which this sleep step may transition to `elapsed`.
       *
       * This is part of the step's configured behavior, not an indicator that the step is still pending.
       */
      target_wake_at: number;
    }
  | {
      /**
       * The sleep step has completed because its target wake time was reached and that outcome was durably recorded.
       */
      state: "elapsed";

      /**
       * Target time at or after which this sleep step became eligible to transition to `elapsed`.
       *
       * Retained on terminal rows so the completed step still carries its original timing configuration.
       */
      target_wake_at: number;

      /**
       * Time at which the transition to `elapsed` was durably recorded.
       *
       * This may be equal to or later than `target_wake_at`.
       */
      resolved_at: number;
    }
);

type WaitStep_Row = {
  id: WaitStepId;
  type: "wait";
  created_at: number;
  /**
   * Id of the enclosing run step if this wait step was created from within that run step's callback; otherwise null.
   */
  parent_step_id: RunStepId | null;

  /**
   * Name of the inbound event that can satisfy this wait step.
   */
  event_name: string;
} & (
  | {
      /**
       * The wait step has started and is not yet terminal.
       *
       * In this state, the expected event has not yet been durably matched to the step, and no timeout outcome has been
       * durably recorded.
       */
      state: "waiting";

      /**
       * Optional time at or after which this wait step may transition to `timed_out`.
       *
       * - If null, the step may wait indefinitely.
       * - If set, reaching or passing this time makes the step eligible to time out.
       */
      timeout_at: number | null;
    }
  | {
      /**
       * The wait step has completed because a matching event was durably received and recorded.
       */
      state: "satisfied";

      /**
       * Optional timeout that applied while this step was active.
       *
       * Retained on terminal rows so the completed step still carries its original timing configuration.
       */
      timeout_at: number | null;

      /**
       * Time at which the transition to `satisfied` was durably recorded.
       */
      resolved_at: number;
    }
  | {
      /**
       * The wait step has completed by timing out before a matching event was durably received.
       */
      state: "timed_out";

      /**
       * Time at or after which this step became eligible to transition to `timed_out`.
       */
      timeout_at: number;

      /**
       * Time at which the transition to `timed_out` was durably recorded.
       *
       * This may be equal to or later than `timeout_at`.
       */
      resolved_at: number;
    }
);

type WaitingWaitStep_Row = Extract<WaitStep_Row, { state: "waiting" }>;
type SatisfiedWaitStep_Row = Extract<WaitStep_Row, { state: "satisfied" }>;
type TimedOutWaitStep_Row = Extract<WaitStep_Row, { state: "timed_out" }>;

/**
 * A step row is created once the workflow reaches that step. From that point on, `status` describes the step's durable
 * lifecycle state.
 */
type Step_Row = RunStep_Row | SleepStep_Row | WaitStep_Row;

export type WorkflowStatus =
  | "pending" // Durable metadata exists, but create() has not initialized the workflow input yet
  | "initialized" // create() has pinned the workflow input, but execution has not started yet
  | "running" // The workflow is currently executing; steps are being created/processed
  | "paused" // The workflow is paused and will not make progress until resumed
  | "completed" // The workflow completed successfully; ('Workflow.next' returned { done: true, status: "succeeded" })
  | "failed" // A step exhausted retries and the workflow aborted; ('Workflow.next' returned { done: true, status: "failed" })
  | "cancelled"; // The workflow was terminated explicitly by the user.

type WorkflowMetadata_Row = {
  created_at: number;
  updated_at: number;
  status: WorkflowStatus;
  definition_input: string | null;
};

/**
 * Durable record of inbound events (`inbound_events`) that may satisfy wait steps.
 *
 * Persists delivered signals across restarts so step resolution is based on durable state rather than in-memory state.
 */
type InboundEvent_Row = {
  id: string;
  event_name: string;
  /**
   * Raw JSON value, or SQL NULL for undefined (no payload).
   */
  payload: string | null;
  created_at: number;
} & (
  | {
      /**
       * Step that claimed the event.
       */
      claimed_by: WaitStepId;
      /**
       * Time the event was claimed.
       */
      claimed_at: number;
    }
  | {
      claimed_by: null;
      claimed_at: null;
    }
);

type ClaimedInboundEvent_Row = Extract<
  InboundEvent_Row,
  {
    claimed_by: WaitStepId;
    claimed_at: number;
  }
>;

type InboundEvent<T extends Json | undefined = Json | undefined> = {
  id: string;
  eventName: string;
  payload: T;
  createdAt: Date;
} & (
  | {
      claimedBy: WaitStepId;
      claimedAt: Date;
    }
  | {
      claimedBy?: never;
      claimedAt?: never;
    }
);
