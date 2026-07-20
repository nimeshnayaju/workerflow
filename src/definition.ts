import { WorkerEntrypoint } from "cloudflare:workers";
import type { Json } from "./json";
import type { RunStepAttemptId, RunStepId, SleepStepId, WaitStepId, WorkflowRuntimeContext } from "./runtime";
import { AsyncLocalStorage } from "node:async_hooks";

declare global {
  interface ErrorConstructor {
    captureStackTrace(targetObject: object, constructorOpt?: Function): void;
  }
}

/**
 * `numOfSuccessfulRunCallbacks` counts successful sibling `run()` experimental_completions in this frame during the
 * current `next()` (see `WorkflowDefinition.run`). `parentStepId` is the innermost enclosing run step for nested
 * steps.
 */
type RunStepFrame = { numOfSuccessfulRunCallbacks: number; parentStepId: RunStepId | null };

const STEP_EXECUTION_INTERRUPTED_ERROR_MESSAGE =
  "Step execution was interrupted before its outcome was durably recorded.";

export abstract class WorkflowDefinition<TInput extends Json | undefined = Json | undefined> extends WorkerEntrypoint<
  Cloudflare.Env,
  { input: TInput }
> {
  #context: WorkflowRuntimeContext | undefined;
  #runStepFrameContext: AsyncLocalStorage<RunStepFrame>;

  /**
   * A set of step ids that have been used in the current workflow execution.
   */
  readonly #seenStepIdsSoFar: Set<RunStepId | SleepStepId | WaitStepId>;

  constructor(ctx: ExecutionContext<{ input: TInput }>, env: Cloudflare.Env) {
    super(ctx, env);
    this.#seenStepIdsSoFar = new Set();
    this.#runStepFrameContext = new AsyncLocalStorage<RunStepFrame>();
  }

  #getRunStepFrame(): RunStepFrame {
    const frame = this.#runStepFrameContext.getStore();
    if (frame === undefined) {
      throw new Error("Run step frame is unset; run step must go through `WorkflowDefinition.next()`.");
    }
    return frame;
  }

  /**
   * @param context - The workflow runtime context: this includes methods to create and update steps.
   * @returns A promise that resolves to a hint on how to proceed next. The hint is one of the following:
   *
   *   - { done: true; status: "completed" | "failed" }: the workflow has completed or aborted.
   *   - { done: false; resume: { type: "immediate" } }: the workflow should resume immediately.
   *   - { done: false; resume: { type: "suspended" } }: durable state already describes how the workflow should resume.
   *   - { done: false; resume: { type: "retry", retryAt: number } }: a workflow-context transport failure should be
   *     retried at the provided timestamp.
   * @internal
   */
  async next(context: WorkflowRuntimeContext): Promise<
    | { done: true; status: "completed" | "failed" }
    | {
        done: false;
        resume: { type: "immediate" } | { type: "suspended" } | { type: "retry"; retryAt: number };
      }
  > {
    this.#context = context;
    try {
      await this.#runStepFrameContext.run({ numOfSuccessfulRunCallbacks: 0, parentStepId: null }, async () => {
        await this.execute();
      });
      return { done: true, status: "completed" };
    } catch (error) {
      if (error instanceof ResumeImmediatelyError) {
        return { done: false, resume: { type: "immediate" } };
      } else if (error instanceof SuspendWorkflowError) {
        return { done: false, resume: { type: "suspended" } };
      } else if (error instanceof AbortWorkflowError) {
        return { done: true, status: "failed" };
      } else if (
        error instanceof NonRetryableStepError ||
        error instanceof MaxAttemptsExceededError ||
        error instanceof WaitStepTimedOutError
      ) {
        console.error(error);
        return { done: true, status: "failed" };
      } else {
        // An exception can be thrown when calling a method on the WorkflowContext RPC target. Durable Object
        // infrastructure errors use `overloaded` and `retryable` to describe how callers should respond; `remote`
        // only identifies where an exception originated and is not itself a reason to retry.
        if (error instanceof Error && "remote" in error && error.remote === true) {
          if ("overloaded" in error && error.overloaded === true) {
            console.warn(`Workflow runtime call was overloaded; deferring recovery to the watchdog: ${String(error)}`);
            return { done: false, resume: { type: "suspended" } };
          }

          if (!("retryable" in error) || error.retryable !== true) {
            console.error(error);
            return { done: true, status: "failed" };
          }

          /**
           * When calling Durable Objects from a Worker, errors may include .retryable and .overloaded properties
           * indicating whether the operation can be retried.
           *
           * See: https://developers.cloudflare.com/durable-objects/best-practices/error-handling/
           */
          const retryAt = Date.now() + 5 * 60 * 1000;
          console.warn(
            `Workflow runtime call failed; retry scheduled for ${new Date(retryAt).toISOString()}: ${String(error)}`
          );
          // This transport failure may not have reached durable state, so request a retry after 5 minutes.
          // In future, we can use a more sophisticated retry strategy.
          return { done: false, resume: { type: "retry", retryAt } };
        }

        // All other errors are considered fatal. In particular, `remote` alone only indicates that an exception
        // originated across an RPC boundary and does not mean that replay is safe.
        console.error(error instanceof Error ? error : String(error));
        return { done: true, status: "failed" };
      }
    } finally {
      this.#context = undefined;
    }
  }

  /**
   * @param id - The step id to assert uniqueness of.
   * @throws An error if the step id has already been used in the current
   * workflow execution.
   */
  #assertUniqueStepIdInCurrentExecution(id: RunStepId | SleepStepId | WaitStepId): void {
    if (this.#seenStepIdsSoFar.has(id)) {
      const error = new Error(
        `Step id '${id}' was already used earlier in this workflow execution. Steps must be uniquely identified within a single workflow execution.`
      );
      Error.captureStackTrace(error, WorkflowDefinition.prototype.run);
      throw error;
    }
    this.#seenStepIdsSoFar.add(id);
  }

  abstract execute(): Promise<void>;

  async #processRunStepAttempt<T extends Json | undefined | void>(
    stepId: RunStepId,
    attemptId: RunStepAttemptId,
    ctx: WorkflowRuntimeContext,
    callback: () => Promise<T>
  ): Promise<T> {
    let _result: unknown;
    try {
      _result = await this.#runStepFrameContext.run(
        { numOfSuccessfulRunCallbacks: 0, parentStepId: stepId },
        async () => await callback()
      );
    } catch (error) {
      /**
       * A 'run' step callback can include nested steps that can throw control flow errors like 'ResumeImmediatelyError'
       * and 'SuspendWorkflowError'. We rethrow these errors without recording a failure on this (parent) attempt.
       */
      if (error instanceof ResumeImmediatelyError || error instanceof SuspendWorkflowError) {
        throw error;
      }

      // Transport errors from a nested context operation belong to next()'s recovery policy. In particular, the
      // operation may already have committed child state that lets the enclosing attempt resume safely on replay.
      if (error instanceof Error && "remote" in error && error.remote === true) {
        throw error;
      }

      // A terminal nested-step outcome cannot be repaired by replaying the enclosing callback. Record the enclosing
      // attempt as non-retryable and preserve the original control flow.
      const isTerminalNestedStepError =
        error instanceof WaitStepTimedOutError ||
        error instanceof AbortWorkflowError ||
        error instanceof MaxAttemptsExceededError;

      const updated = await ctx.handleRunAttemptFailed(stepId, attemptId, {
        errorMessage: String(error),
        errorName: error instanceof Error ? error.name : undefined,
        isNonRetryableStepError: error instanceof NonRetryableStepError || isTerminalNestedStepError
      });

      if (error instanceof NonRetryableStepError || isTerminalNestedStepError) throw error;

      if (updated.nextAttemptAt === undefined) {
        const error = new MaxAttemptsExceededError(stepId);
        Error.captureStackTrace(error, WorkflowDefinition.prototype.run);
        throw error;
      }

      throw new SuspendWorkflowError();
    }

    let resultJson: string | null;
    try {
      resultJson = serializeRunStepResult(_result);
    } catch (cause) {
      // Serialization is part of producing the durable callback outcome. Treat values outside the supported JSON
      // domain as a terminal step failure rather than leaving the attempt in `started` or persisting a value that will
      // replay differently. Keep handleRunAttemptSucceeded() outside this catch: a transport error from that call may
      // mean the success transaction committed and must be recovered by replay instead of rewritten as a failure.
      const error = new NonRetryableStepError(
        `Run step '${stepId}' returned a value that cannot be durably serialized as JSON: ${String(cause)}`
      );
      Error.captureStackTrace(error, WorkflowDefinition.prototype.run);
      await ctx.handleRunAttemptFailed(stepId, attemptId, {
        errorMessage: String(error),
        errorName: error.name,
        isNonRetryableStepError: true
      });
      throw error;
    }

    await ctx.handleRunAttemptSucceeded(stepId, attemptId, resultJson);

    this.#getRunStepFrame().numOfSuccessfulRunCallbacks += 1;
    // Return the same canonical value that a later replay reconstructs from durable state. In particular, JSON turns
    // signed zero into zero and may apply user-defined toJSON methods; callers must not observe a different first-run
    // value from the one replay will produce.
    return (resultJson === null ? undefined : JSON.parse(resultJson)) as T;
  }

  protected async run<T extends Json | undefined | void>(
    id: string,
    callback: () => Promise<T>,
    config?: {
      maxAttempts?: number;
    }
  ): Promise<T> {
    const runStepId = id as RunStepId;
    this.#assertUniqueStepIdInCurrentExecution(runStepId);

    const ctx = this.#context;
    if (ctx === undefined) {
      const error = new Error("Workflow context is unavailable; `run()` must be called from within `execute()`.");
      Error.captureStackTrace(error, WorkflowDefinition.prototype.run);
      throw error;
    }

    const parentStepId = this.#getRunStepFrame().parentStepId;

    const step = await ctx.getOrCreateRunStep(runStepId, {
      maxAttempts: config?.maxAttempts,
      parentStepId
    });

    if (this.#getRunStepFrame().numOfSuccessfulRunCallbacks >= 1) {
      throw new ResumeImmediatelyError();
    }

    const lastAttempt = step.attempts[step.attempts.length - 1];
    if (lastAttempt === undefined) {
      const started = await ctx.handleRunAttemptStarted(runStepId);

      return await this.#processRunStepAttempt(runStepId, started.id, ctx, callback);
    } else if (lastAttempt.state === "started") {
      const hasInProgressChildSteps = await ctx.hasInProgressChildSteps(runStepId);
      if (!hasInProgressChildSteps) {
        const updated = await ctx.handleRunAttemptFailed(runStepId, lastAttempt.id, {
          errorMessage: STEP_EXECUTION_INTERRUPTED_ERROR_MESSAGE,
          errorName: undefined
        });

        if (updated.nextAttemptAt === undefined) {
          const error = new MaxAttemptsExceededError(runStepId);
          Error.captureStackTrace(error, WorkflowDefinition.prototype.run);
          throw error;
        }

        throw new SuspendWorkflowError();
      } else {
        return await this.#processRunStepAttempt(runStepId, lastAttempt.id, ctx, callback);
      }
    } else if (lastAttempt.state === "failed") {
      if (lastAttempt.nextAttemptAt) {
        if (lastAttempt.nextAttemptAt.getTime() <= Date.now()) {
          const started = await ctx.handleRunAttemptStarted(runStepId);
          return await this.#processRunStepAttempt(runStepId, started.id, ctx, callback);
        } else {
          throw new SuspendWorkflowError();
        }
      } else {
        throw new AbortWorkflowError();
      }
    } else if (lastAttempt.state === "succeeded") {
      // Replay: the callback is NOT re-executed. Reconstruct the return value from durable state.
      if (lastAttempt.resultType === "json") {
        return JSON.parse(lastAttempt.resultJson) as T;
      }
      return undefined as T;
    } else {
      throw new Error("Unexpected run step attempt state; expected 'started', 'failed', or 'succeeded'.");
    }
  }

  protected async sleep(id: string, duration: number): Promise<void> {
    const sleepStepId = id as SleepStepId;
    this.#assertUniqueStepIdInCurrentExecution(sleepStepId);

    const ctx = this.#context;
    if (ctx === undefined) {
      const error = new Error("Workflow context is unavailable; `sleep()` must be called from within `execute()`.");
      Error.captureStackTrace(error, WorkflowDefinition.prototype.sleep);
      throw error;
    }

    const step = await ctx.getOrCreateSleepStep(sleepStepId, {
      wakeAt: new Date(Date.now() + duration),
      parentStepId: this.#getRunStepFrame().parentStepId
    });

    // If the sleep step has already elapsed, we return immediately as no further action is needed.
    if (step.state === "elapsed") {
      return;
    } else if (step.state === "waiting") {
      // If the sleep step is not yet due to wake up, we suspend the workflow.
      if (Date.now() < step.wakeAt.getTime()) {
        throw new SuspendWorkflowError();
      }
      // If the sleep step is due to wake up, we mark the step as elapsed and throw a 'ResumeImmediatelyError' to hint the driver to resume the workflow immediately.
      else {
        await ctx.handleSleepStepElapsed(sleepStepId);
        throw new ResumeImmediatelyError();
      }
    }

    throw new Error("Unexpected sleep step state; expected 'waiting' or 'elapsed'.");
  }

  protected async wait<T extends Json | undefined>(
    id: string,
    event: string,
    config?: { timeoutAt?: number }
  ): Promise<T> {
    const waitStepId = id as WaitStepId;
    this.#assertUniqueStepIdInCurrentExecution(waitStepId);

    const ctx = this.#context;
    if (ctx === undefined) {
      const error = new Error("Workflow context is unavailable; `wait()` must be called from within `execute()`.");
      Error.captureStackTrace(error, WorkflowDefinition.prototype.wait);
      throw error;
    }

    const step = await ctx.getOrCreateWaitStep<T>(waitStepId, {
      eventName: event,
      timeoutAt: config?.timeoutAt !== undefined ? new Date(config.timeoutAt) : undefined,
      parentStepId: this.#getRunStepFrame().parentStepId
    });

    if (step.state === "waiting") {
      if (step.timeoutAt !== undefined) {
        // If the timeout has been reached (or exceeded), we mark the step as timed out and throw an 'AbortWorkflowError' to abort the workflow.
        if (Date.now() >= step.timeoutAt.getTime()) {
          await ctx.handleWaitStepTimedOut(waitStepId);
          const error = new WaitStepTimedOutError(waitStepId, event);
          Error.captureStackTrace(error, WorkflowDefinition.prototype.wait);
          throw error;
        } else {
          // If the timeout has not been reached, we suspend the workflow and wait for the next alarm to resume.
          throw new SuspendWorkflowError();
        }
      } else {
        // If the wait step does not have a timeout, we suspend the workflow and wait for the next inbound event to resume.
        throw new SuspendWorkflowError();
      }
    } else if (step.state === "timed_out") {
      // If the wait step has timed out, we throw an 'AbortWorkflowError' to abort the workflow.
      throw new AbortWorkflowError();
    } else if (step.state === "satisfied") {
      // If the wait step has been satisfied, we return the payload of the satisfied step.
      return step.payload;
    }

    throw new Error("Unexpected wait step state; expected 'waiting', 'satisfied', or 'timed_out'.");
  }
}

class ResumeImmediatelyError extends Error {}
class SuspendWorkflowError extends Error {
  constructor() {
    super();
    this.name = "SuspendWorkflowError";
  }
}
class AbortWorkflowError extends Error {}

class MaxAttemptsExceededError extends Error {
  constructor(stepId: RunStepId) {
    super(`Run step '${stepId}' exhausted its configured attempts.`);
    this.name = "MaxAttemptsExceededError";
  }
}
class WaitStepTimedOutError extends Error {
  constructor(stepId: WaitStepId, event: string) {
    super(`Wait step '${stepId}' timed out while waiting for event '${event}'.`);
    this.name = "WaitStepTimedOutError";
  }
}

export class NonRetryableStepError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "NonRetryableStepError";
  }
}

/**
 * Serializes a run callback result for durable storage.
 *
 * SQL NULL is reserved for a root `undefined` result. Non-finite numbers are rejected explicitly because JSON.stringify
 * silently converts them to JSON null, which would otherwise make the first execution and replay observe different
 * values. JSON.stringify itself rejects cyclic structures and BigInt values.
 */
function serializeRunStepResult(value: unknown): string | null {
  if (value === undefined) return null;

  const result = JSON.stringify(value, (_key, nestedValue: unknown) => {
    if (typeof nestedValue === "number" && !Number.isFinite(nestedValue)) {
      throw new TypeError("Run step results cannot contain NaN or infinite numbers.");
    }
    return nestedValue;
  });

  if (result === undefined) {
    throw new TypeError("Run step result is not JSON-serializable.");
  }
  return result;
}
