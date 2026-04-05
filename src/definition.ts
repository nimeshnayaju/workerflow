import { WorkerEntrypoint } from "cloudflare:workers";
import type { Json } from "./json";
import type { RunStepId, SleepStepId, WaitStepId, WorkflowRuntimeContext } from "./runtime";
import { AsyncLocalStorage } from "node:async_hooks";

declare global {
  interface ErrorConstructor {
    captureStackTrace(targetObject: object, constructorOpt?: Function): void;
  }
}

/**
 * `numOfSuccessfulRunCallbacks` counts successful sibling `run()` completions in this frame during the current `next()`
 * (see `WorkflowDefinition.run`). `parentStepId` is the innermost enclosing run step for nested steps.
 */
type RunStepFrame = { numOfSuccessfulRunCallbacks: number; parentStepId: RunStepId | null };

const STEP_EXECUTION_INTERRUPTED_ERROR_MESSAGE =
  "Step execution was interrupted before its outcome was durably recorded.";

export abstract class WorkflowDefinition<TInput extends Json | undefined = Json | undefined> extends WorkerEntrypoint<
  Cloudflare.Env,
  { requestId: string; runtimeInstanceId: string; input: TInput }
> {
  #context: WorkflowRuntimeContext | undefined;
  #requestId: string;
  #runtimeInstanceId: string;
  #runStepFrameContext: AsyncLocalStorage<RunStepFrame>;

  /**
   * A set of step ids that have been used in the current workflow execution.
   */
  readonly #seenStepIdsSoFar: Set<RunStepId | SleepStepId | WaitStepId>;

  constructor(ctx: ExecutionContext, env: Cloudflare.Env) {
    super(ctx, env);
    this.#seenStepIdsSoFar = new Set();
    this.#requestId = this.ctx.props.requestId;
    this.#runtimeInstanceId = this.ctx.props.runtimeInstanceId;
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
   *   - { done: false; resume: { type: "suspended" } }: the workflow should suspend itself and wait for the next alarm or
   *     inbound event to resume.
   * @internal
   */
  async next(context: WorkflowRuntimeContext): Promise<
    | { done: true; status: "completed" | "failed" }
    | {
        done: false;
        resume: { type: "immediate" } | { type: "suspended" };
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
        console.info(error, { requestId: this.#requestId, runtimeInstanceId: this.#runtimeInstanceId });
        return { done: true, status: "failed" };
      } else {
        // An exception can be thrown when calling a method on the WorkflowContext RPC target.
        // The resulting exception will have a 'remote' property set to 'True' in this case.
        if (error instanceof Error && "remote" in error && error.remote) {
          console.info(error, { requestId: this.#requestId, runtimeInstanceId: this.#runtimeInstanceId });
          /**
           * When calling Durable Objects from a Worker, errors may include .retryable and .overloaded properties
           * indicating whether the operation can be retried. See:
           * https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/#handle-errors-and-use-exception-boundaries.
           */
          if ("retryable" in error && error.retryable) {
            return { done: false, resume: { type: "suspended" } };
          }
          // An 'WorkflowInvariantError' indicates that the workflow engine is in an invalid state and the workflow should be aborted.
          else if (error.message.startsWith("WorkflowInvariantError")) {
            return { done: true, status: "failed" };
          }
          // All other remote errors are considered to be transient, so we instruct the workflow to suspend itself and wait for the next alarm to resume.
          else {
            return { done: false, resume: { type: "suspended" } };
          }
        }
        // All other non-remote errors are considered fatal and the workflow should be aborted.
        console.error(error instanceof Error ? error : String(error), {
          requestId: this.#requestId,
          runtimeInstanceId: this.#runtimeInstanceId
        });
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

    const step = await ctx.getOrCreateStep(runStepId, {
      type: "run",
      maxAttempts: config?.maxAttempts,
      parentStepId
    });

    if (this.#getRunStepFrame().numOfSuccessfulRunCallbacks >= 1) {
      throw new ResumeImmediatelyError();
    }

    if (step.state === "pending") {
      if (step.nextAttemptAt.getTime() > Date.now()) {
        throw new SuspendWorkflowError();
      }

      const attemptCount = step.attemptCount + 1; // Increment the attempt count by 1 as we're starting a new attempt
      const maxAttempts = step.maxAttempts;

      await ctx.handleRunAttemptEvent(runStepId, {
        type: "running",
        attemptCount: attemptCount
      });

      let _result: unknown;
      try {
        _result = await this.#runStepFrameContext.run(
          { numOfSuccessfulRunCallbacks: 0, parentStepId: runStepId },
          async () => await callback()
        );
      } catch (error) {
        // 'ResumeImmediatelyError' and 'SuspendWorkflowError' are rethrown so a nested `run()` does not record a spurious failure on the parent.
        if (error instanceof ResumeImmediatelyError || error instanceof SuspendWorkflowError) {
          throw error;
        }

        await ctx.handleRunAttemptEvent(runStepId, {
          type: "failed",
          errorMessage: String(error),
          errorName: error instanceof Error ? error.name : undefined,
          attemptCount: attemptCount,
          isNonRetryableStepError: error instanceof NonRetryableStepError
        });

        if (error instanceof NonRetryableStepError) {
          throw error;
        }

        if (maxAttempts !== null && attemptCount >= maxAttempts) {
          const error = new MaxAttemptsExceededError();
          Error.captureStackTrace(error, WorkflowDefinition.prototype.run);
          throw error;
        }

        throw new SuspendWorkflowError();
      }

      let result: string;
      if (_result === undefined) {
        result = "{}";
      } else {
        result = JSON.stringify({ value: _result });
      }

      await ctx.handleRunAttemptEvent(runStepId, {
        type: "succeeded",
        attemptCount: attemptCount,
        result: result
      });

      this.#getRunStepFrame().numOfSuccessfulRunCallbacks += 1;

      return _result as T;
    } else if (step.state === "running") {
      const maxAttempts = step.maxAttempts;
      const attemptCount = step.attemptCount;

      // If no direct child row explains the parent still being `running` (see `hasRunningOrWaitingChildSteps`), fail the attempt as interrupted.
      if (!(await ctx.hasRunningOrWaitingChildSteps(runStepId))) {
        await ctx.handleRunAttemptEvent(runStepId, {
          type: "failed",
          errorMessage: STEP_EXECUTION_INTERRUPTED_ERROR_MESSAGE,
          errorName: undefined,
          attemptCount: attemptCount
        });

        if (maxAttempts !== null && attemptCount >= maxAttempts) {
          const error = new MaxAttemptsExceededError();
          Error.captureStackTrace(error, WorkflowDefinition.prototype.run);
          throw error;
        } else {
          throw new SuspendWorkflowError();
        }
      }

      // Direct children in non-failure states: continue the same attempt by re-entering the callback.
      let _result: unknown;
      try {
        _result = await this.#runStepFrameContext.run(
          { numOfSuccessfulRunCallbacks: 0, parentStepId: runStepId },
          async () => await callback()
        );
      } catch (error) {
        if (error instanceof ResumeImmediatelyError || error instanceof SuspendWorkflowError) {
          throw error;
        }

        await ctx.handleRunAttemptEvent(runStepId, {
          type: "failed",
          errorMessage: String(error),
          errorName: error instanceof Error ? error.name : undefined,
          attemptCount: attemptCount,
          isNonRetryableStepError: error instanceof NonRetryableStepError
        });

        if (error instanceof NonRetryableStepError) {
          throw error;
        }

        if (maxAttempts !== null && attemptCount >= maxAttempts) {
          const err = new MaxAttemptsExceededError();
          Error.captureStackTrace(err, WorkflowDefinition.prototype.run);
          throw err;
        }

        throw new SuspendWorkflowError();
      }

      const result: string = _result === undefined ? "{}" : JSON.stringify({ value: _result });

      await ctx.handleRunAttemptEvent(runStepId, {
        type: "succeeded",
        attemptCount: attemptCount,
        result: result
      });

      this.#getRunStepFrame().numOfSuccessfulRunCallbacks += 1;

      return _result as T;
    } else if (step.state === "failed") {
      throw new AbortWorkflowError();
    } else if (step.state === "succeeded") {
      const parsed: unknown = JSON.parse(step.result);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        throw new Error(
          "Invalid stored workflow result; expected a non-null object payload; storage may be corrupted or written by an incompatible version."
        );
      }

      const keys = Object.keys(parsed);
      // "{}" means top-level undefined
      if (keys.length === 0) {
        return undefined as T;
      }

      if (keys.length === 1 && Object.hasOwn(parsed, "value")) {
        return (parsed as { value: T }).value;
      }

      throw new Error(
        "Invalid stored workflow result; expected an object payload with a 'value' property or an empty object; storage may be corrupted or written by an incompatible version."
      );
    }

    throw new Error("Unexpected run step state; expected 'pending', 'running', 'failed', or 'succeeded'.");
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

    const step = await ctx.getOrCreateStep(sleepStepId, {
      type: "sleep",
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
        await ctx.handleSleepStepEvent(sleepStepId, { type: "elapsed" });
        throw new ResumeImmediatelyError();
      }
    }

    throw new Error("Unexpected sleep step state; expected 'waiting' or 'elapsed'.");
  }

  protected async wait<T extends Json>(id: string, event: string, config?: { timeoutAt?: number }): Promise<T> {
    const waitStepId = id as WaitStepId;
    this.#assertUniqueStepIdInCurrentExecution(waitStepId);

    const ctx = this.#context;
    if (ctx === undefined) {
      const error = new Error("Workflow context is unavailable; `wait()` must be called from within `execute()`.");
      Error.captureStackTrace(error, WorkflowDefinition.prototype.wait);
      throw error;
    }

    const step = await ctx.getOrCreateStep(waitStepId, {
      type: "wait",
      eventName: event,
      timeoutAt: config?.timeoutAt ? new Date(config.timeoutAt) : undefined,
      parentStepId: this.#getRunStepFrame().parentStepId
    });

    if (step.state === "waiting") {
      // If the wait step has a timeout and the timeout has been reached, we mark the step as timed out and throw an 'AbortWorkflowError' to abort the workflow.
      if (step.timeoutAt !== undefined && Date.now() >= step.timeoutAt.getTime()) {
        await ctx.handleWaitStepEvent(waitStepId, { type: "timed_out" });
        const error = new WaitStepTimedOutError();
        Error.captureStackTrace(error, WorkflowDefinition.prototype.wait);
        throw error;
      }

      // Otherwise, we hint the driver to suspend the workflow until the next alarm or inbound event to resume.
      throw new SuspendWorkflowError();
    } else if (step.state === "timed_out") {
      // If the wait step has timed out, we throw an 'AbortWorkflowError' to abort the workflow.
      throw new AbortWorkflowError();
    } else if (step.state === "satisfied") {
      // If the wait step has been satisfied, we return the payload of the satisfied step.
      return JSON.parse(step.payload) as T;
    }

    throw new Error("Unexpected wait step state; expected 'waiting', 'satisfied', or 'timed_out'.");
  }
}

class ResumeImmediatelyError extends Error {}
class SuspendWorkflowError extends Error {}
class AbortWorkflowError extends Error {}

class MaxAttemptsExceededError extends Error {}
class WaitStepTimedOutError extends Error {}

export class NonRetryableStepError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "NonRetryableStepError";
  }
}
