import { abortAllDurableObjects, evictDurableObject, runInDurableObject, runDurableObjectAlarm } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import {
  WorkflowRuntimeContext,
  type RunStep,
  type RunStepAttempt,
  type RunStepAttemptId,
  type RunStepId,
  type SleepStepId,
  type WaitStepId,
  type WorkflowStatus
} from "../src/runtime";
import { TestCompletionWorkflowRuntime, TestWorkflowDefinition } from "./worker";
import { NonRetryableStepError } from "../src/definition";
import type { JsonObject } from "../src/json";

function createRunStepId(id: string): RunStepId {
  return id as RunStepId;
}
function createRunStepAttemptId(id: string): RunStepAttemptId {
  return id as RunStepAttemptId;
}
function createSleepStepId(id: string): SleepStepId {
  return id as SleepStepId;
}
function createWaitStepId(id: string): WaitStepId {
  return id as WaitStepId;
}

function createRunningWorkflowRuntimeContext(storage: DurableObjectStorage): WorkflowRuntimeContext {
  storage.sql.exec("UPDATE workflow_metadata SET status = 'initialized' WHERE id = 1 AND status = 'pending'");
  storage.sql.exec("UPDATE workflow_metadata SET status = 'running' WHERE id = 1 AND status = 'initialized'");
  return new WorkflowRuntimeContext(storage);
}

type WorkflowEventDeliveryRow = {
  event_id: number;
  attempts: number;
  next_attempt_at: number;
  delivered_at: number | null;
  last_error: string | null;
};

describe("WorkflowRuntime", () => {
  it("constructor() initializes the database and sets the status to pending", async () => {
    const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (instance, state) => {
      expect(instance.getStatus()).toBe("pending");
      expect(
        state.storage.sql.exec<{ version: number }>("SELECT MAX(version) AS version FROM migrations").one()
      ).toEqual({ version: 2 });
    });
  });

  describe("Durable Object restarts", () => {
    it("restores workflow input and resumes a persisted wait without replaying completed side effects", async () => {
      const receivedInputs: unknown[] = [];
      let beforeWaitRuns = 0;
      let afterWaitRuns = 0;
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          receivedInputs.push(this.ctx.props.input);
          await this.run("before-eviction", async () => {
            beforeWaitRuns++;
            return "persisted";
          });
          const payload = await this.wait<{ value: number }>("wait-across-eviction", "resume-after-eviction");
          await this.run("after-eviction", async () => {
            afterWaitRuns++;
            return payload;
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        const input = { workflow: "eviction-test" };

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create(input);
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-across-eviction");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");
          await expect.poll(() => state.storage.getAlarm()).toBeNull();
        });

        await evictDurableObject(stub);
        await stub.handleInboundEvent("resume-after-eviction", { value: 42 });

        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
          expect(instance.getSteps_experimental().find((s) => s.id === "wait-across-eviction")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: { value: 42 }
          });
          expect(instance.getWorkflowEvents_experimental().map((event) => event.type)).toEqual([
            "created",
            "started",
            "completed"
          ]);
        });

        expect(beforeWaitRuns).toBe(1);
        expect(afterWaitRuns).toBe(1);
        expect(receivedInputs.length).toBeGreaterThanOrEqual(2);
        for (const receivedInput of receivedInputs) {
          expect(receivedInput).toEqual(input);
        }
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("resumes a retry from its persisted alarm after eviction", async () => {
      let callbackRuns = 0;
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("retry-across-eviction", async () => {
            callbackRuns++;
            if (callbackRuns === 1) throw new Error("transient");
            return "recovered";
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        let retryAt = 0;

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "retry-across-eviction");
              if (step?.type !== "run") return undefined;
              const attempt = step.attempts[step.attempts.length - 1];
              return attempt?.state === "failed" ? attempt.nextAttemptAt?.getTime() : undefined;
            })
            .toEqual(expect.any(Number));

          const step = instance.getSteps_experimental().find((s) => s.id === "retry-across-eviction");
          expect(step?.type).toBe("run");
          const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
          const failedAttempt = attempts[attempts.length - 1];
          expect(failedAttempt?.state).toBe("failed");
          if (failedAttempt?.state !== "failed" || failedAttempt.nextAttemptAt === undefined) {
            throw new Error("Expected a retryable failed attempt.");
          }
          retryAt = failedAttempt.nextAttemptAt.getTime();
          expect(await state.storage.getAlarm()).toBe(retryAt);
        });

        await evictDurableObject(stub);
        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(retryAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);

          await runInDurableObject(stub, async (instance) => {
            await expect.poll(() => instance.getStatus()).toBe("completed");
            const step = instance.getSteps_experimental().find((s) => s.id === "retry-across-eviction");
            expect(step?.type).toBe("run");
            const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(attempts).toHaveLength(2);
            expect(attempts[0]).toMatchObject({ state: "failed", errorMessage: "Error: transient" });
            expect(attempts[1]).toMatchObject({ state: "succeeded", resultJson: '"recovered"' });
          });
        } finally {
          dateNowSpy.mockRestore();
        }

        expect(callbackRuns).toBe(2);
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("restores paused status and keeps inbound events queued across eviction", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait<{ value: number }>("paused-wait-across-eviction", "resume-paused-after-eviction");
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());

        await runInDurableObject(stub, async (instance) => {
          await instance.create();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "paused-wait-across-eviction");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");
          await instance.pause();
          expect(instance.getStatus()).toBe("paused");
        });

        await evictDurableObject(stub);
        await stub.handleInboundEvent("resume-paused-after-eviction", { value: 42 });
        expect(await runDurableObjectAlarm(stub)).toBe(false);

        await runInDurableObject(stub, async (instance) => {
          expect(instance.getStatus()).toBe("paused");
          expect(instance.getSteps_experimental().find((s) => s.id === "paused-wait-across-eviction")).toMatchObject({
            type: "wait",
            state: "waiting"
          });

          await instance.resume();
          await expect.poll(() => instance.getStatus()).toBe("completed");
          expect(instance.getSteps_experimental().find((s) => s.id === "paused-wait-across-eviction")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: { value: 42 }
          });
          expect(instance.getWorkflowEvents_experimental().map((event) => event.type)).toEqual([
            "created",
            "started",
            "paused",
            "resumed",
            "completed"
          ]);
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("resumes a persisted sleep alarm after eviction", async () => {
      let afterSleepRuns = 0;
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-across-eviction", 1_000);
          await this.run("after-sleep-across-eviction", async () => {
            afterSleepRuns++;
            return "awake";
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        let wakeAt = 0;

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "sleep-across-eviction");
              return step?.type === "sleep" && step.state === "waiting" ? step.wakeAt.getTime() : undefined;
            })
            .toEqual(expect.any(Number));

          const step = instance.getSteps_experimental().find((s) => s.id === "sleep-across-eviction");
          expect(step?.type).toBe("sleep");
          if (step?.type !== "sleep" || step.state !== "waiting") {
            throw new Error("Expected a waiting sleep step.");
          }
          wakeAt = step.wakeAt.getTime();
          expect(await state.storage.getAlarm()).toBe(wakeAt);
        });

        await evictDurableObject(stub);
        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(wakeAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);

          await runInDurableObject(stub, async (instance) => {
            await expect.poll(() => instance.getStatus()).toBe("completed");
            expect(instance.getSteps_experimental().find((s) => s.id === "sleep-across-eviction")).toMatchObject({
              type: "sleep",
              state: "elapsed",
              resolvedAt: expect.any(Date)
            });
            const afterSleep = instance.getSteps_experimental().find((s) => s.id === "after-sleep-across-eviction");
            expect(afterSleep?.type).toBe("run");
            const attempts = (afterSleep as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(attempts).toHaveLength(1);
            expect(attempts[0]).toMatchObject({ state: "succeeded", resultJson: '"awake"' });
          });
        } finally {
          dateNowSpy.mockRestore();
        }

        expect(afterSleepRuns).toBe(1);
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("resumes a persisted wait timeout alarm after eviction", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-timeout-across-eviction", "never-arrives", {
            timeoutAt: Date.now() + 1_000
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        let timeoutAt = 0;

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-timeout-across-eviction");
              return step?.type === "wait" && step.state === "waiting" ? step.timeoutAt?.getTime() : undefined;
            })
            .toEqual(expect.any(Number));

          const step = instance.getSteps_experimental().find((s) => s.id === "wait-timeout-across-eviction");
          expect(step?.type).toBe("wait");
          if (step?.type !== "wait" || step.state !== "waiting" || step.timeoutAt === undefined) {
            throw new Error("Expected a waiting wait step with a timeout.");
          }
          timeoutAt = step.timeoutAt.getTime();
          expect(await state.storage.getAlarm()).toBe(timeoutAt);
        });

        await evictDurableObject(stub);
        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(timeoutAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);

          await runInDurableObject(stub, async (instance) => {
            await expect.poll(() => instance.getStatus()).toBe("failed");
            expect(instance.getSteps_experimental().find((s) => s.id === "wait-timeout-across-eviction")).toMatchObject(
              {
                type: "wait",
                state: "timed_out",
                timeoutAt: new Date(timeoutAt),
                resolvedAt: expect.any(Date)
              }
            );
            expect(instance.getWorkflowEvents_experimental().map((event) => event.type)).toEqual([
              "created",
              "started",
              "failed"
            ]);
          });
        } finally {
          dateNowSpy.mockRestore();
        }

        await stub.handleInboundEvent("never-arrives", { late: true });
        expect(await runDurableObjectAlarm(stub)).toBe(false);
        await runInDurableObject(stub, async (instance, state) => {
          expect(instance.getStatus()).toBe("failed");
          expect(instance.getSteps_experimental().find((s) => s.id === "wait-timeout-across-eviction")).toMatchObject({
            type: "wait",
            state: "timed_out"
          });
          const inboundEventCount = state.storage.sql
            .exec<{ count: number }>("SELECT COUNT(*) AS count FROM inbound_events")
            .one().count;
          expect(inboundEventCount).toBe(0);
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("uses the watchdog to recover a run attempt interrupted by forced teardown", async () => {
      let callbackRuns = 0;
      const firstAttemptStarted = Promise.withResolvers<void>();
      const interruptedAttempt = Promise.withResolvers<never>();
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run(
            "interrupted-by-teardown",
            async () => {
              callbackRuns++;
              if (callbackRuns === 1) {
                firstAttemptStarted.resolve();
                return await interruptedAttempt.promise;
              }
              return "recovered";
            },
            { maxAttempts: 2 }
          );
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        let firstWatchdogAt = 0;

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await firstAttemptStarted.promise;
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "interrupted-by-teardown");
              if (step?.type !== "run") return undefined;
              return step.attempts[step.attempts.length - 1]?.state;
            })
            .toBe("started");

          const watchdogAt = await state.storage.getAlarm();
          expect(watchdogAt).not.toBeNull();
          if (watchdogAt === null) throw new Error("Expected a watchdog alarm.");
          firstWatchdogAt = watchdogAt;
          expect(watchdogAt).toBeGreaterThanOrEqual(Date.now() + 29 * 60_000);
          expect(watchdogAt).toBeLessThanOrEqual(Date.now() + 31 * 60_000);
        });

        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance, state) => {
          const step = instance.getSteps_experimental().find((s) => s.id === "interrupted-by-teardown");
          expect(step?.type).toBe("run");
          expect((step as RunStep & { attempts: RunStepAttempt[] }).attempts.at(-1)).toMatchObject({
            state: "started"
          });
          const replacementWatchdogAt = await state.storage.getAlarm();
          expect(replacementWatchdogAt).not.toBeNull();
          expect(replacementWatchdogAt as number).toBeGreaterThanOrEqual(firstWatchdogAt);
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        let retryAt = 0;
        await runInDurableObject(stub, async (instance, state) => {
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "interrupted-by-teardown");
              if (step?.type !== "run") return undefined;
              const attempt = step.attempts[step.attempts.length - 1];
              return attempt?.state === "failed" ? attempt.nextAttemptAt?.getTime() : undefined;
            })
            .toEqual(expect.any(Number));

          const step = instance.getSteps_experimental().find((s) => s.id === "interrupted-by-teardown");
          expect(step?.type).toBe("run");
          const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
          const failedAttempt = attempts[attempts.length - 1];
          expect(failedAttempt?.state).toBe("failed");
          if (failedAttempt?.state !== "failed" || failedAttempt.nextAttemptAt === undefined) {
            throw new Error("Expected an interrupted attempt with a scheduled retry.");
          }
          retryAt = failedAttempt.nextAttemptAt.getTime();
          expect(await state.storage.getAlarm()).toBe(retryAt);
        });

        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(retryAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);

          await runInDurableObject(stub, async (instance) => {
            await expect.poll(() => instance.getStatus()).toBe("completed");
            const step = instance.getSteps_experimental().find((s) => s.id === "interrupted-by-teardown");
            expect(step?.type).toBe("run");
            const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(attempts).toHaveLength(2);
            expect(attempts[0]).toMatchObject({
              state: "failed",
              errorMessage: "Step execution was interrupted before its outcome was durably recorded.",
              nextAttemptAt: expect.any(Date)
            });
            expect(attempts[1]).toMatchObject({ state: "succeeded", resultJson: '"recovered"' });
          });
        } finally {
          dateNowSpy.mockRestore();
        }

        expect(callbackRuns).toBe(2);
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("hands the watchdog off to a later durable sleep deadline after suspension is acknowledged", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-after-watchdog", 2 * 60 * 60_000);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "sleep-after-watchdog");
              return step?.type === "sleep" ? step.state : undefined;
            })
            .toBe("waiting");

          const step = instance.getSteps_experimental().find((s) => s.id === "sleep-after-watchdog");
          expect(step?.type).toBe("sleep");
          if (step?.type !== "sleep" || step.state !== "waiting") {
            throw new Error("Expected a waiting sleep step.");
          }
          expect(step.wakeAt.getTime()).toBeGreaterThan(Date.now() + 30 * 60_000);
          await expect.poll(() => state.storage.getAlarm()).toBe(step.wakeAt.getTime());
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("drains an inbound run request submitted while suspension handoff is pending", async () => {
      const suspendedResultProduced = Promise.withResolvers<void>();
      const releaseSuspendedResult = Promise.withResolvers<void>();
      const next = TestWorkflowDefinition.prototype.next;
      const nextSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "next")
        .mockImplementationOnce(async function (this: TestWorkflowDefinition, context) {
          const result = await next.call(this, context);
          expect(result).toMatchObject({ done: false, resume: { type: "suspended" } });
          suspendedResultProduced.resolve();
          await releaseSuspendedResult.promise;
          return result;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("inbound-during-handoff", "inbound-during-handoff-event");
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await suspendedResultProduced.promise;

          await instance.handleInboundEvent("inbound-during-handoff-event", { accepted: true });
          releaseSuspendedResult.resolve();
          await scheduler.wait(10);

          await expect.poll(() => instance.getStatus()).toBe("completed");
          expect(await state.storage.getAlarm()).toBeNull();
          expect(instance.getSteps_experimental().find((s) => s.id === "inbound-during-handoff")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: { accepted: true }
          });
          expect(nextSpy).toHaveBeenCalledTimes(2);
        });
      } finally {
        releaseSuspendedResult.resolve();
        nextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("drains a run-success alarm submitted while suspension handoff is pending", async () => {
      const runStarted = Promise.withResolvers<void>();
      const releaseRun = Promise.withResolvers<void>();
      const suspendedResultProduced = Promise.withResolvers<void>();
      const releaseSuspendedResult = Promise.withResolvers<void>();
      const next = TestWorkflowDefinition.prototype.next;
      const nextSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "next")
        .mockImplementationOnce(async function (this: TestWorkflowDefinition, context) {
          const result = await next.call(this, context);
          expect(result).toMatchObject({ done: false, resume: { type: "suspended" } });
          suspendedResultProduced.resolve();
          await releaseSuspendedResult.promise;
          return result;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await Promise.all([
            this.run("run-success-during-handoff", async () => {
              runStarted.resolve();
              await releaseRun.promise;
              return "done";
            }),
            this.sleep("sleep-during-handoff", 2 * 60 * 60_000)
          ]);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await runStarted.promise;
          await suspendedResultProduced.promise;

          releaseRun.resolve();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "run-success-during-handoff");
              if (step?.type !== "run") return undefined;
              return step.attempts[step.attempts.length - 1]?.state;
            })
            .toBe("succeeded");

          releaseSuspendedResult.resolve();
          await scheduler.wait(10);

          const sleepStep = instance.getSteps_experimental().find((s) => s.id === "sleep-during-handoff");
          expect(sleepStep?.type).toBe("sleep");
          if (sleepStep?.type !== "sleep" || sleepStep.state !== "waiting") {
            throw new Error("Expected a waiting sleep step.");
          }
          await expect.poll(() => state.storage.getAlarm()).toBe(sleepStep.wakeAt.getTime());
          expect(nextSpy).toHaveBeenCalledTimes(2);
        });
      } finally {
        releaseRun.resolve();
        releaseSuspendedResult.resolve();
        nextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("replays an in-flight terminal result when another run is requested", async () => {
      const terminalResultProduced = Promise.withResolvers<void>();
      const releaseTerminalResult = Promise.withResolvers<void>();
      const next = TestWorkflowDefinition.prototype.next;
      const nextSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "next")
        .mockImplementationOnce(async function (this: TestWorkflowDefinition, context) {
          const result = await next.call(this, context);
          expect(result).toEqual({ done: true, status: "completed" });
          terminalResultProduced.resolve();
          await releaseTerminalResult.promise;
          return result;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {});

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          await instance.create();
          await terminalResultProduced.promise;

          await instance.pause();
          await instance.resume();
          releaseTerminalResult.resolve();

          await expect.poll(() => instance.getStatus()).toBe("completed");
          expect(nextSpy.mock.calls.length).toBeGreaterThanOrEqual(2);
          expect(instance.getWorkflowEvents_experimental().map((event) => event.type)).toEqual([
            "created",
            "started",
            "paused",
            "resumed",
            "completed"
          ]);
        });
      } finally {
        releaseTerminalResult.resolve();
        nextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers a succeeded run attempt when the context response is lost after commit", async () => {
      let callbackRuns = 0;
      const committed = Promise.withResolvers<void>();
      const responseLost = Promise.withResolvers<never>();
      const handleRunAttemptSucceeded = WorkflowRuntimeContext.prototype.handleRunAttemptSucceeded;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "handleRunAttemptSucceeded")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, attemptId, resultJson) {
          const result = await handleRunAttemptSucceeded.call(this, stepId, attemptId, resultJson);
          committed.resolve();
          await responseLost.promise;
          return result;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("succeeded-before-response-loss", async () => {
            callbackRuns++;
            return "persisted";
          });
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await committed.promise;

          expect(instance.getSteps_experimental().find((s) => s.id === "succeeded-before-response-loss")).toMatchObject(
            {
              type: "run",
              attempts: [{ state: "succeeded", resultJson: '"persisted"' }]
            }
          );
          expect(await state.storage.getAlarm()).not.toBeNull();
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);

        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
        });
        expect(callbackRuns).toBe(1);
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers a retryable failed run attempt when the context response is lost after commit", async () => {
      let callbackRuns = 0;
      const committed = Promise.withResolvers<void>();
      const responseLost = Promise.withResolvers<never>();
      const handleRunAttemptFailed = WorkflowRuntimeContext.prototype.handleRunAttemptFailed;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "handleRunAttemptFailed")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, attemptId, result) {
          const failed = await handleRunAttemptFailed.call(this, stepId, attemptId, result);
          committed.resolve();
          await responseLost.promise;
          return failed;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run(
            "retryable-failure-before-response-loss",
            async () => {
              callbackRuns++;
              if (callbackRuns === 1) throw new Error("transient");
              return "recovered";
            },
            { maxAttempts: 2 }
          );
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        let retryAt = 0;

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await committed.promise;

          const step = instance
            .getSteps_experimental()
            .find((candidate) => candidate.id === "retryable-failure-before-response-loss");
          expect(step?.type).toBe("run");
          const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
          const failed = attempts[attempts.length - 1];
          expect(failed?.state).toBe("failed");
          if (failed?.state !== "failed" || failed.nextAttemptAt === undefined) {
            throw new Error("Expected a retryable failed attempt.");
          }
          retryAt = failed.nextAttemptAt.getTime();
          expect(await state.storage.getAlarm()).toBe(retryAt);
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(retryAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);
          await runInDurableObject(stub, async (instance) => {
            await expect.poll(() => instance.getStatus()).toBe("completed");
            const step = instance
              .getSteps_experimental()
              .find((candidate) => candidate.id === "retryable-failure-before-response-loss");
            expect(step?.type).toBe("run");
            expect((step as RunStep & { attempts: RunStepAttempt[] }).attempts).toMatchObject([
              { state: "failed", nextAttemptAt: new Date(retryAt) },
              { state: "succeeded", resultJson: '"recovered"' }
            ]);
          });
        } finally {
          dateNowSpy.mockRestore();
        }
        expect(callbackRuns).toBe(2);
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers a terminal failed run attempt when the context response is lost after commit", async () => {
      let callbackRuns = 0;
      const committed = Promise.withResolvers<void>();
      const responseLost = Promise.withResolvers<never>();
      const handleRunAttemptFailed = WorkflowRuntimeContext.prototype.handleRunAttemptFailed;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "handleRunAttemptFailed")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, attemptId, result) {
          const failed = await handleRunAttemptFailed.call(this, stepId, attemptId, result);
          committed.resolve();
          await responseLost.promise;
          return failed;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run(
            "terminal-failure-before-response-loss",
            async () => {
              callbackRuns++;
              throw new Error("terminal");
            },
            { maxAttempts: 1 }
          );
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await committed.promise;
          expect(
            instance.getSteps_experimental().find((s) => s.id === "terminal-failure-before-response-loss")
          ).toMatchObject({
            type: "run",
            attempts: [{ state: "failed", errorMessage: "Error: terminal", nextAttemptAt: undefined }]
          });
          expect(await state.storage.getAlarm()).not.toBeNull();
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("failed");
        });
        expect(callbackRuns).toBe(1);
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers a newly-created sleep when the context response is lost after commit", async () => {
      const committed = Promise.withResolvers<void>();
      const responseLost = Promise.withResolvers<never>();
      const getOrCreateSleepStep = WorkflowRuntimeContext.prototype.getOrCreateSleepStep;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "getOrCreateSleepStep")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, options) {
          const step = await getOrCreateSleepStep.call(this, stepId, options);
          committed.resolve();
          await responseLost.promise;
          return step;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-created-before-response-loss", 1_000);
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        let wakeAt = 0;

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await committed.promise;

          const step = instance.getSteps_experimental().find((s) => s.id === "sleep-created-before-response-loss");
          expect(step?.type).toBe("sleep");
          if (step?.type !== "sleep" || step.state !== "waiting") {
            throw new Error("Expected a waiting sleep step.");
          }
          wakeAt = step.wakeAt.getTime();
          expect(await state.storage.getAlarm()).toBe(wakeAt);
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(wakeAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);
          await runInDurableObject(stub, async (instance) => {
            await expect.poll(() => instance.getStatus()).toBe("completed");
            expect(
              instance.getSteps_experimental().find((s) => s.id === "sleep-created-before-response-loss")
            ).toMatchObject({ type: "sleep", state: "elapsed" });
          });
        } finally {
          dateNowSpy.mockRestore();
        }
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers an elapsed sleep when the context response is lost after commit", async () => {
      const committed = Promise.withResolvers<void>();
      const responseLost = Promise.withResolvers<never>();
      const handleSleepStepElapsed = WorkflowRuntimeContext.prototype.handleSleepStepElapsed;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "handleSleepStepElapsed")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId) {
          await handleSleepStepElapsed.call(this, stepId);
          committed.resolve();
          await responseLost.promise;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-elapsed-before-response-loss", 0);
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await committed.promise;
          expect(
            instance.getSteps_experimental().find((s) => s.id === "sleep-elapsed-before-response-loss")
          ).toMatchObject({ type: "sleep", state: "elapsed" });
          expect(await state.storage.getAlarm()).not.toBeNull();
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
        });
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers a newly-created wait timeout when the context response is lost after commit", async () => {
      const committed = Promise.withResolvers<void>();
      const responseLost = Promise.withResolvers<never>();
      const getOrCreateWaitStep = WorkflowRuntimeContext.prototype.getOrCreateWaitStep;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "getOrCreateWaitStep")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, options) {
          const step = await getOrCreateWaitStep.call(this, stepId, options);
          committed.resolve();
          await responseLost.promise;
          return step;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-created-before-response-loss", "never-arrives", {
            timeoutAt: Date.now() + 1_000
          });
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        let timeoutAt = 0;

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await committed.promise;

          const step = instance.getSteps_experimental().find((s) => s.id === "wait-created-before-response-loss");
          expect(step?.type).toBe("wait");
          if (step?.type !== "wait" || step.state !== "waiting" || step.timeoutAt === undefined) {
            throw new Error("Expected a waiting wait step with a timeout.");
          }
          timeoutAt = step.timeoutAt.getTime();
          expect(await state.storage.getAlarm()).toBe(timeoutAt);
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(timeoutAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);
          await runInDurableObject(stub, async (instance) => {
            await expect.poll(() => instance.getStatus()).toBe("failed");
            expect(
              instance.getSteps_experimental().find((s) => s.id === "wait-created-before-response-loss")
            ).toMatchObject({ type: "wait", state: "timed_out" });
          });
        } finally {
          dateNowSpy.mockRestore();
        }
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers a timed-out wait when the context response is lost after commit", async () => {
      const committed = Promise.withResolvers<void>();
      const responseLost = Promise.withResolvers<never>();
      const handleWaitStepTimedOut = WorkflowRuntimeContext.prototype.handleWaitStepTimedOut;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "handleWaitStepTimedOut")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId) {
          await handleWaitStepTimedOut.call(this, stepId);
          committed.resolve();
          await responseLost.promise;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-timed-out-before-response-loss", "never-arrives", {
            timeoutAt: Date.now() - 1_000
          });
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await committed.promise;
          expect(
            instance.getSteps_experimental().find((s) => s.id === "wait-timed-out-before-response-loss")
          ).toMatchObject({ type: "wait", state: "timed_out" });
          expect(await state.storage.getAlarm()).not.toBeNull();
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("failed");
        });
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers a wait satisfied from a queued event when the context response is lost after commit", async () => {
      const committed = Promise.withResolvers<void>();
      const responseLost = Promise.withResolvers<never>();
      const getOrCreateWaitStep = WorkflowRuntimeContext.prototype.getOrCreateWaitStep;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "getOrCreateWaitStep")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, options) {
          const step = await getOrCreateWaitStep.call(this, stepId, options);
          committed.resolve();
          await responseLost.promise;
          return step;
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const payload = await this.wait<{ value: number }>("queued-wait-before-response-loss", "queued-event");
          expect(payload).toEqual({ value: 42 });
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);

        await runInDurableObject(stub, async (instance, state) => {
          await instance.handleInboundEvent("queued-event", { value: 42 });
          await instance.create();
          await committed.promise;
          expect(
            instance.getSteps_experimental().find((s) => s.id === "queued-wait-before-response-loss")
          ).toMatchObject({ type: "wait", state: "satisfied", payload: { value: 42 } });
          expect(await state.storage.getAlarm()).not.toBeNull();
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
        });
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("recovers a live inbound event after forced teardown following commit", async () => {
      let satisfiedRuns = 0;
      const waitReplayed = Promise.withResolvers<void>();
      const resumedExecution = Promise.withResolvers<never>();
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const payload = await this.wait<{ value: number }>("live-wait-before-response-loss", "live-event");
          expect(payload).toEqual({ value: 42 });
          satisfiedRuns++;
          if (satisfiedRuns === 1) {
            waitReplayed.resolve();
            await resumedExecution.promise;
          }
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);

        await runInDurableObject(stub, async (instance) => {
          await instance.create();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "live-wait-before-response-loss");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");
        });

        await runInDurableObject(stub, async (instance, state) => {
          void instance.handleInboundEvent("live-event", { value: 42 });
          await waitReplayed.promise;
          expect(instance.getSteps_experimental().find((s) => s.id === "live-wait-before-response-loss")).toMatchObject(
            { type: "wait", state: "satisfied", payload: { value: 42 } }
          );
          expect(await state.storage.getAlarm()).not.toBeNull();
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
          expect(instance.getSteps_experimental().find((s) => s.id === "live-wait-before-response-loss")).toMatchObject(
            { type: "wait", state: "satisfied", payload: { value: 42 } }
          );
        });
        expect(satisfiedRuns).toBe(2);
      } finally {
        executeSpy.mockRestore();
      }
    });
  });

  it("fails when the same step id is reused across run steps in one execution", async () => {
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run("step-1", async () => 1);
        await this.run("step-1", async () => 2);
      });

    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("failed");
      });
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("fails when the same step id is reused across run steps and wait steps in one execution", async () => {
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run("shared-id", async () => 1);
        await this.wait("shared-id", "event-1");
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };
        await instance.create();
        await expect(promise).resolves.toBe("failed");
      });
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("fails when a step callback throws 'NonRetryableStepError'", async () => {
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run("step-1", async () => {
          throw new NonRetryableStepError("This is a non-retryable step error");
        });
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("failed");
        const steps = instance.getSteps_experimental();
        expect(steps).toHaveLength(1);
        const step = steps[0]!;
        expect(step.type).toBe("run");
        const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(attempts).toHaveLength(1);
        expect(attempts[0]).toMatchObject({
          state: "failed",
          errorMessage: "NonRetryableStepError: This is a non-retryable step error",
          errorName: "NonRetryableStepError"
        });
      });
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("fails when retries are exhausted on a step", async () => {
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run(
          "step-1",
          async () => {
            throw new Error("test");
          },
          { maxAttempts: 2 }
        );
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("failed");
        const steps = instance.getSteps_experimental();
        expect(steps).toHaveLength(1);
        const step = steps[0]!;
        expect(step.type).toBe("run");
        const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(attempts).toHaveLength(2);
        expect(attempts[1]).toMatchObject({
          state: "failed",
          errorMessage: "Error: test",
          errorName: "Error"
        });
      });
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("fails when execute() throws an unhandled error", async () => {
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        throw new Error("test");
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("failed");
      });
    } finally {
      executeSpy.mockRestore();
    }
  });

  describe("definition RPC error handling", () => {
    it("uses scheduler.wait to retry an explicitly retryable definition call across loopback RPC", async () => {
      const waitSpy = vi.spyOn(scheduler, "wait").mockResolvedValue();
      const randomSpy = vi.spyOn(Math, "random").mockReturnValue(0.5);
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const nextSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "next")
        .mockRejectedValueOnce(
          Object.assign(new Error("definition unavailable"), {
            retryable: true
          })
        )
        .mockResolvedValueOnce({ done: true, status: "completed" });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("completed");

          expect(nextSpy).toHaveBeenCalledTimes(2);
          expect(waitSpy).toHaveBeenCalledOnce();
          expect(waitSpy).toHaveBeenCalledWith(5_500);
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        waitSpy.mockRestore();
        randomSpy.mockRestore();
        warnSpy.mockRestore();
        nextSpy.mockRestore();
      }
    });

    it("stops short retries after three attempts and retains the watchdog", async () => {
      const waitSpy = vi.spyOn(scheduler, "wait").mockResolvedValue();
      const randomSpy = vi.spyOn(Math, "random").mockReturnValue(0.5);
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          const definition = vi.fn(() => ({
            next: vi.fn(async () => {
              throw Object.assign(new Error("still unavailable"), { retryable: true });
            })
          }));
          Object.defineProperty(instance, "definition", { configurable: true, value: definition });

          await instance.create();
          await expect
            .poll(() =>
              warnSpy.mock.calls.some(([message]) => String(message).includes("definition retries exhausted"))
            )
            .toBe(true);

          expect(instance.getStatus()).toBe("running");
          expect(definition).toHaveBeenCalledTimes(4);
          expect(waitSpy.mock.calls).toEqual([[5_500], [10_500], [20_500]]);
          const watchdogAt = await state.storage.getAlarm();
          expect(watchdogAt).not.toBeNull();
          expect(watchdogAt as number).toBeGreaterThanOrEqual(Date.now() + 29 * 60_000);
        });
      } finally {
        waitSpy.mockRestore();
        randomSpy.mockRestore();
        warnSpy.mockRestore();
      }
    });

    it("gives overloaded precedence over retryable and defers recovery to the watchdog", async () => {
      const waitSpy = vi.spyOn(scheduler, "wait").mockResolvedValue();
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const nextSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "next")
        .mockRejectedValueOnce(
          Object.assign(new Error("definition overloaded"), {
            retryable: true,
            overloaded: true
          })
        )
        .mockResolvedValueOnce({ done: true, status: "completed" });
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());

      try {
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect
            .poll(() => warnSpy.mock.calls.some(([message]) => String(message).includes("was overloaded")))
            .toBe(true);

          expect(instance.getStatus()).toBe("running");
          expect(nextSpy).toHaveBeenCalledOnce();
          expect(waitSpy).not.toHaveBeenCalled();
          const watchdogAt = await state.storage.getAlarm();
          expect(watchdogAt).not.toBeNull();
          expect(watchdogAt as number).toBeGreaterThanOrEqual(Date.now() + 29 * 60_000);
        });

        await evictDurableObject(stub);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
        });
      } finally {
        waitSpy.mockRestore();
        warnSpy.mockRestore();
        nextSpy.mockRestore();
      }
    });

    it("does not treat remote alone as a reason to retry", async () => {
      const waitSpy = vi.spyOn(scheduler, "wait").mockResolvedValue();
      const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      const nextSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "next")
        .mockRejectedValueOnce(new Error("remote user error"));

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("failed");

          expect(nextSpy).toHaveBeenCalledOnce();
          expect(waitSpy).not.toHaveBeenCalled();
          expect(errorSpy.mock.calls[0]?.[0]).toMatchObject({ remote: true });
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        waitSpy.mockRestore();
        errorSpy.mockRestore();
        nextSpy.mockRestore();
      }
    });

    it("does not retry after the workflow is paused during scheduler.wait", async () => {
      const retryWait = Promise.withResolvers<void>();
      const waitSpy = vi.spyOn(scheduler, "wait").mockImplementation(() => retryWait.promise);
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          const definition = vi.fn(() => ({
            next: vi.fn(async () => {
              throw Object.assign(new Error("definition unavailable"), { retryable: true });
            })
          }));
          Object.defineProperty(instance, "definition", { configurable: true, value: definition });

          await instance.create();
          await expect.poll(() => waitSpy.mock.calls.length).toBe(1);
          await instance.pause();
          retryWait.resolve();
          await retryWait.promise;
          await Promise.resolve();

          expect(instance.getStatus()).toBe("paused");
          expect(definition).toHaveBeenCalledOnce();
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        retryWait.resolve();
        waitSpy.mockRestore();
        warnSpy.mockRestore();
      }
    });

    it("does not retry after the workflow is cancelled during scheduler.wait", async () => {
      const retryWait = Promise.withResolvers<void>();
      const waitSpy = vi.spyOn(scheduler, "wait").mockImplementation(() => retryWait.promise);
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          const definition = vi.fn(() => ({
            next: vi.fn(async () => {
              throw Object.assign(new Error("definition unavailable"), { retryable: true });
            })
          }));
          Object.defineProperty(instance, "definition", { configurable: true, value: definition });

          await instance.create();
          await expect.poll(() => waitSpy.mock.calls.length).toBe(1);
          await instance.cancel("cancel during retry wait");
          retryWait.resolve();
          await retryWait.promise;
          await Promise.resolve();

          expect(instance.getStatus()).toBe("cancelled");
          expect(definition).toHaveBeenCalledOnce();
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        retryWait.resolve();
        waitSpy.mockRestore();
        warnSpy.mockRestore();
      }
    });

    it("defers to the watchdog when scheduler.wait is interrupted", async () => {
      const waitSpy = vi.spyOn(scheduler, "wait").mockRejectedValueOnce(new DOMException("interrupted", "AbortError"));
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const definition = vi.fn(() => ({
        next: vi.fn(async () => {
          throw Object.assign(new Error("definition unavailable"), { retryable: true });
        })
      }));
      const objectName = crypto.randomUUID();
      let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);

      try {
        await runInDurableObject(stub, async (instance, state) => {
          Object.defineProperty(instance, "definition", { configurable: true, value: definition });
          await instance.create();
          await expect
            .poll(() => warnSpy.mock.calls.some(([message]) => String(message).includes("retry wait was interrupted")))
            .toBe(true);

          expect(instance.getStatus()).toBe("running");
          expect(definition).toHaveBeenCalledOnce();
          const watchdogAt = await state.storage.getAlarm();
          expect(watchdogAt).not.toBeNull();
          expect(watchdogAt as number).toBeGreaterThanOrEqual(Date.now() + 29 * 60_000);
        });

        await evictDurableObject(stub);
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
        });
      } finally {
        waitSpy.mockRestore();
        warnSpy.mockRestore();
      }
    });

    it("recovers from forced teardown during scheduler.wait using the persisted watchdog", async () => {
      const retryWaitStarted = Promise.withResolvers<void>();
      const schedulerWait = scheduler.wait.bind(scheduler);
      const waitSpy = vi.spyOn(scheduler, "wait").mockImplementation((delay, options) => {
        retryWaitStarted.resolve();
        return schedulerWait(delay, options);
      });
      const randomSpy = vi.spyOn(Math, "random").mockReturnValue(0);
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const definition = vi.fn(() => ({
        next: vi.fn(async () => {
          throw Object.assign(new Error("definition unavailable"), { retryable: true });
        })
      }));
      const objectName = crypto.randomUUID();
      let stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);

      try {
        await runInDurableObject(stub, async (instance, state) => {
          Object.defineProperty(instance, "definition", { configurable: true, value: definition });
          await instance.create();
          await retryWaitStarted.promise;
          expect(instance.getStatus()).toBe("running");
          const watchdogAt = await state.storage.getAlarm();
          expect(watchdogAt).not.toBeNull();
          expect(watchdogAt as number).toBeGreaterThanOrEqual(Date.now() + 29 * 60_000);
        });

        await abortAllDurableObjects();
        stub = env.TEST_WORKFLOW_RUNTIME.getByName(objectName);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
        });

        expect(definition).toHaveBeenCalledOnce();
      } finally {
        waitSpy.mockRestore();
        randomSpy.mockRestore();
        warnSpy.mockRestore();
      }
    });

    it("preserves completion delivery when cancelled during scheduler.wait", async () => {
      const retryWait = Promise.withResolvers<void>();
      const waitSpy = vi.spyOn(scheduler, "wait").mockImplementation(() => retryWait.promise);
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const definition = vi.fn(() => ({
        next: vi.fn(async () => {
          throw Object.assign(new Error("definition unavailable"), { retryable: true });
        })
      }));
      const receivedEvents: Parameters<TestCompletionWorkflowRuntime["completion"]>[0][] = [];
      const completionSpy = vi
        .spyOn(TestCompletionWorkflowRuntime.prototype, "completion")
        .mockImplementation(async (event) => {
          receivedEvents.push(event);
        });

      try {
        const stub = env.TEST_COMPLETION_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          Object.defineProperty(instance, "definition", { configurable: true, value: definition });
          await instance.create();
          await expect.poll(() => waitSpy.mock.calls.length).toBe(1);
        });
        await stub.cancel("cancel during retry wait");
        retryWait.resolve();
        await retryWait.promise;
        await Promise.resolve();

        await runInDurableObject(stub, async (instance, state) => {
          expect(instance.getStatus()).toBe("cancelled");
          expect(definition).toHaveBeenCalledOnce();
          expect(completionSpy).toHaveBeenCalledOnce();
          expect(receivedEvents).toHaveLength(1);
          expect(receivedEvents[0]).toMatchObject({ status: "cancelled" });
          const delivery = state.storage.sql
            .exec<WorkflowEventDeliveryRow>("SELECT * FROM workflow_event_deliveries")
            .one();
          expect(delivery.delivered_at).toEqual(expect.any(Number));
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        retryWait.resolve();
        waitSpy.mockRestore();
        warnSpy.mockRestore();
        completionSpy.mockRestore();
      }
    });

    it("schedules the existing five-minute alarm for a retryable workflow-context call", async () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const contextSpy = vi.spyOn(WorkflowRuntimeContext.prototype, "getOrCreateRunStep").mockImplementationOnce(() => {
        throw Object.assign(new Error("workflow runtime unavailable"), {
          retryable: true
        });
      });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("context-retryable", async () => "recovered");
        });
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());

      try {
        const beforeCreate = Date.now();
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect
            .poll(() => warnSpy.mock.calls.some(([message]) => String(message).includes("retry scheduled")))
            .toBe(true);

          expect(instance.getStatus()).toBe("running");
          const retryAt = await state.storage.getAlarm();
          expect(retryAt).not.toBeNull();
          expect(retryAt as number).toBeGreaterThanOrEqual(beforeCreate + 5 * 60_000);
          expect(retryAt as number).toBeLessThanOrEqual(Date.now() + 5 * 60_000);
        });

        await evictDurableObject(stub);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
        });
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
        warnSpy.mockRestore();
      }
    });

    it("preserves an earlier context alarm when a committed operation returns a retryable transport error", async () => {
      let callbackRuns = 0;
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const handleRunAttemptFailed = WorkflowRuntimeContext.prototype.handleRunAttemptFailed;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "handleRunAttemptFailed")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, attemptId, result) {
          await handleRunAttemptFailed.call(this, stepId, attemptId, result);
          throw Object.assign(new Error("response lost after commit"), { retryable: true });
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run(
            "committed-before-transport-error",
            async () => {
              callbackRuns++;
              if (callbackRuns === 1) throw new Error("transient");
              return "recovered";
            },
            { maxAttempts: 2 }
          );
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        const beforeCreate = Date.now();
        let retryAt = 0;
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect
            .poll(() => warnSpy.mock.calls.some(([message]) => String(message).includes("retry scheduled")))
            .toBe(true);

          expect(instance.getStatus()).toBe("running");
          const step = instance.getSteps_experimental().find((s) => s.id === "committed-before-transport-error");
          expect(step?.type).toBe("run");
          const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
          const failed = attempts[attempts.length - 1];
          expect(failed?.state).toBe("failed");
          if (failed?.state !== "failed" || failed.nextAttemptAt === undefined) {
            throw new Error("Expected a retryable failed attempt.");
          }
          retryAt = failed.nextAttemptAt.getTime();
          expect(retryAt).toBeLessThan(beforeCreate + 5 * 60_000);
          expect(await state.storage.getAlarm()).toBe(retryAt);
        });

        await evictDurableObject(stub);
        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(retryAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);
          await runInDurableObject(stub, async (instance) => {
            await expect.poll(() => instance.getStatus()).toBe("completed");
          });
        } finally {
          dateNowSpy.mockRestore();
        }
        expect(callbackRuns).toBe(2);
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
        warnSpy.mockRestore();
      }
    });

    it("defers an overloaded workflow-context call to the watchdog even when it is also retryable", async () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const contextSpy = vi.spyOn(WorkflowRuntimeContext.prototype, "getOrCreateRunStep").mockImplementationOnce(() => {
        throw Object.assign(new Error("workflow runtime overloaded"), {
          retryable: true,
          overloaded: true
        });
      });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("context-overloaded", async () => "recovered");
        });
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());

      try {
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect
            .poll(() => warnSpy.mock.calls.some(([message]) => String(message).includes("runtime call was overloaded")))
            .toBe(true);

          expect(instance.getStatus()).toBe("running");
          const watchdogAt = await state.storage.getAlarm();
          expect(watchdogAt).not.toBeNull();
          expect(watchdogAt as number).toBeGreaterThanOrEqual(Date.now() + 29 * 60_000);
        });

        await evictDurableObject(stub);
        expect(await runDurableObjectAlarm(stub)).toBe(true);
        await runInDurableObject(stub, async (instance) => {
          await expect.poll(() => instance.getStatus()).toBe("completed");
        });
      } finally {
        contextSpy.mockRestore();
        executeSpy.mockRestore();
        warnSpy.mockRestore();
      }
    });
  });

  it("retry workflow completes after a transient failure on the first attempt", async () => {
    let attemptCount = 0;
    const nextSpy = vi.spyOn(TestWorkflowDefinition.prototype, "next");
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run("step-1", async () => {
          attemptCount++;
          if (attemptCount === 1) {
            throw new Error("transient");
          }
          return "ok";
        });
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };
        await instance.create();
        await expect(promise).resolves.toBe("completed");
        const steps = instance.getSteps_experimental();
        expect(steps).toHaveLength(1);
        const step = steps[0]!;
        expect(step.type).toBe("run");
        const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(attempts).toHaveLength(2);
        expect(attempts[0]).toMatchObject({ state: "failed" });
        expect(attempts[1]).toMatchObject({ state: "succeeded" });
        // First next(): failed attempt yields suspended. Retry alarm: second next() replays `execute()` and completes
        // the successful attempt in the same invocation (no extra immediate loop).
        expect(nextSpy).toHaveBeenCalledTimes(2);
      });
    } finally {
      executeSpy.mockRestore();
      nextSpy.mockRestore();
    }
  });

  it("wait workflow with an already-passed timeout fails without an inbound event", async () => {
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.wait("wait-1", "event-1", {
          timeoutAt: Date.now() - 60_000
        });
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("failed");
        const steps = instance.getSteps_experimental();
        expect(steps).toHaveLength(1);
        expect(steps[0]).toMatchObject({
          type: "wait",
          state: "timed_out"
        });
      });
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("treats timeoutAt: 0 as an immediate wait timeout", async () => {
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.wait("wait-at-epoch", "never-arrives", { timeoutAt: 0 });
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("failed");
        expect(instance.getSteps_experimental()).toEqual([
          expect.objectContaining({
            id: "wait-at-epoch",
            type: "wait",
            state: "timed_out",
            timeoutAt: new Date(0)
          })
        ]);
      });
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("records a non-finite run result as a non-retryable failure", async () => {
    let callbackRuns = 0;
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run("non-finite-result", async () => {
          callbackRuns++;
          return { nested: [1, Number.NaN, Number.POSITIVE_INFINITY] };
        });
      });

    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance, state) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("failed");

        const step = instance.getSteps_experimental().find((candidate) => candidate.id === "non-finite-result");
        expect(step?.type).toBe("run");
        const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(attempts).toHaveLength(1);
        expect(attempts[0]).toMatchObject({
          state: "failed",
          errorName: "NonRetryableStepError",
          nextAttemptAt: undefined
        });
        expect(attempts[0]?.state === "failed" ? attempts[0].errorMessage : "").toContain(
          "cannot contain NaN or infinite numbers"
        );
        expect(await state.storage.getAlarm()).toBeNull();
      });
      expect(callbackRuns).toBe(1);
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("records a cyclic run result as a non-retryable failure instead of leaving the attempt started", async () => {
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run("cyclic-result", async () => {
          // The recursive JsonObject type cannot express acyclicity, so this is accepted by the public callback type.
          const cyclic: JsonObject = {};
          cyclic.self = cyclic;
          return cyclic;
        });
      });

    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("failed");

        const step = instance.getSteps_experimental().find((candidate) => candidate.id === "cyclic-result");
        expect(step?.type).toBe("run");
        const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(attempts).toHaveLength(1);
        expect(attempts[0]).toMatchObject({
          state: "failed",
          errorName: "NonRetryableStepError",
          nextAttemptAt: undefined
        });
        expect(attempts[0]?.state === "failed" ? attempts[0].errorMessage : "").toContain(
          "cannot be durably serialized as JSON"
        );
      });
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("returns the canonical persisted run result on both first execution and replay", async () => {
    const observedSignedZero: boolean[] = [];
    let callbackRuns = 0;
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        const value = await this.run("signed-zero-result", async () => {
          callbackRuns++;
          return -0;
        });
        observedSignedZero.push(Object.is(value, -0));
        await this.run("after-signed-zero", async () => undefined);
      });

    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("completed");

        const step = instance.getSteps_experimental().find((candidate) => candidate.id === "signed-zero-result");
        expect(step?.type).toBe("run");
        const attempts = (step as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(attempts).toMatchObject([{ state: "succeeded", resultType: "json", resultJson: "0" }]);
      });

      expect(callbackRuns).toBe(1);
      expect(observedSignedZero).toEqual([false, false]);
    } finally {
      executeSpy.mockRestore();
    }
  });

  it("runs sequential sibling run() steps across multiple next() calls (one callback budget per level per next())", async () => {
    const nextSpy = vi.spyOn(TestWorkflowDefinition.prototype, "next");
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run("step-a", async () => 1);
        await this.run("step-b", async () => 2);
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("completed");

        const steps = instance.getSteps_experimental();
        expect(steps).toHaveLength(2);
        const firstStep = steps[0]!;
        expect(firstStep.id).toBe("step-a");
        expect(firstStep.type).toBe("run");
        const firstAttempts = (firstStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(firstAttempts[firstAttempts.length - 1]).toMatchObject({ state: "succeeded" });
        const secondStep = steps[1]!;
        expect(secondStep.id).toBe("step-b");
        expect(secondStep.type).toBe("run");
        const secondAttempts = (secondStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(secondAttempts[secondAttempts.length - 1]).toMatchObject({ state: "succeeded" });

        expect(nextSpy).toHaveBeenCalledTimes(2);
      });
    } finally {
      nextSpy.mockRestore();
      executeSpy.mockRestore();
    }
  });

  it("chains a run step and a zero-duration sleep in one next() invocation", async () => {
    const nextSpy = vi.spyOn(TestWorkflowDefinition.prototype, "next");
    const executeSpy = vi
      .spyOn(TestWorkflowDefinition.prototype, "execute")
      .mockImplementation(async function (this: TestWorkflowDefinition) {
        await this.run("before-sleep", async () => {
          await this.sleep("sleep-after-run", 0);
          return 1;
        });
      });
    try {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("completed");

        const steps = instance.getSteps_experimental();
        const before = steps.find((s) => s.id === "before-sleep");
        expect(before?.type).toBe("run");
        const beforeAttempts = (before as RunStep & { attempts: RunStepAttempt[] }).attempts;
        expect(beforeAttempts[beforeAttempts.length - 1]).toMatchObject({ state: "succeeded" });
        expect(steps.find((s) => s.id === "sleep-after-run")).toMatchObject({
          type: "sleep",
          state: "elapsed",
          parentStepId: "before-sleep"
        });
        // `sleep(0)` elapses via `ResumeImmediatelyError`, so the runtime runs one follow-up `next()` to finish `execute()`.
        expect(nextSpy).toHaveBeenCalledTimes(2);
      });
    } finally {
      nextSpy.mockRestore();
      executeSpy.mockRestore();
    }
  });

  describe("create()", () => {
    it("passes input through to the definition props on each execute()", async () => {
      const received: unknown[] = [];
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          received.push(this.ctx.props.input);
          await this.run("step-1", async () => 1);
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        const input = { key: "value", n: 42 };
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };
          await instance.create(input);
          await expect(promise).resolves.toBe("completed");
        });
        expect(received.length).toBeGreaterThanOrEqual(1);
        for (const row of received) {
          expect(row).toEqual(input);
        }
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("does not repin input after the workflow is initialized", async () => {
      const received: unknown[] = [];
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          received.push(this.ctx.props.input);
          await this.wait("wait-1", "event-done", {
            timeoutAt: Date.now() + 86_400_000
          });
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve: resolveRunning, promise: running } = Promise.withResolvers<WorkflowStatus>();
          const { resolve: resolveDone, promise: done } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") {
              resolveRunning(status);
            } else {
              resolveDone(status);
            }
          };
          const input = { key: "original" };
          await instance.create(input);
          await expect(running).resolves.toBe("running");

          await instance.create({ key: "ignored" });
          await instance.handleInboundEvent("event-done");
          await expect(done).resolves.toBe("completed");

          expect(received.length).toBeGreaterThanOrEqual(1);
          for (const row of received) {
            expect(row).toEqual(input);
          }
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("does not repin undefined input after the workflow is initialized", async () => {
      const received: unknown[] = [];
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          received.push(this.ctx.props.input);
          await this.wait("wait-1", "event-done", {
            timeoutAt: Date.now() + 86_400_000
          });
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve: resolveRunning, promise: running } = Promise.withResolvers<WorkflowStatus>();
          const { resolve: resolveDone, promise: done } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") {
              resolveRunning(status);
            } else {
              resolveDone(status);
            }
          };

          await instance.create();
          await expect(running).resolves.toBe("running");

          await instance.create({ key: "ignored" });
          await instance.handleInboundEvent("event-done");
          await expect(done).resolves.toBe("completed");

          expect(received.length).toBeGreaterThanOrEqual(1);
          for (const row of received) {
            expect(row).toBeUndefined();
          }
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("is a no-op when the workflow is already in a terminal state", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };
        await instance.create();
        await expect(promise).resolves.toBe("completed");
        expect(instance.getStatus()).toBe("completed");

        await instance.create();
        expect(instance.getStatus()).toBe("completed");
      });
    });
  });

  describe("nested run steps", () => {
    it("chains parentStepId across three nested run() levels", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("L0", async () => {
            await this.run("L1", async () => {
              await this.run("L2", async () => "deep");
            });
          });
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          for (const id of ["L0", "L1", "L2"] as const) {
            const row = steps.find((s) => s.id === id);
            expect(row?.type).toBe("run");
            const attempts = (row as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(attempts.filter((a) => a.state === "failed")).toHaveLength(0);
            expect(attempts[attempts.length - 1]).toMatchObject({ state: "succeeded" });
          }
          expect(steps.find((s) => s.id === "L0")).toMatchObject({ parentStepId: null });
          expect(steps.find((s) => s.id === "L1")).toMatchObject({ parentStepId: "L0" });
          expect(steps.find((s) => s.id === "L2")).toMatchObject({ parentStepId: "L1" });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("assigns parentStepId null to a root run() that follows a completed nested run()", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("nest-outer", async () => {
            await this.run("nest-inner", async () => 1);
          });
          await this.run("root-after", async () => 2);
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          const rootAfter = steps.find((s) => s.id === "root-after");
          expect(rootAfter?.type).toBe("run");
          expect(rootAfter).toMatchObject({ parentStepId: null });
          const raa = (rootAfter as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(raa[raa.length - 1]).toMatchObject({ state: "succeeded" });
          expect(steps.find((s) => s.id === "nest-inner")).toMatchObject({
            parentStepId: "nest-outer"
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("Promise.all: parallel branches each with nested run() complete and keep distinct parent chains", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await Promise.all([
            this.run("branch-a", async () => {
              const v = await this.run("branch-a-inner", async () => "a");
              return v;
            }),
            this.run("branch-b", async () => {
              const v = await this.run("branch-b-inner", async () => "b");
              return v;
            })
          ]);
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          for (const id of ["branch-a", "branch-a-inner", "branch-b", "branch-b-inner"] as const) {
            const row = steps.find((s) => s.id === id);
            expect(row?.type).toBe("run");
            const attempts = (row as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(attempts.filter((a) => a.state === "failed")).toHaveLength(0);
            expect(attempts[attempts.length - 1]).toMatchObject({ state: "succeeded" });
          }
          expect(steps.find((s) => s.id === "branch-a-inner")).toMatchObject({ parentStepId: "branch-a" });
          expect(steps.find((s) => s.id === "branch-b-inner")).toMatchObject({ parentStepId: "branch-b" });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("Promise.allSettled: nested run() in one branch still records parentStepId when the other branch waits", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await Promise.allSettled([
            this.run("nested-branch", async () => {
              await this.run("nested-branch-inner", async () => 99);
            }),
            this.wait("parallel-wait-nested", "evt-nested-parallel", {
              timeoutAt: Date.now() + 86_400_000
            })
          ]);
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          const nbInner = steps.find((s) => s.id === "nested-branch-inner");
          expect(nbInner?.type).toBe("run");
          expect(nbInner).toMatchObject({ parentStepId: "nested-branch" });
          const nbia = (nbInner as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(nbia[nbia.length - 1]).toMatchObject({ state: "succeeded" });
          expect(steps.find((s) => s.id === "parallel-wait-nested")).toMatchObject({
            type: "wait",
            state: "waiting"
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("records parentStepId on sleep() nested inside run()", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("outer-sleep", async () => {
            await this.sleep("deep-sleep", 0);
          });
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          expect(steps.find((s) => s.id === "deep-sleep")).toMatchObject({
            type: "sleep",
            parentStepId: "outer-sleep",
            state: "elapsed"
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("records parentStepId on wait() nested inside run() and completes after handleInboundEvent", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("outer-wait", async () => {
            await this.wait("deep-wait", "deep-event", {
              timeoutAt: Date.now() + 86_400_000
            });
          });
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();

          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "deep-wait");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          const stepsWaiting = instance.getSteps_experimental();
          expect(stepsWaiting.find((s) => s.id === "deep-wait")).toMatchObject({
            type: "wait",
            parentStepId: "outer-wait",
            state: "waiting"
          });

          await instance.handleInboundEvent("deep-event", { ok: true });
          await expect(promise).resolves.toBe("completed");

          expect(instance.getSteps_experimental().find((s) => s.id === "deep-wait")).toMatchObject({
            type: "wait",
            parentStepId: "outer-wait",
            state: "satisfied",
            payload: { ok: true }
          });
          const outerWait = instance.getSteps_experimental().find((s) => s.id === "outer-wait");
          expect(outerWait?.type).toBe("run");
          const owa = (outerWait as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(owa[owa.length - 1]).toMatchObject({ state: "succeeded" });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("fails a parent run without retrying when a nested wait times out", async () => {
      let outerCallbackRuns = 0;
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run(
            "timeout-outer",
            async () => {
              outerCallbackRuns++;
              await this.wait("timeout-inner", "never-arrives", { timeoutAt: Date.now() - 1 });
            },
            { maxAttempts: 5 }
          );
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("failed");
          expect(outerCallbackRuns).toBe(1);
          expect(instance.getSteps_experimental().find((step) => step.id === "timeout-inner")).toMatchObject({
            type: "wait",
            state: "timed_out",
            parentStepId: "timeout-outer"
          });

          const outer = instance.getSteps_experimental().find((step) => step.id === "timeout-outer");
          expect(outer?.type).toBe("run");
          const attempts = (outer as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(attempts).toHaveLength(1);
          expect(attempts[0]).toMatchObject({
            state: "failed",
            errorName: "WaitStepTimedOutError",
            nextAttemptAt: undefined
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("fails the workflow when a nested run() throws NonRetryableStepError", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("fail-outer", async () => {
            await this.run("fail-inner", async () => {
              throw new NonRetryableStepError("inner only");
            });
          });
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("failed");

          const steps = instance.getSteps_experimental();
          const fi = steps.find((s) => s.id === "fail-inner");
          expect(fi?.type).toBe("run");
          const fia = (fi as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(fia[fia.length - 1]).toMatchObject({ state: "failed", errorName: "NonRetryableStepError" });
          const fo = steps.find((s) => s.id === "fail-outer");
          expect(fo?.type).toBe("run");
          const foa = (fo as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(foa[foa.length - 1]).toMatchObject({ state: "failed", errorName: "NonRetryableStepError" });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    describe("nested error propagation", () => {
      it("preserves the parent attempt when a committed nested context response is lost", async () => {
        let outerCallbackRuns = 0;
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        const getOrCreateSleepStep = WorkflowRuntimeContext.prototype.getOrCreateSleepStep;
        const contextSpy = vi
          .spyOn(WorkflowRuntimeContext.prototype, "getOrCreateSleepStep")
          .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, options) {
            await getOrCreateSleepStep.call(this, stepId, options);
            throw Object.assign(new Error("nested response lost after commit"), { retryable: true });
          });
        const executeSpy = vi
          .spyOn(TestWorkflowDefinition.prototype, "execute")
          .mockImplementation(async function (this: TestWorkflowDefinition) {
            await this.run(
              "transport-outer",
              async () => {
                outerCallbackRuns++;
                await this.sleep("transport-inner", 60_000);
              },
              { maxAttempts: 1 }
            );
          });

        try {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          let wakeAt = 0;
          await runInDurableObject(stub, async (instance) => {
            await instance.create();
            await expect
              .poll(() => warnSpy.mock.calls.some(([message]) => String(message).includes("retry scheduled")))
              .toBe(true);

            const beforeRecovery = instance.getSteps_experimental();
            expect(beforeRecovery.find((step) => step.id === "transport-inner")).toMatchObject({
              type: "sleep",
              state: "waiting",
              parentStepId: "transport-outer"
            });
            const inner = beforeRecovery.find((step) => step.id === "transport-inner");
            if (inner?.type !== "sleep" || inner.state !== "waiting") {
              throw new Error("Expected a waiting nested sleep step.");
            }
            wakeAt = inner.wakeAt.getTime();
            expect(beforeRecovery.find((step) => step.id === "transport-outer")).toMatchObject({
              type: "run",
              attempts: [{ state: "started" }]
            });
          });

          const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(wakeAt);
          try {
            expect(await runDurableObjectAlarm(stub)).toBe(true);
            await runInDurableObject(stub, async (instance) => {
              await expect.poll(() => instance.getStatus()).toBe("completed");
              expect(instance.getSteps_experimental().find((step) => step.id === "transport-outer")).toMatchObject({
                type: "run",
                attempts: [{ state: "succeeded" }]
              });
            });
          } finally {
            dateNowSpy.mockRestore();
          }
          expect(outerCallbackRuns).toBe(3);
        } finally {
          contextSpy.mockRestore();
          executeSpy.mockRestore();
          warnSpy.mockRestore();
        }
      });

      it("does not record attempt_failed on the outer run when the inner run suspends for retry, then completes", async () => {
        let innerAttempts = 0;
        const executeSpy = vi
          .spyOn(TestWorkflowDefinition.prototype, "execute")
          .mockImplementation(async function (this: TestWorkflowDefinition) {
            await this.run("suspend-outer", async () => {
              await this.run(
                "suspend-inner",
                async () => {
                  innerAttempts += 1;
                  if (innerAttempts < 2) {
                    throw new Error("retryable inner");
                  }
                  return "done";
                },
                { maxAttempts: 3 }
              );
            });
          });
        try {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance) => {
            const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
            instance.onStatusChange = (status) => {
              if (status === "running") return;
              resolve(status);
            };

            await instance.create();
            await expect(promise).resolves.toBe("completed");

            expect(innerAttempts).toBe(2);
            const outerRow = instance.getSteps_experimental().find((s) => s.id === "suspend-outer");
            expect(outerRow?.type).toBe("run");
            const oa = (outerRow as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(oa.filter((a) => a.state === "failed")).toHaveLength(0);
            expect(oa[oa.length - 1]).toMatchObject({ state: "succeeded" });
            const innerRow = instance.getSteps_experimental().find((s) => s.id === "suspend-inner");
            expect(innerRow?.type).toBe("run");
            const ia = (innerRow as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(ia[ia.length - 1]).toMatchObject({ state: "succeeded" });
          });
        } finally {
          executeSpy.mockRestore();
        }
      });

      it("fails the workflow when the inner run exhausts maxAttempts", async () => {
        let outerCallbackRuns = 0;
        const executeSpy = vi
          .spyOn(TestWorkflowDefinition.prototype, "execute")
          .mockImplementation(async function (this: TestWorkflowDefinition) {
            await this.run("ex-outer", async () => {
              outerCallbackRuns++;
              await this.run(
                "ex-inner",
                async () => {
                  throw new Error("always fail");
                },
                { maxAttempts: 1 }
              );
            });
          });
        try {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance) => {
            const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
            instance.onStatusChange = (status) => {
              if (status === "running") return;
              resolve(status);
            };

            await instance.create();
            await expect(promise).resolves.toBe("failed");

            const steps = instance.getSteps_experimental();
            const exInner = steps.find((s) => s.id === "ex-inner");
            expect(exInner?.type).toBe("run");
            const exIa = (exInner as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(exIa[exIa.length - 1]).toMatchObject({
              state: "failed",
              errorName: "Error",
              errorMessage: "Error: always fail"
            });
            const exOuter = steps.find((s) => s.id === "ex-outer");
            expect(exOuter?.type).toBe("run");
            const exOa = (exOuter as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(exOa).toHaveLength(1);
            expect(exOa[0]).toMatchObject({
              state: "failed",
              errorName: "MaxAttemptsExceededError",
              errorMessage: "MaxAttemptsExceededError: Run step 'ex-inner' exhausted its configured attempts."
            });
            expect(outerCallbackRuns).toBe(1);
          });
        } finally {
          executeSpy.mockRestore();
        }
      });

      it("records the outer run failure when the outer callback throws after the inner run succeeded", async () => {
        const executeSpy = vi
          .spyOn(TestWorkflowDefinition.prototype, "execute")
          .mockImplementation(async function (this: TestWorkflowDefinition) {
            await this.run("post-outer", async () => {
              await this.run("post-inner", async () => 42);
              throw new Error("outer-only failure");
            });
          });
        try {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance) => {
            const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
            instance.onStatusChange = (status) => {
              if (status === "running") return;
              resolve(status);
            };

            await instance.create();
            await expect(promise).resolves.toBe("failed");

            const steps = instance.getSteps_experimental();
            const pi = steps.find((s) => s.id === "post-inner");
            expect(pi?.type).toBe("run");
            const pia = (pi as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(pia[pia.length - 1]).toMatchObject({ state: "succeeded" });
            const po = steps.find((s) => s.id === "post-outer");
            expect(po?.type).toBe("run");
            const poa = (po as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(poa[poa.length - 1]).toMatchObject({
              state: "failed",
              errorMessage: expect.stringContaining("outer-only failure")
            });
          });
        } finally {
          executeSpy.mockRestore();
        }
      });

      it("does not record attempt_failed on a root run that only suspends via nested wait (child step explains suspend)", async () => {
        const executeSpy = vi
          .spyOn(TestWorkflowDefinition.prototype, "execute")
          .mockImplementation(async function (this: TestWorkflowDefinition) {
            await this.run("root-wait-run", async () => {
              await this.wait("root-deep-wait", "root-deep-ev", {
                timeoutAt: Date.now() + 86_400_000
              });
            });
          });
        try {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
            instance.onStatusChange = (status) => {
              if (status === "running") return;
              resolve(status);
            };

            await instance.create();
            await expect
              .poll(() => {
                const step = instance.getSteps_experimental().find((s) => s.id === "root-deep-wait");
                return step?.type === "wait" ? step.state : undefined;
              })
              .toBe("waiting");

            const waitStep = instance.getSteps_experimental().find((s) => s.id === "root-deep-wait");
            expect(waitStep?.type).toBe("wait");
            if (waitStep?.type !== "wait" || waitStep.state !== "waiting" || waitStep.timeoutAt === undefined) {
              throw new Error("Expected a waiting nested wait step with a timeout.");
            }
            await expect.poll(() => state.storage.getAlarm()).toBe(waitStep.timeoutAt.getTime());

            const rootRunBefore = instance.getSteps_experimental().find((s) => s.id === "root-wait-run");
            expect(rootRunBefore?.type).toBe("run");
            const rba = (rootRunBefore as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(rba.filter((a) => a.state === "failed")).toHaveLength(0);

            await instance.handleInboundEvent("root-deep-ev", true);
            await expect(promise).resolves.toBe("completed");

            const rootRunAfter = instance.getSteps_experimental().find((s) => s.id === "root-wait-run");
            expect(rootRunAfter?.type).toBe("run");
            const raa = (rootRunAfter as RunStep & { attempts: RunStepAttempt[] }).attempts;
            expect(raa.filter((a) => a.state === "failed")).toHaveLength(0);
            expect(raa[raa.length - 1]).toMatchObject({ state: "succeeded" });
          });
        } finally {
          executeSpy.mockRestore();
        }
      });
    });
  });

  describe("Promise.allSettled", () => {
    it("swallows SuspendWorkflowError so the workflow completes while a wait step is still waiting", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await Promise.allSettled([
            this.run("parallel-run", async () => 1),
            this.wait("parallel-wait", "parallel-event", {
              timeoutAt: Date.now() + 86_400_000
            })
          ]);
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          expect(steps).toHaveLength(2);
          const prun = steps.find((s) => s.id === "parallel-run");
          expect(prun?.type).toBe("run");
          const pra = (prun as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(pra[pra.length - 1]).toMatchObject({ state: "succeeded" });
          expect(steps.find((s) => s.id === "parallel-wait")).toMatchObject({
            type: "wait",
            state: "waiting"
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("swallows NonRetryableStepError so the workflow completes despite a failed run step", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await Promise.allSettled([
            this.run("parallel-fail", async () => {
              throw new NonRetryableStepError("branch failed");
            }),
            this.run("parallel-ok", async () => 1)
          ]);
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          expect(steps).toHaveLength(2);
          const pf = steps.find((s) => s.id === "parallel-fail");
          expect(pf?.type).toBe("run");
          const pfa = (pf as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(pfa[pfa.length - 1]).toMatchObject({ state: "failed", errorName: "NonRetryableStepError" });
          const pok = steps.find((s) => s.id === "parallel-ok");
          expect(pok?.type).toBe("run");
          const poka = (pok as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(poka[poka.length - 1]).toMatchObject({ state: "succeeded" });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("forwards suspend when the caller rethrows after inspecting settled results", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const results = await Promise.allSettled([
            this.run("allsettled-rerun-run", async () => 1),
            this.wait("allsettled-rerun-wait", "allsettled-rerun-event", {
              timeoutAt: Date.now() + 86_400_000
            })
          ]);
          for (const r of results) {
            if (r.status === "rejected") {
              throw r.reason;
            }
          }
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const terminalStatuses: WorkflowStatus[] = [];
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            terminalStatuses.push(status);
          };

          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          expect(terminalStatuses).toHaveLength(0);

          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "allsettled-rerun-wait");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          const steps = instance.getSteps_experimental();
          expect(steps.find((s) => s.id === "allsettled-rerun-wait")).toMatchObject({
            type: "wait",
            state: "waiting"
          });
          // Unlike `Promise.all`, `allSettled` waits for every branch before returning, so the run can finish
          // durably before we rethrow the `wait()` rejection.
          const asRun = steps.find((s) => s.id === "allsettled-rerun-run");
          expect(asRun?.type).toBe("run");
          const asra = (asRun as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(asra[asra.length - 1]).toMatchObject({ state: "succeeded" });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });
  });

  describe("Promise.all", () => {
    it("propagates SuspendWorkflowError so the workflow stays running with a waiting step", async () => {
      const runStarted = Promise.withResolvers<void>();
      const releaseRun = Promise.withResolvers<void>();
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await Promise.all([
            this.run("parallel-run", async () => {
              runStarted.resolve();
              await releaseRun.promise;
              return 1;
            }),
            this.wait("parallel-wait", "parallel-event", {
              timeoutAt: Date.now() + 86_400_000
            })
          ]);
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          const terminalStatuses: WorkflowStatus[] = [];
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            terminalStatuses.push(status);
          };

          await instance.create();
          await runStarted.promise;
          await expect.poll(() => instance.getStatus()).toBe("running");
          expect(terminalStatuses).toHaveLength(0);

          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "parallel-wait");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          const steps = instance.getSteps_experimental();
          expect(steps.find((s) => s.id === "parallel-wait")).toMatchObject({
            type: "wait",
            state: "waiting"
          });

          const parRun = steps.find((s) => s.id === "parallel-run");
          expect(parRun?.type).toBe("run");
          const paa = (parRun as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(paa[paa.length - 1]).toMatchObject({ state: "started" });
          const parallelWait = steps.find((s) => s.id === "parallel-wait");
          expect(parallelWait?.type).toBe("wait");
          if (
            parallelWait?.type !== "wait" ||
            parallelWait.state !== "waiting" ||
            parallelWait.timeoutAt === undefined
          ) {
            throw new Error("Expected a waiting parallel wait step with a timeout.");
          }
          const watchdogAt = await state.storage.getAlarm();
          expect(watchdogAt).not.toBeNull();
          expect(watchdogAt as number).toBeLessThan(parallelWait.timeoutAt.getTime());
          expect(watchdogAt as number).toBeGreaterThanOrEqual(Date.now() + 29 * 60_000);
          expect(watchdogAt as number).toBeLessThanOrEqual(Date.now() + 31 * 60_000);
          expect(instance.getStatus()).toBe("running");
          expect(terminalStatuses).toHaveLength(0);
        });
      } finally {
        releaseRun.resolve();
        executeSpy.mockRestore();
      }
    });
  });

  describe("completion()", () => {
    it("does not create a delivery or alarm when the runtime has no completion handler", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());

      await runInDurableObject(stub, async (instance, state) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status !== "running") resolve(status);
        };

        await instance.create();
        await expect(promise).resolves.toBe("completed");

        expect(state.storage.sql.exec("SELECT event_id FROM workflow_event_deliveries").toArray()).toHaveLength(0);
        expect(await state.storage.getAlarm()).toBeNull();
      });
    });

    it("delivers and acknowledges a completed workflow outcome", async () => {
      const receivedEvents: Parameters<TestCompletionWorkflowRuntime["completion"]>[0][] = [];
      const completionSpy = vi
        .spyOn(TestCompletionWorkflowRuntime.prototype, "completion")
        .mockImplementation(async (event) => {
          receivedEvents.push(event);
        });

      try {
        const stub = env.TEST_COMPLETION_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        const input = { orderId: "order-123", amount: 42 };

        await runInDurableObject(stub, async (instance, state) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status !== "running") resolve(status);
          };

          await instance.create(input);
          await expect(promise).resolves.toBe("completed");
          await expect.poll(() => receivedEvents.length).toBe(1);

          const workflowEvent = state.storage.sql
            .exec<{ id: number; recorded_at: number }>(
              "SELECT id, recorded_at FROM workflow_events WHERE type = 'completed'"
            )
            .one();
          const delivery = state.storage.sql
            .exec<WorkflowEventDeliveryRow>("SELECT * FROM workflow_event_deliveries")
            .one();
          const received = receivedEvents[0]!;

          expect(received).toEqual({
            id: `${state.id.toString()}:${workflowEvent.id}`,
            status: "completed",
            finishedAt: new Date(workflowEvent.recorded_at)
          });
          expect(delivery).toMatchObject({
            event_id: workflowEvent.id,
            attempts: 1,
            last_error: null
          });
          expect(delivery.delivered_at).toEqual(expect.any(Number));
          expect(await state.storage.getAlarm()).toBeNull();

          await instance.create(input);
          expect(receivedEvents).toHaveLength(1);
          expect(state.storage.sql.exec("SELECT event_id FROM workflow_event_deliveries").toArray()).toHaveLength(1);
        });
      } finally {
        completionSpy.mockRestore();
      }
    });

    it("keeps the workflow completed and retries a rejected delivery after eviction", async () => {
      const receivedEventIds: string[] = [];
      const completionSpy = vi
        .spyOn(TestCompletionWorkflowRuntime.prototype, "completion")
        .mockImplementation(async (event) => {
          receivedEventIds.push(event.id);
          if (receivedEventIds.length === 1) throw new Error("projection unavailable");
        });

      try {
        const stub = env.TEST_COMPLETION_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        let retryAt = 0;

        await runInDurableObject(stub, async (instance, state) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status !== "running") resolve(status);
          };

          await instance.create({ workflow: "retry-completion" });
          await expect(promise).resolves.toBe("completed");
          await expect.poll(() => receivedEventIds.length).toBe(1);

          expect(instance.getStatus()).toBe("completed");
          expect(instance.getWorkflowEvents_experimental().map((event) => event.type)).toEqual([
            "created",
            "started",
            "completed"
          ]);

          const delivery = state.storage.sql
            .exec<WorkflowEventDeliveryRow>("SELECT * FROM workflow_event_deliveries")
            .one();
          expect(delivery).toMatchObject({
            attempts: 1,
            delivered_at: null,
            last_error: "Error: projection unavailable"
          });
          retryAt = delivery.next_attempt_at;
          expect(await state.storage.getAlarm()).toBe(retryAt);
        });

        await evictDurableObject(stub);
        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(retryAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);
        } finally {
          dateNowSpy.mockRestore();
        }

        await runInDurableObject(stub, async (instance, state) => {
          expect(instance.getStatus()).toBe("completed");
          expect(receivedEventIds).toHaveLength(2);
          expect(new Set(receivedEventIds).size).toBe(1);

          const delivery = state.storage.sql
            .exec<WorkflowEventDeliveryRow>("SELECT * FROM workflow_event_deliveries")
            .one();
          expect(delivery.attempts).toBe(2);
          expect(delivery.delivered_at).toEqual(expect.any(Number));
          expect(delivery.last_error).toBeNull();
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        completionSpy.mockRestore();
      }
    });

    it("redelivers after forced teardown while the completion handler is still running", async () => {
      const firstDeliveryStarted = Promise.withResolvers<void>();
      const interruptedDelivery = Promise.withResolvers<never>();
      const receivedEvents: Parameters<TestCompletionWorkflowRuntime["completion"]>[0][] = [];
      const completionSpy = vi
        .spyOn(TestCompletionWorkflowRuntime.prototype, "completion")
        .mockImplementation(async (event) => {
          receivedEvents.push(event);
          if (receivedEvents.length === 1) {
            firstDeliveryStarted.resolve();
            return await interruptedDelivery.promise;
          }
        });

      try {
        const objectName = crypto.randomUUID();
        let stub = env.TEST_COMPLETION_WORKFLOW_RUNTIME.getByName(objectName);
        let visibilityTimeoutAt = 0;
        const input = { workflow: "interrupted-completion" };

        await runInDurableObject(stub, async (instance, state) => {
          await instance.create(input);
          await firstDeliveryStarted.promise;

          expect(instance.getStatus()).toBe("completed");
          const delivery = state.storage.sql
            .exec<WorkflowEventDeliveryRow>("SELECT * FROM workflow_event_deliveries")
            .one();
          expect(delivery.attempts).toBe(1);
          expect(delivery.delivered_at).toBeNull();
          visibilityTimeoutAt = delivery.next_attempt_at;
          expect(await state.storage.getAlarm()).toBe(visibilityTimeoutAt);
        });

        await abortAllDurableObjects();
        stub = env.TEST_COMPLETION_WORKFLOW_RUNTIME.getByName(objectName);

        const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(visibilityTimeoutAt);
        try {
          expect(await runDurableObjectAlarm(stub)).toBe(true);
        } finally {
          dateNowSpy.mockRestore();
        }

        expect(receivedEvents).toHaveLength(2);
        expect(receivedEvents[1]).toEqual(receivedEvents[0]);
        expect(receivedEvents[1]?.status).toBe("completed");

        await runInDurableObject(stub, async (instance, state) => {
          expect(instance.getStatus()).toBe("completed");
          const delivery = state.storage.sql
            .exec<WorkflowEventDeliveryRow>("SELECT * FROM workflow_event_deliveries")
            .one();
          expect(delivery.attempts).toBe(2);
          expect(delivery.delivered_at).toEqual(expect.any(Number));
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        completionSpy.mockRestore();
      }
    });

    it("delivers failed and cancelled workflow outcomes", async () => {
      const receivedEvents: Parameters<TestCompletionWorkflowRuntime["completion"]>[0][] = [];
      const completionSpy = vi
        .spyOn(TestCompletionWorkflowRuntime.prototype, "completion")
        .mockImplementation(async (event) => {
          receivedEvents.push(event);
        });
      const executeSpy = vi.spyOn(TestWorkflowDefinition.prototype, "execute").mockImplementation(async () => {
        throw new Error("fatal workflow error");
      });

      try {
        const failedStub = env.TEST_COMPLETION_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(failedStub, async (instance, state) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status !== "running") resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("failed");
          await expect.poll(() => receivedEvents.length).toBe(1);

          const workflowEvent = state.storage.sql
            .exec<{ id: number; recorded_at: number }>(
              "SELECT id, recorded_at FROM workflow_events WHERE type = 'failed'"
            )
            .one();
          expect(receivedEvents[0]).toEqual({
            id: `${state.id.toString()}:${workflowEvent.id}`,
            status: "failed",
            finishedAt: new Date(workflowEvent.recorded_at)
          });
          expect(
            state.storage.sql.exec<WorkflowEventDeliveryRow>("SELECT * FROM workflow_event_deliveries").one()
              .delivered_at
          ).toEqual(expect.any(Number));
          expect(await state.storage.getAlarm()).toBeNull();
        });

        const cancelledStub = env.TEST_COMPLETION_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(cancelledStub, async (instance, state) => {
          await instance.cancel("not needed");

          const workflowEvent = state.storage.sql
            .exec<{ id: number; recorded_at: number }>(
              "SELECT id, recorded_at FROM workflow_events WHERE type = 'cancelled'"
            )
            .one();
          expect(receivedEvents[1]).toEqual({
            id: `${state.id.toString()}:${workflowEvent.id}`,
            status: "cancelled",
            finishedAt: new Date(workflowEvent.recorded_at)
          });
          expect(
            state.storage.sql.exec<WorkflowEventDeliveryRow>("SELECT * FROM workflow_event_deliveries").one()
              .delivered_at
          ).toEqual(expect.any(Number));
          expect(await state.storage.getAlarm()).toBeNull();
        });

        expect(completionSpy).toHaveBeenCalledTimes(2);
      } finally {
        executeSpy.mockRestore();
        completionSpy.mockRestore();
      }
    });
  });

  describe("alarm()", () => {
    it("no alarm is scheduled after the workflow has completed", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange = (status) => {
          if (status === "running") return;
          resolve(status);
        };
        await instance.create();
        await expect(promise).resolves.toBe("completed");
      });

      expect(await runDurableObjectAlarm(stub)).toBe(false);
    });

    it("no alarm is scheduled after the workflow has failed", async () => {
      const executeSpy = vi.spyOn(TestWorkflowDefinition.prototype, "execute").mockImplementation(async function () {
        throw new Error("fatal");
      });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };
          await instance.create();
          await expect(promise).resolves.toBe("failed");
        });

        expect(await runDurableObjectAlarm(stub)).toBe(false);
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("no alarm is scheduled after the workflow has been cancelled", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        await instance.cancel("test reason");
      });

      expect(await runDurableObjectAlarm(stub)).toBe(false);
    });
  });

  describe("pause and resume", () => {
    it("pause() transitions running workflow to paused and clears alarm", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-1", "never-arrives", {
            timeoutAt: Date.now() + 86_400_000
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-1");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          const step = instance.getSteps_experimental().find((step) => step.id === "wait-1");
          expect(step?.type).toBe("wait");
          if (step?.type !== "wait" || step.state !== "waiting" || step.timeoutAt === undefined) {
            throw new Error("Expected a waiting wait step with a timeout.");
          }
          await expect.poll(() => state.storage.getAlarm()).toBe(step.timeoutAt.getTime());

          await instance.pause();
          expect(instance.getStatus()).toBe("paused");
        });

        expect(await runDurableObjectAlarm(stub)).toBe(false);
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("does not recreate alarms when in-flight step transitions commit after pause()", async () => {
      const runStarted = Promise.withResolvers<void>();
      const releaseRun = Promise.withResolvers<void>();
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("run-completed-while-paused", async () => {
            runStarted.resolve();
            await releaseRun.promise;
            return "done";
          });
          await this.sleep("sleep-created-while-paused", 0);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await runStarted.promise;

          await instance.pause();
          expect(instance.getStatus()).toBe("paused");
          expect(await state.storage.getAlarm()).toBeNull();

          releaseRun.resolve();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "run-completed-while-paused");
              if (step?.type !== "run") return undefined;
              return step.attempts.at(-1)?.state;
            })
            .toBe("succeeded");
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "sleep-created-while-paused");
              return step?.type === "sleep" ? step.state : undefined;
            })
            .toBe("elapsed");

          expect(instance.getStatus()).toBe("paused");
          expect(await state.storage.getAlarm()).toBeNull();

          await instance.resume();
          await expect.poll(() => instance.getStatus()).toBe("completed");
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        releaseRun.resolve();
        executeSpy.mockRestore();
      }
    });

    it("pause() fires onStatusChange with 'paused'", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-1", 60_000);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<void>();
          instance.onStatusChange = (status) => {
            if (status === "paused") resolve();
          };

          await instance.create();
          await instance.pause();
          await promise;
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("pause() is a no-op when workflow is pending", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        expect(instance.getStatus()).toBe("pending");
        await instance.pause();
        expect(instance.getStatus()).toBe("pending");
      });
    });

    it("pause() is a no-op when workflow is already paused", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-1", 60_000);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          await instance.pause();
          expect(instance.getStatus()).toBe("paused");

          await instance.pause();
          expect(instance.getStatus()).toBe("paused");
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("resume() transitions paused workflow back to running", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("step-1", async () => 1);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          await instance.pause();
          expect(instance.getStatus()).toBe("paused");

          await instance.resume();
          await expect(promise).resolves.toBe("completed");
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("resume() throws when workflow is not paused", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        expect(instance.getStatus()).toBe("pending");
        await expect(instance.resume()).rejects.toThrow(
          "Cannot resume workflow: expected status 'paused' but got 'pending'."
        );
      });
    });

    it("resume() throws when workflow is running", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-1", 60_000);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          await expect(instance.resume()).rejects.toThrow(
            "Cannot resume workflow: expected status 'paused' but got 'running'."
          );
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("handleInboundEvent() queues event without satisfying wait step when paused", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-1", "event-1");
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          await instance.create();

          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-1");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          await instance.pause();

          expect(instance.getSteps_experimental().find((s) => s.id === "wait-1")).toMatchObject({
            type: "wait",
            state: "waiting"
          });

          await instance.handleInboundEvent("event-1", { data: "test" });

          expect(instance.getSteps_experimental().find((s) => s.id === "wait-1")).toMatchObject({
            type: "wait",
            state: "waiting"
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("queued event is consumed after resume()", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-1", "event-1");
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.create();
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-1");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");
          await instance.pause();

          await instance.handleInboundEvent("event-1", { data: "test" });

          await instance.resume();
          await expect(promise).resolves.toBe("completed");

          expect(instance.getSteps_experimental().find((s) => s.id === "wait-1")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: { data: "test" }
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("alarm() does not call run() when paused", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-1", "never-arrives", {
            timeoutAt: Date.now() + 86_400_000
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-1");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          await instance.pause();
          expect(instance.getStatus()).toBe("paused");
        });

        expect(await runDurableObjectAlarm(stub)).toBe(false);

        await runInDurableObject(stub, async (instance) => {
          expect(instance.getStatus()).toBe("paused");
        });
      } finally {
        executeSpy.mockRestore();
      }
    });
  });

  describe("handleInboundEvent()", () => {
    it("does not satisfy an expired wait with an event that arrived after its deadline", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance, state) => {
        const context = createRunningWorkflowRuntimeContext(state.storage);
        const waitStepId = createWaitStepId("expired-wait");
        const timeoutAt = new Date(Date.now() - 1_000);
        await context.getOrCreateWaitStep(waitStepId, {
          eventName: "deadline-event",
          timeoutAt,
          parentStepId: null
        });

        await instance.handleInboundEvent("deadline-event", { late: true });

        const replayed = await context.getOrCreateWaitStep(waitStepId, {
          eventName: "deadline-event",
          parentStepId: null
        });
        expect(replayed).toMatchObject({
          id: "expired-wait",
          state: "waiting",
          timeoutAt
        });
        expect(
          state.storage.sql
            .exec<{ claimed_by: string | null }>(
              "SELECT claimed_by FROM inbound_events WHERE event_name = ?",
              "deadline-event"
            )
            .one()
        ).toEqual({ claimed_by: null });
      });
    });

    it("does not lose an inbound event that arrives while its wait step is being created", async () => {
      const contextCallStarted = Promise.withResolvers<void>();
      const createWaitStep = Promise.withResolvers<void>();
      const getOrCreateWaitStep = WorkflowRuntimeContext.prototype.getOrCreateWaitStep;
      const contextSpy = vi
        .spyOn(WorkflowRuntimeContext.prototype, "getOrCreateWaitStep")
        .mockImplementationOnce(async function (this: WorkflowRuntimeContext, stepId, options) {
          contextCallStarted.resolve();
          await createWaitStep.promise;
          return await getOrCreateWaitStep.call(this, stepId, options);
        });
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const payload = await this.wait<{ value: number }>("wait-created-during-inbound", "racing-event", {
            timeoutAt: Date.now() + 60_000
          });
          expect(payload).toEqual({ value: 42 });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          await instance.create();
          await contextCallStarted.promise;

          await instance.handleInboundEvent("racing-event", { value: 42 });
          createWaitStep.resolve();

          await expect.poll(() => instance.getStatus()).toBe("completed");
          expect(instance.getSteps_experimental().find((s) => s.id === "wait-created-during-inbound")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: { value: 42 }
          });
          expect(
            state.storage.sql
              .exec<{ claimed_by: string | null }>(
                "SELECT claimed_by FROM inbound_events WHERE event_name = ?",
                "racing-event"
              )
              .toArray()
          ).toEqual([{ claimed_by: "wait-created-during-inbound" }]);
          expect(await state.storage.getAlarm()).toBeNull();
        });
      } finally {
        createWaitStep.resolve();
        contextSpy.mockRestore();
        executeSpy.mockRestore();
      }
    });

    it("persists satisfied wait payload on inbound_events with claimed_by pointing at the wait step", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-inbound-row", "evt-claim", {
            timeoutAt: Date.now() + 86_400_000
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance, state) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();

          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-inbound-row");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          await instance.handleInboundEvent("evt-claim", { trace: "x" });
          await expect(promise).resolves.toBe("completed");

          const formatted = instance.getSteps_experimental().find((s) => s.id === "wait-inbound-row");
          expect(formatted).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: { trace: "x" }
          });

          const rows = state.storage.sql
            .exec<{ payload: string; claimed_by: string | null }>(
              `SELECT payload, claimed_by FROM inbound_events WHERE claimed_by = ?`,
              "wait-inbound-row"
            )
            .toArray();
          expect(rows).toHaveLength(1);
          expect(rows[0]!.claimed_by).toBe("wait-inbound-row");
          expect(JSON.parse(rows[0]!.payload)).toEqual({ trace: "x" });
        });

        expect(await runDurableObjectAlarm(stub)).toBe(false);
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("satisfies a waiting wait step when called without a payload", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const payload = await this.wait<undefined>("wait-no-payload", "evt");
          await this.run("after-wait-no-payload", async () => payload);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();

          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-no-payload");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          await instance.handleInboundEvent("evt");
          await expect(promise).resolves.toBe("completed");

          expect(instance.getSteps_experimental().find((s) => s.id === "wait-no-payload")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: undefined
          });

          const afterWait = instance.getSteps_experimental().find((s) => s.id === "after-wait-no-payload");
          expect(afterWait?.type).toBe("run");
          const attempts = (afterWait as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(attempts[attempts.length - 1]).toMatchObject({
            state: "succeeded",
            resultType: "none"
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("satisfies a waiting wait step when called with an explicit null payload", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const payload = await this.wait<null>("wait-null", "evt");
          await this.run("after-wait-null", async () => payload);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();

          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-null");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          await instance.handleInboundEvent("evt", null);
          await expect(promise).resolves.toBe("completed");

          expect(instance.getSteps_experimental().find((s) => s.id === "wait-null")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: null
          });

          const afterWait = instance.getSteps_experimental().find((s) => s.id === "after-wait-null");
          expect(afterWait?.type).toBe("run");
          const attempts = (afterWait as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(attempts[attempts.length - 1]).toMatchObject({
            state: "succeeded",
            resultType: "json",
            resultJson: "null"
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("satisfies a waiting wait step via queued payloadless event claimed on resume", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-queued", "evt");
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.create();

          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "wait-queued");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          await instance.pause();

          // Queue the event without a payload while paused
          await instance.handleInboundEvent("evt");

          // The wait step should still be waiting (event was only queued, not claimed)
          expect(instance.getSteps_experimental().find((s) => s.id === "wait-queued")).toMatchObject({
            type: "wait",
            state: "waiting"
          });

          await instance.resume();
          await expect(promise).resolves.toBe("completed");

          expect(instance.getSteps_experimental().find((s) => s.id === "wait-queued")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: undefined
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("is a no-op when the workflow is in a terminal state", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("step-1", async () => "done");
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          // Should not throw even though there is no matching wait step
          await instance.handleInboundEvent("any-event", { data: 1 });
          expect(instance.getStatus()).toBe("completed");
        });
      } finally {
        executeSpy.mockRestore();
      }
    });
  });

  describe("getWorkflowEvents_experimental()", () => {
    it("records 'created' when workflow is first initialized", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const events = instance.getWorkflowEvents_experimental();
        expect(events).toHaveLength(1);
        expect(events[0]).toMatchObject({
          type: "created",
          recordedAt: expect.any(Date)
        });
      });
    });

    it("records 'started' when workflow transitions from initialized to running", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("step-1", async () => 1);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("completed");

          const events = instance.getWorkflowEvents_experimental();
          expect(events.map((event) => event.type)).toEqual(["created", "started", "completed"]);
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("records 'paused' when workflow is paused", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-1", 60_000);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          await instance.pause();

          const events = instance.getWorkflowEvents_experimental();
          expect(events.map((event) => event.type)).toEqual(["created", "started", "paused"]);
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("records 'resumed' when workflow is resumed from paused", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("step-1", async () => 1);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          await instance.pause();
          await instance.resume();
          await expect(promise).resolves.toBe("completed");

          const events = instance.getWorkflowEvents_experimental();
          expect(events.map((event) => event.type)).toEqual(["created", "started", "paused", "resumed", "completed"]);
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("records 'failed' when workflow fails", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("step-1", async () => {
            throw new Error("intentional failure");
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange = (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create();
          await expect(promise).resolves.toBe("failed");

          const events = instance.getWorkflowEvents_experimental();
          expect(events.map((event) => event.type)).toEqual(["created", "started", "failed"]);
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("records 'cancelled' with reason when workflow is cancelled", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-1", 60_000);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          await instance.create();
          await expect.poll(() => instance.getStatus()).toBe("running");
          await instance.cancel("user requested cancellation");

          const events = instance.getWorkflowEvents_experimental();
          expect(events.map((event) => event.type)).toEqual(["created", "started", "cancelled"]);
          const cancelledEntry = events.find((event) => event.type === "cancelled");
          expect(cancelledEntry).toMatchObject({
            type: "cancelled",
            cancellationReason: "user requested cancellation"
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("records 'cancelled' without reason when workflow is cancelled from pending", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        expect(instance.getStatus()).toBe("pending");
        await instance.cancel();

        const events = instance.getWorkflowEvents_experimental();
        expect(events.map((event) => event.type)).toEqual(["created", "cancelled"]);
        const cancelledEntry = events.find((event) => event.type === "cancelled");
        expect(cancelledEntry).toMatchObject({
          type: "cancelled",
          cancellationReason: undefined
        });
      });
    });
  });

  describe("WorkflowRuntimeContext", () => {
    describe("recovery alarm scheduling", () => {
      it("keeps the earliest of multiple sleep deadlines regardless of creation order", async () => {
        const now = Date.now();
        const earlier = now + 60_000;
        const later = now + 120_000;

        for (const [first, second] of [
          [earlier, later],
          [later, earlier]
        ] as const) {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateSleepStep(createSleepStepId("sleep-first"), {
              wakeAt: new Date(first),
              parentStepId: null
            });
            await context.getOrCreateSleepStep(createSleepStepId("sleep-second"), {
              wakeAt: new Date(second),
              parentStepId: null
            });

            expect(await state.storage.getAlarm()).toBe(earlier);
          });
        }
      });

      it("keeps the watchdog ahead of later durable deadlines", async () => {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (_instance, state) => {
          const watchdogAt = Date.now() + 30 * 60_000;
          const earlier = watchdogAt + 60_000;
          const later = watchdogAt + 120_000;
          await state.storage.setAlarm(watchdogAt);
          const context = createRunningWorkflowRuntimeContext(state.storage);

          await context.getOrCreateSleepStep(createSleepStepId("sleep-after-watchdog"), {
            wakeAt: new Date(earlier),
            parentStepId: null
          });
          await context.getOrCreateWaitStep(createWaitStepId("wait-after-sleep"), {
            eventName: "event-after-sleep",
            timeoutAt: new Date(later),
            parentStepId: null
          });

          expect(await state.storage.getAlarm()).toBe(watchdogAt);
        });
      });

      it("keeps an earlier run retry ahead of later sleep and wait deadlines", async () => {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (_instance, state) => {
          const context = createRunningWorkflowRuntimeContext(state.storage);
          const runStepId = createRunStepId("retry-first");
          await context.getOrCreateRunStep(runStepId, { maxAttempts: 2, parentStepId: null });
          const started = context.handleRunAttemptStarted(runStepId);
          const failed = await context.handleRunAttemptFailed(runStepId, started.id, { errorMessage: "transient" });
          expect(failed.nextAttemptAt).toEqual(expect.any(Date));
          if (failed.nextAttemptAt === undefined) {
            throw new Error("Expected a retryable failed attempt.");
          }

          await context.getOrCreateSleepStep(createSleepStepId("sleep-after-retry"), {
            wakeAt: new Date(failed.nextAttemptAt.getTime() + 60_000),
            parentStepId: null
          });
          await context.getOrCreateWaitStep(createWaitStepId("wait-after-retry"), {
            eventName: "event-after-retry",
            timeoutAt: new Date(failed.nextAttemptAt.getTime() + 120_000),
            parentStepId: null
          });

          expect(await state.storage.getAlarm()).toBe(failed.nextAttemptAt.getTime());
        });
      });

      it("does not replace immediate recovery with a later durable deadline", async () => {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (_instance, state) => {
          const context = createRunningWorkflowRuntimeContext(state.storage);
          const runStepId = createRunStepId("succeeded-before-sleep");
          await context.getOrCreateRunStep(runStepId, { parentStepId: null });
          const started = context.handleRunAttemptStarted(runStepId);
          await context.handleRunAttemptSucceeded(runStepId, started.id, '"done"');
          const immediateAlarm = await state.storage.getAlarm();
          expect(immediateAlarm).not.toBeNull();
          if (immediateAlarm === null) {
            throw new Error("Expected an immediate recovery alarm.");
          }

          await context.getOrCreateSleepStep(createSleepStepId("sleep-after-success"), {
            wakeAt: new Date(immediateAlarm + 60_000),
            parentStepId: null
          });

          expect(await state.storage.getAlarm()).toBe(immediateAlarm);
        });
      });

      it("keeps immediate recovery when it races a future deadline after the watchdog", async () => {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (_instance, state) => {
          const preparationContext = createRunningWorkflowRuntimeContext(state.storage);
          const runStepId = createRunStepId("succeeded-during-sleep");
          await preparationContext.getOrCreateRunStep(runStepId, { parentStepId: null });
          const started = preparationContext.handleRunAttemptStarted(runStepId);

          const watchdogAt = Date.now() + 30 * 60_000;
          await state.storage.setAlarm(watchdogAt);
          const context = createRunningWorkflowRuntimeContext(state.storage);
          const sleepAt = watchdogAt + 60_000;

          await Promise.all([
            context.handleRunAttemptSucceeded(runStepId, started.id, '"done"'),
            context.getOrCreateSleepStep(createSleepStepId("sleep-during-success"), {
              wakeAt: new Date(sleepAt),
              parentStepId: null
            })
          ]);

          const alarm = await state.storage.getAlarm();
          expect(alarm).not.toBeNull();
          expect(alarm as number).toBeLessThan(sleepAt);
        });
      });
    });

    describe("run steps", () => {
      describe("getOrCreateRunStep()", () => {
        it("creates a new run step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const step = await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step).toMatchObject({
              id: "step-1",
              type: "run",
              maxAttempts: 3,
              parentStepId: null
            });
            expect(step.attempts).toEqual([]);
          });
        });

        it("creates a run step once and returns the same durable row on subsequent reads", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const first = await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const second = await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(first).toEqual(second);
          });
        });

        it("leaves attempts empty until handleRunAttemptStarted", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const step = await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step.attempts).toEqual([]);
          });
        });

        it("persists 'max_attempts' on the run step row", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const step = await context.getOrCreateRunStep(createRunStepId("step-1"), {
              maxAttempts: 5,
              parentStepId: null
            });
            expect(step).toMatchObject({
              id: "step-1",
              type: "run",
              maxAttempts: 5
            });
          });
        });

        it("re-arms a persisted retry deadline when re-reading a failed run step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const stepId = createRunStepId("step-1");
            await context.getOrCreateRunStep(stepId, { maxAttempts: 2, parentStepId: null });
            const started = context.handleRunAttemptStarted(stepId);
            const failed = await context.handleRunAttemptFailed(stepId, started.id, { errorMessage: "transient" });
            expect(failed.nextAttemptAt).toEqual(expect.any(Date));

            await state.storage.deleteAlarm();
            await context.getOrCreateRunStep(stepId, { parentStepId: null });
            expect(await state.storage.getAlarm()).toBe(failed.nextAttemptAt?.getTime());
          });
        });
      });

      describe("hasInProgressChildSteps()", () => {
        it("returns false when the run step has no direct child rows", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("leaf"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("leaf"));
            expect(context.hasInProgressChildSteps(createRunStepId("leaf"))).toBe(false);
          });
        });

        it("returns true when the only direct child run is still pending", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("parent"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("parent"));
            await context.getOrCreateRunStep(createRunStepId("child"), {
              parentStepId: createRunStepId("parent")
            });
            expect(context.hasInProgressChildSteps(createRunStepId("parent"))).toBe(true);
          });
        });

        it("returns true when a direct child run step is running", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("parent"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("parent"));
            await context.getOrCreateRunStep(createRunStepId("child"), {
              parentStepId: createRunStepId("parent")
            });
            context.handleRunAttemptStarted(createRunStepId("child"));
            expect(context.hasInProgressChildSteps(createRunStepId("parent"))).toBe(true);
          });
        });

        it("returns true when a direct child run exists in a non-failure state (e.g. pending)", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("gp"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("gp"));
            await context.getOrCreateRunStep(createRunStepId("mid"), { parentStepId: createRunStepId("gp") });
            await context.getOrCreateRunStep(createRunStepId("leaf"), {
              parentStepId: createRunStepId("mid")
            });
            expect(context.hasInProgressChildSteps(createRunStepId("gp"))).toBe(true);
          });
        });

        it("returns true when a direct child exists (pending or running) and false for a leaf", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("mid"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("mid"));
            await context.getOrCreateRunStep(createRunStepId("leaf"), {
              parentStepId: createRunStepId("mid")
            });
            expect(context.hasInProgressChildSteps(createRunStepId("mid"))).toBe(true);
            expect(context.hasInProgressChildSteps(createRunStepId("leaf"))).toBe(false);
          });
        });

        it("returns false when the only direct child run has failed", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("par"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("par"));
            await context.getOrCreateRunStep(createRunStepId("bad-child"), {
              parentStepId: createRunStepId("par"),
              maxAttempts: 1
            });
            const childAttempt = context.handleRunAttemptStarted(createRunStepId("bad-child"));
            await context.handleRunAttemptFailed(createRunStepId("bad-child"), childAttempt.id, {
              errorMessage: "x"
            });
            expect(context.hasInProgressChildSteps(createRunStepId("par"))).toBe(false);
          });
        });

        it("returns true when the only direct child is a non-run step (sleep) in waiting", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("run-parent"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("run-parent"));
            await context.getOrCreateSleepStep(createSleepStepId("child-sleep"), {
              wakeAt: new Date(Date.now() + 60_000),
              parentStepId: createRunStepId("run-parent")
            });
            expect(context.hasInProgressChildSteps(createRunStepId("run-parent"))).toBe(true);
          });
        });
      });

      describe("handleRunAttemptStarted()", () => {
        it("inserts a started attempt", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const started = context.handleRunAttemptStarted(createRunStepId("step-1"));
            expect(started).toMatchObject({ state: "started", stepId: "step-1" });
            const step = await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step.attempts).toHaveLength(1);
            expect(step.attempts[0]).toMatchObject({ state: "started" });
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            expect(() => context.handleRunAttemptStarted(createRunStepId("nonexistent"))).toThrow(/not found/);
          });
        });

        it("throws when an attempt is already in progress", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            expect(() => context.handleRunAttemptStarted(createRunStepId("step-1"))).toThrow(/already in progress/);
          });
        });
      });

      describe("handleRunAttemptSucceeded()", () => {
        it("rejects a stale attempt outcome without mutating the newer attempt", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const stepId = createRunStepId("step-1");
            await context.getOrCreateRunStep(stepId, { maxAttempts: 2, parentStepId: null });

            const first = context.handleRunAttemptStarted(stepId);
            await context.handleRunAttemptFailed(stepId, first.id, { errorMessage: "first attempt failed" });
            const second = context.handleRunAttemptStarted(stepId);

            await expect(context.handleRunAttemptSucceeded(stepId, first.id, '"stale"')).rejects.toThrow(
              /not in progress/
            );
            await expect(
              context.handleRunAttemptFailed(stepId, first.id, { errorMessage: "late failure" })
            ).rejects.toThrow(/not in progress/);

            const beforeCurrentOutcome = await context.getOrCreateRunStep(stepId, { parentStepId: null });
            expect(beforeCurrentOutcome.attempts.find((attempt) => attempt.id === second.id)).toMatchObject({
              state: "started"
            });

            await context.handleRunAttemptSucceeded(stepId, second.id, '"current"');
            const afterCurrentOutcome = await context.getOrCreateRunStep(stepId, { parentStepId: null });
            expect(afterCurrentOutcome.attempts.find((attempt) => attempt.id === second.id)).toMatchObject({
              state: "succeeded",
              resultJson: '"current"'
            });
          });
        });

        it("marks the in-flight attempt succeeded with a json result", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const started = context.handleRunAttemptStarted(createRunStepId("step-1"));
            const before = Date.now();
            const done = await context.handleRunAttemptSucceeded(
              createRunStepId("step-1"),
              started.id,
              JSON.stringify(0)
            );
            expect(done).toMatchObject({
              state: "succeeded",
              resultType: "json",
              resultJson: JSON.stringify(0)
            });
            const step = await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step.attempts).toHaveLength(1);
            expect(step.attempts[0]).toMatchObject({
              state: "succeeded",
              resultType: "json",
              resultJson: JSON.stringify(0)
            });
            expect(await state.storage.getAlarm()).toBeGreaterThanOrEqual(before);
          });
        });

        it("marks the in-flight attempt succeeded with result_type none", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const started = context.handleRunAttemptStarted(createRunStepId("step-1"));
            const done = await context.handleRunAttemptSucceeded(createRunStepId("step-1"), started.id, null);
            expect(done).toMatchObject({
              state: "succeeded",
              resultType: "none"
            });
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await expect(
              context.handleRunAttemptSucceeded(
                createRunStepId("nonexistent"),
                createRunStepAttemptId("missing-attempt"),
                null
              )
            ).rejects.toThrow(/not found/);
          });
        });

        it("throws when the attempt id does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            await expect(
              context.handleRunAttemptSucceeded(
                createRunStepId("step-1"),
                createRunStepAttemptId("missing-attempt"),
                null
              )
            ).rejects.toThrow(/not found/);
          });
        });
      });

      describe("handleRunAttemptFailed()", () => {
        it("marks terminal failed when max attempts exhausted", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { maxAttempts: 1, parentStepId: null });
            const started = context.handleRunAttemptStarted(createRunStepId("step-1"));
            const before = Date.now();
            const failed = await context.handleRunAttemptFailed(createRunStepId("step-1"), started.id, {
              errorMessage: "error"
            });
            expect(failed).toMatchObject({
              state: "failed",
              errorMessage: "error",
              nextAttemptAt: undefined
            });
            const step = await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step.attempts).toHaveLength(1);
            expect(step.attempts[0]).toMatchObject({ state: "failed", errorMessage: "error" });
            expect(await state.storage.getAlarm()).toBeGreaterThanOrEqual(before);
          });
        });

        it("records the retry backoff sequence and caps subsequent delays", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          const now = Date.now();
          const dateNowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
          try {
            await runInDurableObject(stub, async (_instance, state) => {
              const context = createRunningWorkflowRuntimeContext(state.storage);
              const stepId = createRunStepId("step-1");
              await context.getOrCreateRunStep(stepId, { maxAttempts: 10, parentStepId: null });

              const backoffs = [250, 500, 1_000, 2_000, 4_000, 8_000, 10_000, 10_000, 10_000];
              for (const [index, backoff] of backoffs.entries()) {
                const started = context.handleRunAttemptStarted(stepId);
                // These direct context calls bypass alarm delays. Give each row a unique durable timestamp so the
                // storage ordering is deterministic even though attempt ids are random.
                state.storage.sql.exec(
                  "UPDATE run_step_attempts SET started_at = ? WHERE step_id = ? AND state = 'started'",
                  now - 1_000 + index,
                  stepId
                );
                const failed = await context.handleRunAttemptFailed(stepId, started.id, {
                  errorMessage: "transient"
                });
                expect(failed).toMatchObject({
                  state: "failed",
                  nextAttemptAt: new Date(now + backoff)
                });
                expect(await state.storage.getAlarm()).toBe(now + backoff);
                // A real next attempt starts after this alarm fires. These direct context calls bypass that alarm
                // invocation, so consume it here before recording the next retry deadline.
                await state.storage.deleteAlarm();
              }

              const started = context.handleRunAttemptStarted(stepId);
              state.storage.sql.exec(
                "UPDATE run_step_attempts SET started_at = ? WHERE step_id = ? AND state = 'started'",
                now - 1_000 + backoffs.length,
                stepId
              );
              expect(
                await context.handleRunAttemptFailed(stepId, started.id, { errorMessage: "terminal" })
              ).toMatchObject({
                state: "failed",
                nextAttemptAt: undefined
              });
              expect(await state.storage.getAlarm()).toBeGreaterThanOrEqual(now);
              expect((await context.getOrCreateRunStep(stepId, { parentStepId: null })).attempts).toHaveLength(10);
            });
          } finally {
            dateNowSpy.mockRestore();
          }
        });

        it("marks terminal failed when isNonRetryableStepError is true", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { maxAttempts: 10, parentStepId: null });
            const started = context.handleRunAttemptStarted(createRunStepId("step-1"));
            const failed = await context.handleRunAttemptFailed(createRunStepId("step-1"), started.id, {
              errorMessage: "x",
              isNonRetryableStepError: true
            });
            expect(failed).toMatchObject({ state: "failed", nextAttemptAt: undefined });
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await expect(
              context.handleRunAttemptFailed(
                createRunStepId("nonexistent"),
                createRunStepAttemptId("missing-attempt"),
                { errorMessage: "e" }
              )
            ).rejects.toThrow(/not found/);
          });
        });

        it("throws when the attempt id does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            await expect(
              context.handleRunAttemptFailed(createRunStepId("step-1"), createRunStepAttemptId("missing-attempt"), {
                errorMessage: "bad"
              })
            ).rejects.toThrow(/not found/);
          });
        });
      });
    });

    describe("sleep steps", () => {
      describe("getOrCreateSleepStep()", () => {
        it("creates a sleep step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            const step = await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt,
              parentStepId: null
            });
            expect(step).toMatchObject({
              id: "sleep-1",
              type: "sleep",
              state: "waiting",
              wakeAt
            });
          });
        });

        it("creates a sleep step once and returns the same durable row on subsequent reads", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const w = new Date();
            const first = await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: w,
              parentStepId: null
            });
            const second = await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(),
              parentStepId: null
            });
            expect(first).toEqual(second);
          });
        });

        it("atomically schedules the sleep wake alarm", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), { wakeAt, parentStepId: null });
            expect(await state.storage.getAlarm()).toBe(wakeAt.getTime());
          });
        });

        it("replaces an existing alarm with the sleep wake alarm", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const prior = Date.now() + 999_999;
            await state.storage.setAlarm(prior);
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt,
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBe(wakeAt.getTime());
          });
        });

        it("re-arms the wake deadline when re-reading an existing sleep step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt,
              parentStepId: null
            });
            await state.storage.deleteAlarm();
            await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt,
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBe(wakeAt.getTime());
          });
        });
      });

      describe("handleSleepStepElapsed()", () => {
        it("moves a sleep step from 'waiting' to 'elapsed'", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(),
              parentStepId: null
            });
            await state.storage.deleteAlarm();
            const before = Date.now();
            await context.handleSleepStepElapsed(createSleepStepId("sleep-1"));
            const step = await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(),
              parentStepId: null
            });
            expect(step).toMatchObject({
              state: "elapsed",
              resolvedAt: expect.any(Date)
            });
            expect(await state.storage.getAlarm()).toBeGreaterThanOrEqual(before);
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await expect(context.handleSleepStepElapsed(createSleepStepId("nonexistent"))).rejects.toThrow(/not found/);
          });
        });

        it("throws when the step is already elapsed", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(Date.now() + 60_000),
              parentStepId: null
            });
            await context.handleSleepStepElapsed(createSleepStepId("sleep-1"));
            await expect(context.handleSleepStepElapsed(createSleepStepId("sleep-1"))).rejects.toThrow(
              /Expected 'waiting' but got elapsed/
            );
          });
        });
      });
    });

    describe("wait steps", () => {
      describe("getOrCreateWaitStep()", () => {
        it("creates a wait step when no timeout is provided", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const step = await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(step).toMatchObject({
              id: "wait-1",
              type: "wait",
              state: "waiting",
              eventName: "event-1"
            });
            expect(await state.storage.getAlarm()).toBeNull();
          });
        });

        it("creates a wait step when a timeout is provided", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() + 60_000);
            const step = await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt
            });
            expect(step).toMatchObject({
              id: "wait-1",
              type: "wait",
              state: "waiting",
              eventName: "event-1",
              timeoutAt
            });
          });
        });

        it("creates a wait step once and returns the same durable row on subsequent reads", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const first = await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            const second = await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(first).toEqual(second);
          });
        });

        it("uses the persisted event name when replay arguments change", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const waitStepId = createWaitStepId("wait-1");
            await context.getOrCreateWaitStep(waitStepId, {
              eventName: "event-original",
              parentStepId: null
            });
            state.storage.sql.exec(
              `INSERT INTO inbound_events (event_name, payload) VALUES (?, ?)`,
              "event-replayed",
              JSON.stringify({ source: "replayed" })
            );
            state.storage.sql.exec(
              `INSERT INTO inbound_events (event_name, payload) VALUES (?, ?)`,
              "event-original",
              JSON.stringify({ source: "original" })
            );

            const replayed = await context.getOrCreateWaitStep(waitStepId, {
              eventName: "event-replayed",
              parentStepId: null
            });
            expect(replayed).toMatchObject({
              id: "wait-1",
              state: "satisfied",
              eventName: "event-original",
              payload: { source: "original" }
            });
            expect(
              state.storage.sql
                .exec<{ event_name: string; claimed_by: string | null }>(
                  "SELECT event_name, claimed_by FROM inbound_events ORDER BY event_name"
                )
                .toArray()
            ).toEqual([
              { event_name: "event-original", claimed_by: "wait-1" },
              { event_name: "event-replayed", claimed_by: null }
            ]);
          });
        });

        it("claims a queued event that arrived before the wait deadline even after the deadline passes", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const timeoutAt = Date.now() - 1_000;
            state.storage.sql.exec(
              `INSERT INTO inbound_events (event_name, payload, created_at) VALUES (?, ?, ?)`,
              "event-before-deadline",
              JSON.stringify({ onTime: true }),
              timeoutAt - 1
            );

            const step = await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-before-deadline",
              timeoutAt: new Date(timeoutAt),
              parentStepId: null
            });
            expect(step).toMatchObject({
              state: "satisfied",
              payload: { onTime: true }
            });
          });
        });

        it("re-arms the timeout deadline when re-reading an existing wait step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() + 60_000);
            await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt
            });

            await state.storage.deleteAlarm();
            await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBe(timeoutAt.getTime());
          });
        });

        it("atomically schedules the wait timeout alarm", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() + 60_000);
            await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt
            });
            expect(await state.storage.getAlarm()).toBe(timeoutAt.getTime());
          });
        });

        it("replaces an existing alarm with the wait timeout alarm", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const prior = Date.now() + 999_999;
            await state.storage.setAlarm(prior);
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() + 60_000);
            await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt
            });
            expect(await state.storage.getAlarm()).toBe(timeoutAt.getTime());
          });
        });

        it("satisfies from a queued inbound event when creating the wait step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            state.storage.sql.exec(
              `INSERT INTO inbound_events (event_name, payload) VALUES (?, ?)`,
              "event-1",
              JSON.stringify({ v: 1 })
            );
            const before = Date.now();
            const step = await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(step).toMatchObject({
              state: "satisfied",
              payload: { v: 1 }
            });
            expect(await state.storage.getAlarm()).toBeGreaterThanOrEqual(before);
          });
        });

        it("rejects a second inbound_events row with the same claimed_by", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            const t = Date.now();
            state.storage.sql.exec(
              `INSERT INTO inbound_events (event_name, payload, claimed_by, claimed_at) VALUES (?, ?, ?, ?)`,
              "event-1",
              null,
              "wait-1",
              t
            );
            expect(() =>
              state.storage.sql.exec(
                `INSERT INTO inbound_events (event_name, payload, claimed_by, claimed_at) VALUES (?, ?, ?, ?)`,
                "event-1",
                null,
                "wait-1",
                t + 1
              )
            ).toThrow(/UNIQUE/);
          });
        });
      });

      describe("handleWaitStepTimedOut()", () => {
        it("moves a wait step from 'waiting' to 'timed_out'", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() - 1000);
            await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt
            });
            await state.storage.deleteAlarm();
            const before = Date.now();
            await context.handleWaitStepTimedOut(createWaitStepId("wait-1"));
            const step = await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(step).toMatchObject({ state: "timed_out" });
            expect(await state.storage.getAlarm()).toBeGreaterThanOrEqual(before);
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await expect(context.handleWaitStepTimedOut(createWaitStepId("nonexistent"))).rejects.toThrow(/not found/);
          });
        });

        it("throws when the step is already timed out", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = createRunningWorkflowRuntimeContext(state.storage);
            await context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: new Date(Date.now() - 1000)
            });
            await context.handleWaitStepTimedOut(createWaitStepId("wait-1"));
            await expect(context.handleWaitStepTimedOut(createWaitStepId("wait-1"))).rejects.toThrow(
              /Expected 'waiting' but got timed_out/
            );
          });
        });
      });
    });
  });
});
