import { runInDurableObject, runDurableObjectAlarm } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import {
  WorkflowRuntimeContext,
  type RunStep,
  type RunStepAttempt,
  type RunStepId,
  type SleepStepId,
  type WaitStepId,
  type WorkflowStatus
} from "../src/runtime";
import { TestWorkflowDefinition } from "./worker";
import { NonRetryableStepError } from "../src/definition";

function createRunStepId(id: string): RunStepId {
  return id as RunStepId;
}
function createSleepStepId(id: string): SleepStepId {
  return id as SleepStepId;
}
function createWaitStepId(id: string): WaitStepId {
  return id as WaitStepId;
}

describe("WorkflowRuntime", () => {
  it("constructor() initializes the database and sets the status to pending", async () => {
    const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (instance) => {
      expect(instance.getStatus()).toBe("pending");
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create({ definitionVersion: "2026-03-19" });
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };
        await instance.create({ definitionVersion: "2026-03-19" });
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create({ definitionVersion: "2026-03-19" });
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create({ definitionVersion: "2026-03-19" });
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create({ definitionVersion: "2026-03-19" });
        await expect(promise).resolves.toBe("failed");
      });
    } finally {
      executeSpy.mockRestore();
    }
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };
        await instance.create({ definitionVersion: "2026-03-19" });
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create({ definitionVersion: "2026-03-19" });
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create({ definitionVersion: "2026-03-19" });
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
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };

        await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };
          await instance.create({ definitionVersion: "2026-03-19", input });
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

    it("throws when the workflow is not terminal and definition version is already pinned to a different version", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.wait("wait-1", "event-never", {
            timeoutAt: Date.now() + 86_400_000
          });
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            resolve(status);
          };
          await instance.create({ definitionVersion: "2026-03-19" });
          await expect(promise).resolves.toBe("running");

          await expect(instance.create({ definitionVersion: "2026-03-20" })).rejects.toThrow(
            "Workflow definition version is already pinned to '2026-03-19' and cannot be changed to '2026-03-20'."
          );
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("is a no-op when the workflow is already in a terminal state", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };
        await instance.create({ definitionVersion: "2026-03-19" });
        await expect(promise).resolves.toBe("completed");
        expect(instance.getStatus()).toBe("completed");

        await instance.create({ definitionVersion: "2026-03-20" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });

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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
            instance.onStatusChange_experimental = async (status) => {
              if (status === "running") return;
              resolve(status);
            };

            await instance.create({ definitionVersion: "2026-03-19" });
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
        const executeSpy = vi
          .spyOn(TestWorkflowDefinition.prototype, "execute")
          .mockImplementation(async function (this: TestWorkflowDefinition) {
            await this.run("ex-outer", async () => {
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
            instance.onStatusChange_experimental = async (status) => {
              if (status === "running") return;
              resolve(status);
            };

            await instance.create({ definitionVersion: "2026-03-19" });
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
            expect(exOa[exOa.length - 1]).toMatchObject({
              state: "failed",
              errorName: "Error"
            });
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
            instance.onStatusChange_experimental = async (status) => {
              if (status === "running") return;
              resolve(status);
            };

            await instance.create({ definitionVersion: "2026-03-19" });
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
          await runInDurableObject(stub, async (instance) => {
            const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
            instance.onStatusChange_experimental = async (status) => {
              if (status === "running") return;
              resolve(status);
            };

            await instance.create({ definitionVersion: "2026-03-19" });
            await expect
              .poll(() => {
                const step = instance.getSteps_experimental().find((s) => s.id === "root-deep-wait");
                return step?.type === "wait" ? step.state : undefined;
              })
              .toBe("waiting");

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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            terminalStatuses.push(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await Promise.all([
            this.run("parallel-run", async () => 1),
            this.wait("parallel-wait", "parallel-event", {
              timeoutAt: Date.now() + 86_400_000
            })
          ]);
        });
      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const terminalStatuses: WorkflowStatus[] = [];
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            terminalStatuses.push(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          // `wait()` usually wins the race; the run branch can still be mid-flight so the latest attempt may still be in flight.
          const parRun = steps.find((s) => s.id === "parallel-run");
          expect(parRun?.type).toBe("run");
          const paa = (parRun as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(paa[paa.length - 1]).toMatchObject({ state: "started" });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });
  });

  describe("alarm()", () => {
    it("no alarm is scheduled after the workflow has completed", async () => {
      const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
      await runInDurableObject(stub, async (instance) => {
        const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
        instance.onStatusChange_experimental = async (status) => {
          if (status === "running") return;
          resolve(status);
        };
        await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };
          await instance.create({ definitionVersion: "2026-03-19" });
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
        await runInDurableObject(stub, async (instance) => {
          await instance.create({ definitionVersion: "2026-03-19" });
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
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("pause() fires onStatusChange_experimental with 'paused'", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.sleep("sleep-1", 60_000);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<void>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "paused") resolve();
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          await instance.create({ definitionVersion: "2026-03-19" });
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
          await instance.create({ definitionVersion: "2026-03-19" });

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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });

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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });

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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });

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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });

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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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

    it("records 'started' when workflow transitions from pending to running", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("step-1", async () => 1);
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
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
          await instance.create({ definitionVersion: "2026-03-19" });
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


  describe("ReadableStream support in run()", () => {
    it("stores stream chunks and returns a synthetic stream on first run", async () => {
      const inputBytes = new Uint8Array([72, 101, 108, 108, 111]);
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const stream = await this.run("stream-step", async () => {
            return new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(inputBytes);
                controller.close();
              }
            });
          });

          const reader = stream.getReader();
          const collected: Uint8Array[] = [];
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            collected.push(new Uint8Array(value));
          }

          await this.run("verify", async () => {
            return { length: collected.length, firstChunk: Array.from(collected[0]!) };
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          const streamStep = steps.find((s) => s.id === "stream-step");
          expect(streamStep?.type).toBe("run");
          const attempts = (streamStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(attempts[0]).toMatchObject({
            state: "succeeded",
            resultType: "stream"
          });

          const verifyStep = steps.find((s) => s.id === "verify");
          expect(verifyStep?.type).toBe("run");
          const verifyAttempts = (verifyStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(verifyAttempts[0]).toMatchObject({
            state: "succeeded",
            resultType: "json"
          });
          expect(JSON.parse((verifyAttempts[0]! as Extract<RunStepAttempt, { resultType: "json" }>).resultJson)).toEqual({
            length: 1,
            firstChunk: Array.from(inputBytes)
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("replays a stream step from stored chunks without re-executing the callback", async () => {
      const callCounts = { streamCallback: 0, afterStream: 0 };
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const stream = await this.run("stream-step", async () => {
            callCounts.streamCallback++;
            return new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(new Uint8Array([1, 2, 3]));
                controller.close();
              }
            });
          });

          const reader = stream.getReader();
          const collected: Uint8Array[] = [];
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            collected.push(new Uint8Array(value));
          }

          callCounts.afterStream++;
          await this.run("after-stream", async () => {
            return { bytes: Array.from(collected[0]!) };
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
          await expect(promise).resolves.toBe("completed");

          expect(callCounts.streamCallback).toBe(1);
          expect(callCounts.afterStream).toBeGreaterThanOrEqual(1);

          const steps = instance.getSteps_experimental();
          const afterStream = steps.find((s) => s.id === "after-stream");
          expect(afterStream?.type).toBe("run");
          const attempts = (afterStream as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(attempts[0]).toMatchObject({ state: "succeeded", resultType: "json" });
          expect(
            JSON.parse((attempts[0]! as Extract<RunStepAttempt, { resultType: "json" }>).resultJson)
          ).toEqual({ bytes: [1, 2, 3] });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("handles an empty ReadableStream from run()", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const stream = await this.run("empty-stream", async () => {
            return new ReadableStream<Uint8Array>({
              start(controller) {
                controller.close();
              }
            });
          });

          const reader = stream.getReader();
          const { done } = await reader.read();
          await this.run("after-empty", async () => {
            return { isEmpty: done };
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();
          const emptyStep = steps.find((s) => s.id === "empty-stream");
          expect(emptyStep?.type).toBe("run");
          const attempts = (emptyStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(attempts[0]).toMatchObject({
            state: "succeeded",
            resultType: "stream"
          });

          const afterStep = steps.find((s) => s.id === "after-empty");
          expect(afterStep?.type).toBe("run");
          const afterAttempts = (afterStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(JSON.parse((afterAttempts[0]! as Extract<RunStepAttempt, { resultType: "json" }>).resultJson)).toEqual({ isEmpty: true });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("stream step nested inside a parent run() records parentStepId and completes", async () => {
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          await this.run("outer", async () => {
            const stream = await this.run("inner-stream", async () => {
              return new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(new Uint8Array([10, 20, 30]));
                  controller.close();
                }
              });
            });

            const reader = stream.getReader();
            const { value } = await reader.read();
            return { innerBytes: Array.from(new Uint8Array(value!)) };
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
          await expect(promise).resolves.toBe("completed");

          const steps = instance.getSteps_experimental();

          const outer = steps.find((s) => s.id === "outer");
          expect(outer).toMatchObject({ type: "run", parentStepId: null });
          const outerAttempts = (outer as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(outerAttempts[0]).toMatchObject({ state: "succeeded", resultType: "json" });
          expect(JSON.parse((outerAttempts[0]! as Extract<RunStepAttempt, { resultType: "json" }>).resultJson)).toEqual({
            innerBytes: [10, 20, 30]
          });

          const inner = steps.find((s) => s.id === "inner-stream");
          expect(inner).toMatchObject({ type: "run", parentStepId: "outer" });
          const innerAttempts = (inner as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(innerAttempts[0]).toMatchObject({ state: "succeeded", resultType: "stream" });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("stream step followed by a sibling run() completes across multiple next() calls", async () => {
      let executeCount = 0;
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          executeCount++;
          const stream = await this.run("stream-step", async () => {
            return new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(new Uint8Array([7, 8, 9]));
                controller.close();
              }
            });
          });

          const reader = stream.getReader();
          const collected: number[] = [];
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            collected.push(...new Uint8Array(value));
          }

          await this.run("sibling-after-stream", async () => {
            return { collected };
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
          await expect(promise).resolves.toBe("completed");

          expect(executeCount).toBeGreaterThanOrEqual(2);

          const steps = instance.getSteps_experimental();
          const streamStep = steps.find((s) => s.id === "stream-step");
          expect(streamStep?.type).toBe("run");
          const streamAttempts = (streamStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(streamAttempts[0]).toMatchObject({ state: "succeeded", resultType: "stream" });

          const sibling = steps.find((s) => s.id === "sibling-after-stream");
          expect(sibling?.type).toBe("run");
          const siblingAttempts = (sibling as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(siblingAttempts[0]).toMatchObject({ state: "succeeded", resultType: "json" });
          expect(
            JSON.parse((siblingAttempts[0]! as Extract<RunStepAttempt, { resultType: "json" }>).resultJson)
          ).toEqual({ collected: [7, 8, 9] });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("stream replay works correctly after pause() and resume()", async () => {
      let streamCallbackCount = 0;
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const stream = await this.run("stream-before-pause", async () => {
            streamCallbackCount++;
            return new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(new Uint8Array([42, 43]));
                controller.close();
              }
            });
          });

          const reader = stream.getReader();
          const { value } = await reader.read();

          await this.wait("pause-point", "resume-signal", {
            timeoutAt: Date.now() + 86_400_000
          });

          await this.run("after-pause", async () => {
            return { bytes: Array.from(new Uint8Array(value!)) };
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          await instance.create({ definitionVersion: "2026-03-19" });
          await expect.poll(() => instance.getStatus()).toBe("running");
          await expect
            .poll(() => {
              const step = instance.getSteps_experimental().find((s) => s.id === "pause-point");
              return step?.type === "wait" ? step.state : undefined;
            })
            .toBe("waiting");

          expect(streamCallbackCount).toBe(1);

          await instance.pause();
          expect(instance.getStatus()).toBe("paused");

          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running" || status === "paused") return;
            resolve(status);
          };

          await instance.resume();
          await instance.handleInboundEvent("resume-signal", undefined);
          await expect(promise).resolves.toBe("completed");

          expect(streamCallbackCount).toBe(1);

          const steps = instance.getSteps_experimental();

          const streamStep = steps.find((s) => s.id === "stream-before-pause");
          expect(streamStep?.type).toBe("run");
          const streamAttempts = (streamStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(streamAttempts[0]).toMatchObject({ state: "succeeded", resultType: "stream" });

          const afterPause = steps.find((s) => s.id === "after-pause");
          expect(afterPause?.type).toBe("run");
          const attempts = (afterPause as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(attempts[0]).toMatchObject({ state: "succeeded", resultType: "json" });
          expect(JSON.parse((attempts[0]! as Extract<RunStepAttempt, { resultType: "json" }>).resultJson)).toEqual({
            bytes: [42, 43]
          });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });

    it("retries after a stream step callback throws (crash safety)", async () => {
      let callbackCount = 0;
      const executeSpy = vi
        .spyOn(TestWorkflowDefinition.prototype, "execute")
        .mockImplementation(async function (this: TestWorkflowDefinition) {
          const stream = await this.run(
            "crashable-stream",
            async () => {
              callbackCount++;
              if (callbackCount === 1) {
                throw new Error("simulated crash");
              }
              return new ReadableStream<Uint8Array>({
                start(controller) {
                  controller.enqueue(new Uint8Array([99]));
                  controller.close();
                }
              });
            },
            { maxAttempts: 3 }
          );

          const reader = stream.getReader();
          const { value } = await reader.read();
          await this.run("after-retry", async () => {
            return { byte: Array.from(new Uint8Array(value!)) };
          });
        });

      try {
        const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
        await runInDurableObject(stub, async (instance) => {
          const { resolve, promise } = Promise.withResolvers<WorkflowStatus>();
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
          await expect(promise).resolves.toBe("completed");

          expect(callbackCount).toBe(2);

          const steps = instance.getSteps_experimental();
          const crashStep = steps.find((s) => s.id === "crashable-stream");
          expect(crashStep?.type).toBe("run");
          const attempts = (crashStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
          expect(attempts).toHaveLength(2);
          expect(attempts[0]).toMatchObject({ state: "failed" });
          expect(attempts[1]).toMatchObject({
            state: "succeeded",
            resultType: "stream"
          });

          const afterStep = steps.find((s) => s.id === "after-retry");
          expect(afterStep?.type).toBe("run");
          const afterAttempts = (afterStep as RunStep & { attempts: RunStepAttempt[] }).attempts;
          const afterResult = afterAttempts[0]! as Extract<RunStepAttempt, { resultType: "json" }>;
          expect(JSON.parse(afterResult.resultJson)).toEqual({ byte: [99] });
        });
      } finally {
        executeSpy.mockRestore();
      }
    });
  });

  describe("WorkflowRuntimeContext", () => {
    describe("run steps", () => {
      describe("getOrCreateRunStep()", () => {
        it("creates a new run step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const step = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
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
            const context = new WorkflowRuntimeContext(state.storage);
            const first = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const second = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(first).toEqual(second);
          });
        });

        it("leaves attempts empty until handleRunAttemptStarted", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const step = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step.attempts).toEqual([]);
          });
        });

        it("persists 'max_attempts' on the run step row", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const step = context.getOrCreateRunStep(createRunStepId("step-1"), {
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
      });

      describe("hasInProgressChildSteps()", () => {
        it("returns false when the run step has no direct child rows", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("leaf"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("leaf"));
            expect(context.hasInProgressChildSteps(createRunStepId("leaf"))).toBe(false);
          });
        });

        it("returns true when the only direct child run is still pending", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("parent"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("parent"));
            context.getOrCreateRunStep(createRunStepId("child"), {
              parentStepId: createRunStepId("parent")
            });
            expect(context.hasInProgressChildSteps(createRunStepId("parent"))).toBe(true);
          });
        });

        it("returns true when a direct child run step is running", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("parent"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("parent"));
            context.getOrCreateRunStep(createRunStepId("child"), {
              parentStepId: createRunStepId("parent")
            });
            context.handleRunAttemptStarted(createRunStepId("child"));
            expect(context.hasInProgressChildSteps(createRunStepId("parent"))).toBe(true);
          });
        });

        it("returns true when a direct child run exists in a non-failure state (e.g. pending)", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("gp"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("gp"));
            context.getOrCreateRunStep(createRunStepId("mid"), { parentStepId: createRunStepId("gp") });
            context.getOrCreateRunStep(createRunStepId("leaf"), {
              parentStepId: createRunStepId("mid")
            });
            expect(context.hasInProgressChildSteps(createRunStepId("gp"))).toBe(true);
          });
        });

        it("returns true when a direct child exists (pending or running) and false for a leaf", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("mid"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("mid"));
            context.getOrCreateRunStep(createRunStepId("leaf"), {
              parentStepId: createRunStepId("mid")
            });
            expect(context.hasInProgressChildSteps(createRunStepId("mid"))).toBe(true);
            expect(context.hasInProgressChildSteps(createRunStepId("leaf"))).toBe(false);
          });
        });

        it("returns false when the only direct child run has failed", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("par"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("par"));
            context.getOrCreateRunStep(createRunStepId("bad-child"), {
              parentStepId: createRunStepId("par"),
              maxAttempts: 1
            });
            context.handleRunAttemptStarted(createRunStepId("bad-child"));
            context.handleRunAttemptFailed(createRunStepId("bad-child"), { errorMessage: "x" });
            expect(context.hasInProgressChildSteps(createRunStepId("par"))).toBe(false);
          });
        });

        it("returns true when the only direct child is a non-run step (sleep) in waiting", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("run-parent"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("run-parent"));
            context.getOrCreateSleepStep(createSleepStepId("child-sleep"), {
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
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const started = context.handleRunAttemptStarted(createRunStepId("step-1"));
            expect(started).toMatchObject({ state: "started", stepId: "step-1" });
            const step = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step.attempts).toHaveLength(1);
            expect(step.attempts[0]).toMatchObject({ state: "started" });
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            expect(() => context.handleRunAttemptStarted(createRunStepId("nonexistent"))).toThrow(/not found/);
          });
        });

        it("throws when an attempt is already in progress", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            expect(() => context.handleRunAttemptStarted(createRunStepId("step-1"))).toThrow(/already in progress/);
          });
        });
      });

      describe("handleRunAttemptSucceeded()", () => {
        it("marks the in-flight attempt succeeded with a json result", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            const done = context.handleRunAttemptSucceeded(createRunStepId("step-1"), JSON.stringify(0));
            expect(done).toMatchObject({
              state: "succeeded",
              resultType: "json",
              resultJson: JSON.stringify(0)
            });
            const step = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step.attempts).toHaveLength(1);
            expect(step.attempts[0]).toMatchObject({
              state: "succeeded",
              resultType: "json",
              resultJson: JSON.stringify(0)
            });
          });
        });

        it("marks the in-flight attempt succeeded with result_type none", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            const done = context.handleRunAttemptSucceeded(createRunStepId("step-1"), null);
            expect(done).toMatchObject({
              state: "succeeded",
              resultType: "none"
            });
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            expect(() => context.handleRunAttemptSucceeded(createRunStepId("nonexistent"), null)).toThrow(/not found/);
          });
        });

        it("throws when no attempt is in progress", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(() => context.handleRunAttemptSucceeded(createRunStepId("step-1"), null)).toThrow(
              /No attempt in progress/
            );
          });
        });
      });

      describe("handleRunAttemptStreamResult()", () => {
        it("consumes a stream, stores chunks, and marks the attempt succeeded", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("stream-step"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("stream-step"));

            const chunks = [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])];
            const inputStream = new ReadableStream<Uint8Array>({
              start(controller) {
                for (const chunk of chunks) controller.enqueue(chunk);
                controller.close();
              }
            });

            const syntheticStream = await context.handleRunAttemptStreamResult(
              createRunStepId("stream-step"),
              inputStream
            );

            const step = context.getOrCreateRunStep(createRunStepId("stream-step"), { parentStepId: null });
            expect(step.attempts).toHaveLength(1);
            expect(step.attempts[0]).toMatchObject({
              state: "succeeded",
              resultType: "stream"
            });

            const storedChunks = state.storage.sql
              .exec<{ seq: number; data: ArrayBuffer }>(
                "SELECT seq, data FROM stream_chunks WHERE attempt_id = ? ORDER BY seq",
                step.attempts[0]!.id
              )
              .toArray();
            expect(storedChunks).toHaveLength(2);
            expect(new Uint8Array(storedChunks[0]!.data)).toEqual(new Uint8Array([1, 2, 3]));
            expect(new Uint8Array(storedChunks[1]!.data)).toEqual(new Uint8Array([4, 5, 6]));

            const reader = syntheticStream.getReader();
            const r1 = await reader.read();
            expect(r1.done).toBe(false);
            expect(new Uint8Array(r1.value!)).toEqual(new Uint8Array([1, 2, 3]));
            const r2 = await reader.read();
            expect(r2.done).toBe(false);
            expect(new Uint8Array(r2.value!)).toEqual(new Uint8Array([4, 5, 6]));
            const r3 = await reader.read();
            expect(r3.done).toBe(true);
          });
        });

        it("handles an empty stream", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("empty-stream"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("empty-stream"));

            const inputStream = new ReadableStream<Uint8Array>({
              start(controller) {
                controller.close();
              }
            });

            const syntheticStream = await context.handleRunAttemptStreamResult(
              createRunStepId("empty-stream"),
              inputStream
            );

            const step = context.getOrCreateRunStep(createRunStepId("empty-stream"), { parentStepId: null });
            expect(step.attempts[0]).toMatchObject({
              state: "succeeded",
              resultType: "stream"
            });

            const storedChunks = state.storage.sql
              .exec("SELECT seq FROM stream_chunks WHERE attempt_id = ?", step.attempts[0]!.id)
              .toArray();
            expect(storedChunks).toHaveLength(0);

            const reader = syntheticStream.getReader();
            const result = await reader.read();
            expect(result.done).toBe(true);
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const stream = new ReadableStream<Uint8Array>({
              start(controller) {
                controller.close();
              }
            });
            await expect(
              context.handleRunAttemptStreamResult(createRunStepId("nonexistent"), stream)
            ).rejects.toThrow(/not found/);
          });
        });

        it("throws when no attempt is in progress", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const stream = new ReadableStream<Uint8Array>({
              start(controller) {
                controller.close();
              }
            });
            await expect(
              context.handleRunAttemptStreamResult(createRunStepId("step-1"), stream)
            ).rejects.toThrow(/No in-flight attempt/);
          });
        });

        it("orphaned stream_chunks from a failed attempt do not corrupt the retried attempt", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { maxAttempts: 3, parentStepId: null });

            context.handleRunAttemptStarted(createRunStepId("step-1"));
            const step1 = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const attempt1Id = step1.attempts[0]!.id;

            state.storage.sql.exec(
              "INSERT INTO stream_chunks (attempt_id, seq, data) VALUES (?, ?, ?)",
              attempt1Id,
              0,
              new Uint8Array([0xDE, 0xAD])
            );
            state.storage.sql.exec(
              "INSERT INTO stream_chunks (attempt_id, seq, data) VALUES (?, ?, ?)",
              attempt1Id,
              1,
              new Uint8Array([0xBE, 0xEF])
            );

            context.handleRunAttemptFailed(createRunStepId("step-1"), {
              errorMessage: "stream interrupted"
            });

            context.handleRunAttemptStarted(createRunStepId("step-1"));

            const goodStream = new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(new Uint8Array([1, 2, 3]));
                controller.close();
              }
            });

            const syntheticStream = await context.handleRunAttemptStreamResult(
              createRunStepId("step-1"),
              goodStream
            );

            const step2 = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step2.attempts).toHaveLength(2);
            const failedAttempt = step2.attempts.find((a) => a.state === "failed");
            const succeededAttempt = step2.attempts.find((a) => a.state === "succeeded");
            expect(failedAttempt).toBeDefined();
            expect(succeededAttempt).toMatchObject({ state: "succeeded", resultType: "stream" });

            const attempt2Id = succeededAttempt!.id;

            const orphanedRows = state.storage.sql
              .exec<{ seq: number; data: ArrayBuffer }>(
                "SELECT seq, data FROM stream_chunks WHERE attempt_id = ? ORDER BY seq",
                attempt1Id
              )
              .toArray();
            expect(orphanedRows).toHaveLength(2);
            expect(new Uint8Array(orphanedRows[0]!.data)).toEqual(new Uint8Array([0xde, 0xad]));

            const goodRows = state.storage.sql
              .exec<{ seq: number; data: ArrayBuffer }>(
                "SELECT seq, data FROM stream_chunks WHERE attempt_id = ? ORDER BY seq",
                attempt2Id
              )
              .toArray();
            expect(goodRows).toHaveLength(1);
            expect(new Uint8Array(goodRows[0]!.data)).toEqual(new Uint8Array([1, 2, 3]));

            const reader = syntheticStream.getReader();
            const { value } = await reader.read();
            expect(new Uint8Array(value!)).toEqual(new Uint8Array([1, 2, 3]));
            const { done } = await reader.read();
            expect(done).toBe(true);
          });
        });
      });

      describe("getStoredStream()", () => {
        it("reconstructs a stream from stored chunks", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            context.handleRunAttemptSucceeded(createRunStepId("step-1"), null);

            const step = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const attemptId = step.attempts[0]!.id;

            state.storage.sql.exec(
              "INSERT INTO stream_chunks (attempt_id, seq, data) VALUES (?, ?, ?)",
              attemptId,
              0,
              new Uint8Array([10, 20])
            );
            state.storage.sql.exec(
              "INSERT INTO stream_chunks (attempt_id, seq, data) VALUES (?, ?, ?)",
              attemptId,
              1,
              new Uint8Array([30, 40])
            );

            const stream = context.getStoredStream(attemptId);
            const reader = stream.getReader();
            const r1 = await reader.read();
            expect(new Uint8Array(r1.value!)).toEqual(new Uint8Array([10, 20]));
            const r2 = await reader.read();
            expect(new Uint8Array(r2.value!)).toEqual(new Uint8Array([30, 40]));
            const r3 = await reader.read();
            expect(r3.done).toBe(true);
          });
        });

        it("returns an empty stream when no chunks exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            context.handleRunAttemptSucceeded(createRunStepId("step-1"), null);

            const step = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            const stream = context.getStoredStream(step.attempts[0]!.id);
            const reader = stream.getReader();
            const result = await reader.read();
            expect(result.done).toBe(true);
          });
        });
      });

      describe("handleRunAttemptFailed()", () => {
        it("marks terminal failed when max attempts exhausted", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { maxAttempts: 1, parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            const failed = context.handleRunAttemptFailed(createRunStepId("step-1"), { errorMessage: "error" });
            expect(failed).toMatchObject({
              state: "failed",
              errorMessage: "error",
              nextAttemptAt: undefined
            });
            const step = context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(step.attempts).toHaveLength(1);
            expect(step.attempts[0]).toMatchObject({ state: "failed", errorMessage: "error" });
          });
        });

        it("records next_attempt_at when retries remain", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const before = Date.now();
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            const failed = context.handleRunAttemptFailed(createRunStepId("step-1"), { errorMessage: "transient" });
            const after = Date.now();
            expect(failed.state).toBe("failed");
            if (failed.state !== "failed") throw new Error("expected failed");
            expect(failed.nextAttemptAt).toBeDefined();
            expect(failed.nextAttemptAt!.getTime()).toBeGreaterThanOrEqual(before + 250);
            expect(failed.nextAttemptAt!.getTime()).toBeLessThanOrEqual(after + 500 + 100);
          });
        });

        it("marks terminal failed when isNonRetryableStepError is true", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { maxAttempts: 10, parentStepId: null });
            context.handleRunAttemptStarted(createRunStepId("step-1"));
            const failed = context.handleRunAttemptFailed(createRunStepId("step-1"), {
              errorMessage: "x",
              isNonRetryableStepError: true
            });
            expect(failed).toMatchObject({ state: "failed", nextAttemptAt: undefined });
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            expect(() => context.handleRunAttemptFailed(createRunStepId("nonexistent"), { errorMessage: "e" })).toThrow(
              /not found/
            );
          });
        });

        it("throws when no attempt is in progress", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateRunStep(createRunStepId("step-1"), { parentStepId: null });
            expect(() => context.handleRunAttemptFailed(createRunStepId("step-1"), { errorMessage: "bad" })).toThrow(
              /No attempt in progress/
            );
          });
        });
      });
    });

    describe("sleep steps", () => {
      describe("getOrCreateSleepStep()", () => {
        it("creates a sleep step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            const step = context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
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
            const context = new WorkflowRuntimeContext(state.storage);
            const w = new Date();
            const first = context.getOrCreateSleepStep(createSleepStepId("sleep-1"), { wakeAt: w, parentStepId: null });
            const second = context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(),
              parentStepId: null
            });
            expect(first).toEqual(second);
          });
        });

        it("does not set a durable object alarm by itself", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            context.getOrCreateSleepStep(createSleepStepId("sleep-1"), { wakeAt, parentStepId: null });
            expect(await state.storage.getAlarm()).toBeNull();
          });
        });

        it("leaves an existing alarm unchanged when creating a sleep step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const prior = Date.now() + 999_999;
            await state.storage.setAlarm(prior);
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(Date.now() + 60_000),
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBe(prior);
          });
        });

        it("does not set an alarm when re-reading an existing sleep step after deleteAlarm", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const pastWakeAt = new Date(Date.now() - 10_000);
            context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: pastWakeAt,
              parentStepId: null
            });
            await state.storage.deleteAlarm();
            context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: pastWakeAt,
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBeNull();
          });
        });
      });

      describe("handleSleepStepElapsed()", () => {
        it("moves a sleep step from 'waiting' to 'elapsed'", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(),
              parentStepId: null
            });
            context.handleSleepStepElapsed(createSleepStepId("sleep-1"));
            const step = context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(),
              parentStepId: null
            });
            expect(step).toMatchObject({
              state: "elapsed",
              resolvedAt: expect.any(Date)
            });
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            expect(() => context.handleSleepStepElapsed(createSleepStepId("nonexistent"))).toThrow(/not found/);
          });
        });

        it("throws when the step is already elapsed", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateSleepStep(createSleepStepId("sleep-1"), {
              wakeAt: new Date(Date.now() + 60_000),
              parentStepId: null
            });
            context.handleSleepStepElapsed(createSleepStepId("sleep-1"));
            expect(() => context.handleSleepStepElapsed(createSleepStepId("sleep-1"))).toThrow(
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
            const context = new WorkflowRuntimeContext(state.storage);
            const step = context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(step).toMatchObject({
              id: "wait-1",
              type: "wait",
              state: "waiting",
              eventName: "event-1"
            });
          });
        });

        it("creates a wait step when a timeout is provided", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() + 60_000);
            const step = context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
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
            const context = new WorkflowRuntimeContext(state.storage);
            const first = context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            const second = context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(first).toEqual(second);
          });
        });

        it("does not set a durable object alarm by itself", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: new Date(Date.now() + 60_000)
            });
            expect(await state.storage.getAlarm()).toBeNull();
          });
        });

        it("leaves an existing alarm unchanged when creating a wait step with timeout", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const prior = Date.now() + 999_999;
            await state.storage.setAlarm(prior);
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: new Date(Date.now() + 60_000)
            });
            expect(await state.storage.getAlarm()).toBe(prior);
          });
        });

        it("satisfies from a queued inbound event when creating the wait step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            state.storage.sql.exec(
              `INSERT INTO inbound_events (event_name, payload) VALUES (?, ?)`,
              "event-1",
              JSON.stringify({ v: 1 })
            );
            const step = context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(step).toMatchObject({
              state: "satisfied",
              payload: { v: 1 }
            });
          });
        });

        it("rejects a second inbound_events row with the same claimed_by", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
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
            const context = new WorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() - 1000);
            context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt
            });
            context.handleWaitStepTimedOut(createWaitStepId("wait-1"));
            const step = context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null
            });
            expect(step).toMatchObject({ state: "timed_out" });
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            expect(() => context.handleWaitStepTimedOut(createWaitStepId("nonexistent"))).toThrow(/not found/);
          });
        });

        it("throws when the step is already timed out", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            context.getOrCreateWaitStep(createWaitStepId("wait-1"), {
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: new Date(Date.now() - 1000)
            });
            context.handleWaitStepTimedOut(createWaitStepId("wait-1"));
            expect(() => context.handleWaitStepTimedOut(createWaitStepId("wait-1"))).toThrow(
              /Expected 'waiting' but got timed_out/
            );
          });
        });
      });
    });
  });
});
