import { runInDurableObject, runDurableObjectAlarm } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import {
  WorkflowRuntimeContext,
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
        expect(steps[0]).toMatchObject({
          type: "run",
          state: "failed",
          errorMessage: "NonRetryableStepError: This is a non-retryable step error",
          errorName: "NonRetryableStepError",
          attemptCount: 1
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
        expect(steps[0]).toMatchObject({
          type: "run",
          state: "failed",
          errorMessage: "Error: test",
          errorName: "Error",
          attemptCount: 2
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
        expect(steps[0]).toMatchObject({
          type: "run",
          attemptCount: 2,
          state: "succeeded"
        });
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
        expect(steps[0]).toMatchObject({ id: "step-a", type: "run", state: "succeeded" });
        expect(steps[1]).toMatchObject({ id: "step-b", type: "run", state: "succeeded" });

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
        expect(steps.find((s) => s.id === "before-sleep")).toMatchObject({ type: "run", state: "succeeded" });
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
          expect(steps.find((s) => s.id === "L0")).toMatchObject({
            type: "run",
            parentStepId: null,
            state: "succeeded"
          });
          expect(steps.find((s) => s.id === "L1")).toMatchObject({
            type: "run",
            parentStepId: "L0",
            state: "succeeded"
          });
          expect(steps.find((s) => s.id === "L2")).toMatchObject({
            type: "run",
            parentStepId: "L1",
            state: "succeeded"
          });

          const events = await instance.getStepEvents_experimental();
          for (const id of ["L0", "L1"] as const) {
            expect(events.filter((event) => event.stepId === id && event.type === "attempt_failed")).toHaveLength(0);
          }
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
          expect(steps.find((s) => s.id === "root-after")).toMatchObject({
            type: "run",
            parentStepId: null,
            state: "succeeded"
          });
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
          expect(steps.find((s) => s.id === "branch-a")).toMatchObject({
            type: "run",
            parentStepId: null,
            state: "succeeded"
          });
          expect(steps.find((s) => s.id === "branch-a-inner")).toMatchObject({
            type: "run",
            parentStepId: "branch-a",
            state: "succeeded"
          });
          expect(steps.find((s) => s.id === "branch-b")).toMatchObject({
            type: "run",
            parentStepId: null,
            state: "succeeded"
          });
          expect(steps.find((s) => s.id === "branch-b-inner")).toMatchObject({
            type: "run",
            parentStepId: "branch-b",
            state: "succeeded"
          });

          const events = await instance.getStepEvents_experimental();
          expect(events.filter((event) => event.stepId === "branch-a" && event.type === "attempt_failed")).toHaveLength(0);
          expect(events.filter((event) => event.stepId === "branch-b" && event.type === "attempt_failed")).toHaveLength(0);
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
          expect(steps.find((s) => s.id === "nested-branch-inner")).toMatchObject({
            type: "run",
            parentStepId: "nested-branch",
            state: "succeeded"
          });
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
            .poll(() => instance.getSteps_experimental().find((s) => s.id === "deep-wait")?.state)
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
            payload: JSON.stringify({ ok: true })
          });
          expect(instance.getSteps_experimental().find((s) => s.id === "outer-wait")).toMatchObject({
            type: "run",
            state: "succeeded"
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
          instance.onStatusChange_experimental = async (status) => {
            if (status === "running") return;
            resolve(status);
          };

          await instance.create({ definitionVersion: "2026-03-19" });
          await expect(promise).resolves.toBe("failed");

          const steps = instance.getSteps_experimental();
          expect(steps.find((s) => s.id === "fail-inner")).toMatchObject({
            type: "run",
            state: "failed",
            errorName: "NonRetryableStepError"
          });
          expect(steps.find((s) => s.id === "fail-outer")).toMatchObject({
            type: "run",
            state: "failed",
            errorName: "NonRetryableStepError"
          });
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
            const events = await instance.getStepEvents_experimental();
            expect(events.filter((event) => event.stepId === "suspend-outer" && event.type === "attempt_failed")).toHaveLength(0);
            expect(instance.getSteps_experimental().find((s) => s.id === "suspend-outer")).toMatchObject({
              type: "run",
              state: "succeeded"
            });
            expect(instance.getSteps_experimental().find((s) => s.id === "suspend-inner")).toMatchObject({
              type: "run",
              state: "succeeded"
            });
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
            expect(steps.find((s) => s.id === "ex-inner")).toMatchObject({
              type: "run",
              state: "failed",
              errorName: "Error",
              errorMessage: "Error: always fail"
            });
            expect(steps.find((s) => s.id === "ex-outer")).toMatchObject({
              type: "run",
              state: "failed",
              errorName: "Error",
              errorMessage: "Error"
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
            expect(steps.find((s) => s.id === "post-inner")).toMatchObject({
              type: "run",
              state: "succeeded"
            });
            expect(steps.find((s) => s.id === "post-outer")).toMatchObject({
              type: "run",
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
              .poll(() => instance.getSteps_experimental().find((s) => s.id === "root-deep-wait")?.state)
              .toBe("waiting");

            const eventsBefore = await instance.getStepEvents_experimental();
            expect(
              eventsBefore.filter((event) => event.stepId === "root-wait-run" && event.type === "attempt_failed")
            ).toHaveLength(0);

            await instance.handleInboundEvent("root-deep-ev", true);
            await expect(promise).resolves.toBe("completed");

            const eventsAfter = await instance.getStepEvents_experimental();
            expect(
              eventsAfter.filter((event) => event.stepId === "root-wait-run" && event.type === "attempt_failed")
            ).toHaveLength(0);
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
          expect(steps.find((s) => s.id === "parallel-run")).toMatchObject({
            type: "run",
            state: "succeeded"
          });
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
          expect(steps.find((s) => s.id === "parallel-fail")).toMatchObject({
            type: "run",
            state: "failed",
            errorName: "NonRetryableStepError"
          });
          expect(steps.find((s) => s.id === "parallel-ok")).toMatchObject({
            type: "run",
            state: "succeeded"
          });
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
            .poll(() => instance.getSteps_experimental().find((s) => s.id === "allsettled-rerun-wait")?.state)
            .toBe("waiting");

          const steps = instance.getSteps_experimental();
          expect(steps.find((s) => s.id === "allsettled-rerun-wait")).toMatchObject({
            type: "wait",
            state: "waiting"
          });
          // Unlike `Promise.all`, `allSettled` waits for every branch before returning, so the run can finish
          // durably before we rethrow the `wait()` rejection.
          expect(steps.find((s) => s.id === "allsettled-rerun-run")).toMatchObject({
            type: "run",
            state: "succeeded"
          });
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
            .poll(() => instance.getSteps_experimental().find((s) => s.id === "parallel-wait")?.state)
            .toBe("waiting");

          const steps = instance.getSteps_experimental();
          expect(steps.find((s) => s.id === "parallel-wait")).toMatchObject({
            type: "wait",
            state: "waiting"
          });
          // `wait()` usually wins the race; the run branch can still be mid-flight so the step may not yet be "succeeded".
          expect(steps.find((s) => s.id === "parallel-run")).toMatchObject({
            type: "run",
            state: "running"
          });
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
            .poll(() => instance.getSteps_experimental().find((s) => s.id === "wait-1")?.state)
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
            .poll(() => instance.getSteps_experimental().find((s) => s.id === "wait-1")?.state)
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
            .poll(() => instance.getSteps_experimental().find((s) => s.id === "wait-1")?.state)
            .toBe("waiting");
          await instance.pause();

          await instance.handleInboundEvent("event-1", { data: "test" });

          await instance.resume();
          await expect(promise).resolves.toBe("completed");

          expect(instance.getSteps_experimental().find((s) => s.id === "wait-1")).toMatchObject({
            type: "wait",
            state: "satisfied",
            payload: JSON.stringify({ data: "test" })
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
            .poll(() => instance.getSteps_experimental().find((s) => s.id === "wait-1")?.state)
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

  describe("WorkflowRuntimeContext", () => {
    describe("run steps", () => {
      describe("getOrCreateStep()", () => {
        it("creates a new run step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const step = await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            expect(step).toMatchObject({
              id: "step-1",
              type: "run",
              state: "pending",
              attemptCount: 0
            });
          });
        });

        it("creates a run step once and returns the same durable row on subsequent reads", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const first = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              parentStepId: null
            });
            const second = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              parentStepId: null
            });
            expect(first).toEqual(second);
          });
        });

        it("does not write an 'attempt_started' step event when a run step is already in progress", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });

            await expect(instance.getStepEvents_experimental()).toMatchObject([]);
          });
        });

        it("persists 'max_attempts' on the run step row", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const step = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
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

      describe("hasRunningOrWaitingChildSteps()", () => {
        it("returns false when the run step has no direct child rows", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("leaf"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("leaf"), { type: "running", attemptCount: 1 });
            await expect(context.hasRunningOrWaitingChildSteps(createRunStepId("leaf"))).resolves.toBe(false);
          });
        });

        it("returns true when the only direct child run is still pending", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("parent"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("parent"), { type: "running", attemptCount: 1 });
            await context.getOrCreateStep(createRunStepId("child"), {
              type: "run",
              parentStepId: createRunStepId("parent")
            });
            await expect(context.hasRunningOrWaitingChildSteps(createRunStepId("parent"))).resolves.toBe(true);
          });
        });

        it("returns true when a direct child run step is running", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("parent"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("parent"), { type: "running", attemptCount: 1 });
            await context.getOrCreateStep(createRunStepId("child"), {
              type: "run",
              parentStepId: createRunStepId("parent")
            });
            await context.handleRunAttemptEvent(createRunStepId("child"), { type: "running", attemptCount: 1 });
            await expect(context.hasRunningOrWaitingChildSteps(createRunStepId("parent"))).resolves.toBe(true);
          });
        });

        it("returns true when a direct child run exists in a non-failure state (e.g. pending)", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("gp"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("gp"), { type: "running", attemptCount: 1 });
            await context.getOrCreateStep(createRunStepId("mid"), { type: "run", parentStepId: createRunStepId("gp") });
            await context.getOrCreateStep(createRunStepId("leaf"), {
              type: "run",
              parentStepId: createRunStepId("mid")
            });
            await expect(context.hasRunningOrWaitingChildSteps(createRunStepId("gp"))).resolves.toBe(true);
          });
        });

        it("returns true when a direct child exists (pending or running) and false for a leaf", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("mid"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("mid"), { type: "running", attemptCount: 1 });
            await context.getOrCreateStep(createRunStepId("leaf"), {
              type: "run",
              parentStepId: createRunStepId("mid")
            });
            await expect(context.hasRunningOrWaitingChildSteps(createRunStepId("mid"))).resolves.toBe(true);
            await expect(context.hasRunningOrWaitingChildSteps(createRunStepId("leaf"))).resolves.toBe(false);
          });
        });

        it("returns false when the only direct child run has failed", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("par"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("par"), { type: "running", attemptCount: 1 });
            await context.getOrCreateStep(createRunStepId("bad-child"), {
              type: "run",
              parentStepId: createRunStepId("par")
            });
            await context.handleRunAttemptEvent(createRunStepId("bad-child"), { type: "running", attemptCount: 1 });
            await context.handleRunAttemptEvent(createRunStepId("bad-child"), {
              type: "failed",
              errorMessage: "x",
              attemptCount: 1,
              isNonRetryableStepError: true
            });
            await expect(context.hasRunningOrWaitingChildSteps(createRunStepId("par"))).resolves.toBe(false);
          });
        });

        it("returns true when the only direct child is a non-run step (sleep) in waiting", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("run-parent"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("run-parent"), { type: "running", attemptCount: 1 });
            const wakeAt = new Date(Date.now() + 60_000);
            await context.getOrCreateStep(createSleepStepId("child-sleep"), {
              type: "sleep",
              wakeAt,
              parentStepId: createRunStepId("run-parent")
            });
            await expect(context.hasRunningOrWaitingChildSteps(createRunStepId("run-parent"))).resolves.toBe(true);
          });
        });
      });

      describe("handleRunAttemptEvent({ type: 'running' })", () => {
        it("moves a run step from 'pending' to 'running'", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            const updatedStep = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              parentStepId: null
            });
            expect(updatedStep).toMatchObject({
              state: "running",
              attemptCount: 1
            });
          });
        });

        it("writes an 'attempt_started' step event when a run step is started", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "attempt_started",
                stepId: "step-1",
                attemptNumber: 1,
                recordedAt: expect.any(Date)
              }
            ]);
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await expect(
              context.handleRunAttemptEvent(createRunStepId("nonexistent"), {
                type: "running",
                attemptCount: 1
              })
            ).rejects.toThrow(/not found/);
          });
        });

        it("throws when the step is not in 'pending' state", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await expect(
              context.handleRunAttemptEvent(createRunStepId("step-1"), {
                type: "running",
                attemptCount: 2
              })
            ).rejects.toThrow(/Expected 'pending' but got running/);
          });
        });

        it("rejects when 'next_attempt_at' is in the future", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "backoff"
            });
            const future = Date.now() + 3600_000;
            state.storage.sql.exec("UPDATE steps SET next_attempt_at = ? WHERE id = 'step-1'", future);
            await expect(
              context.handleRunAttemptEvent(createRunStepId("step-1"), {
                type: "running",
                attemptCount: 2
              })
            ).rejects.toThrow(/next attempt at/);
          });
        });

        it("rejects when 'attemptCount' does not match the expected next attempt", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await expect(
              context.handleRunAttemptEvent(createRunStepId("step-1"), {
                type: "running",
                attemptCount: 99
              })
            ).rejects.toThrow(/Expected 98 but got 0/);
          });
        });
      });

      describe("handleRunAttemptEvent({ type: 'succeeded' })", () => {
        it("moves a run step from 'running' to 'succeeded'", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "succeeded",
              attemptCount: 1,
              result: JSON.stringify({ value: 0 })
            });
            const updatedStep = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              parentStepId: null
            });
            expect(updatedStep).toMatchObject({
              state: "succeeded",
              attemptCount: 1,
              result: JSON.stringify({ value: 0 }),
              resolvedAt: expect.any(Date)
            });
          });
        });

        it("writes an 'attempt_succeeded' step event when a run step is succeeded", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "succeeded",
              attemptCount: 1,
              result: JSON.stringify({ value: 0 })
            });
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "attempt_started",
                stepId: "step-1",
                attemptNumber: 1,
                recordedAt: expect.any(Date)
              },
              {
                type: "attempt_succeeded",
                stepId: "step-1",
                attemptNumber: 1,
                recordedAt: expect.any(Date)
              }
            ]);
          });
        });

        it("throws when the attempt number does not match", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await expect(
              context.handleRunAttemptEvent(createRunStepId("step-1"), {
                type: "succeeded",
                attemptCount: 999,
                result: JSON.stringify({ value: 0 })
              })
            ).rejects.toThrow(/Unexpected attempt count/);
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await expect(
              context.handleRunAttemptEvent(createRunStepId("nonexistent"), {
                type: "succeeded",
                attemptCount: 1,
                result: JSON.stringify({ value: 0 })
              })
            ).rejects.toThrow(/not found/);
          });
        });

        it("rejects when the step is still in 'pending' state", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await expect(
              context.handleRunAttemptEvent(createRunStepId("step-1"), {
                type: "succeeded",
                attemptCount: 1,
                result: JSON.stringify({ value: 1 })
              })
            ).rejects.toThrow(/Expected 'running' but got pending/);
          });
        });
      });

      describe("handleRunAttemptEvent({ type: 'failed' })", () => {
        it("moves a run step from 'running' to 'failed'", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              maxAttempts: 1,
              parentStepId: null
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "error"
            });
            const updatedStep = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              maxAttempts: 1,
              parentStepId: null
            });
            expect(updatedStep).toMatchObject({
              state: "failed",
              attemptCount: 1,
              errorMessage: "error"
            });
          });
        });

        it("writes an 'attempt_failed' step event when a run step is failed", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              maxAttempts: 1,
              parentStepId: null
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "error"
            });
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "attempt_started",
                stepId: "step-1",
                attemptNumber: 1,
                recordedAt: expect.any(Date)
              },
              {
                type: "attempt_failed",
                stepId: "step-1",
                attemptNumber: 1,
                errorMessage: "error",
                recordedAt: expect.any(Date)
              }
            ]);
          });
        });

        it("moves a run step back to 'pending' when retries are available", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "transient error"
            });
            const updatedStep = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              parentStepId: null
            });
            expect(updatedStep).toMatchObject({
              state: "pending",
              attemptCount: 1
            });
            expect(updatedStep).toHaveProperty("nextAttemptAt");
            expect((updatedStep as { nextAttemptAt: Date }).nextAttemptAt.getTime()).toBeGreaterThan(Date.now());
          });
        });

        it("moves a run step to 'failed' when 'isNonRetryableStepError' is true and retries are available", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              maxAttempts: 10,
              parentStepId: null
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "transient error",
              isNonRetryableStepError: true
            });
            const updatedStep = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              parentStepId: null
            });
            expect(updatedStep).toMatchObject({
              state: "failed",
              attemptCount: 1,
              errorMessage: "transient error"
            });
          });
        });

        it("schedules an alarm when retries are available", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "transient error"
            });
            const alarm = await state.storage.getAlarm();
            expect(alarm).not.toBeNull();
            expect(alarm).toBeGreaterThan(Date.now());
          });
        });

        it("replaces an existing alarm when retries are available", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            await state.storage.setAlarm(Date.now() + 999_999);

            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "transient error"
            });
            const alarm = await state.storage.getAlarm();
            expect(alarm).not.toBeNull();
            expect(alarm).toBeLessThan(Date.now() + 999_999);
          });
        });

        it("writes an 'attempt_failed' step event when retrying", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "transient error"
            });
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "attempt_started",
                stepId: "step-1",
                attemptNumber: 1
              },
              {
                type: "attempt_failed",
                stepId: "step-1",
                attemptNumber: 1,
                errorMessage: "transient error"
              }
            ]);
          });
        });

        it("throws when the attempt number does not match", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await expect(
              context.handleRunAttemptEvent(createRunStepId("step-1"), {
                type: "failed",
                attemptCount: 999,
                errorMessage: "error"
              })
            ).rejects.toThrow(/Unexpected attempt count/);
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await expect(
              context.handleRunAttemptEvent(createRunStepId("nonexistent"), {
                type: "failed",
                attemptCount: 1,
                errorMessage: "error"
              })
            ).rejects.toThrow(/not found/);
          });
        });

        it("uses backoff delay for next attempt when retrying", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const before = Date.now();
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "running",
              attemptCount: 1
            });
            await context.handleRunAttemptEvent(createRunStepId("step-1"), {
              type: "failed",
              attemptCount: 1,
              errorMessage: "transient error"
            });
            const after = Date.now();
            const updatedStep = await context.getOrCreateStep(createRunStepId("step-1"), {
              type: "run",
              parentStepId: null
            });
            expect(updatedStep).toMatchObject({
              state: "pending",
              attemptCount: 1
            });
            const nextAttemptAt = (updatedStep as { nextAttemptAt: Date }).nextAttemptAt.getTime();
            expect(nextAttemptAt).toBeGreaterThanOrEqual(before + 250);
            expect(nextAttemptAt).toBeLessThanOrEqual(after + 500 + 100);
          });
        });

        it("rejects when the step is still in 'pending' state", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createRunStepId("step-1"), { type: "run", parentStepId: null });
            await expect(
              context.handleRunAttemptEvent(createRunStepId("step-1"), {
                type: "failed",
                attemptCount: 1,
                errorMessage: "bad"
              })
            ).rejects.toThrow(/Expected 'running' but got pending/);
          });
        });
      });
    });

    describe("sleep steps", () => {
      describe("getOrCreateStep()", () => {
        it("creates a sleep step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            const step = await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: wakeAt,
              parentStepId: null
            });
            expect(step).toMatchObject({
              id: "sleep-1",
              type: "sleep",
              state: "waiting",
              wakeAt: wakeAt
            });
          });
        });

        it("creates a sleep step once and returns the same durable row on subsequent reads", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const first = await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: new Date(),
              parentStepId: null
            });
            const second = await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: new Date(),
              parentStepId: null
            });
            expect(first).toEqual(second);
          });
        });

        it("writes a 'sleep_waiting' step event", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const wakeAt = new Date();
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: wakeAt,
              parentStepId: null
            });
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "sleep_waiting",
                stepId: "sleep-1",
                wakeAt: wakeAt,
                recordedAt: expect.any(Date)
              }
            ]);
          });
        });

        it("schedules an alarm", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: wakeAt,
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBe(wakeAt.getTime());
          });
        });

        it("replaces any existing alarm with a new alarm", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            await state.storage.setAlarm(Date.now() + 999_999);

            const context = new WorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: wakeAt,
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBe(wakeAt.getTime());
          });
        });

        it("does not schedule an alarm when an existing sleep step has a past wake_at", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const pastWakeAt = new Date(Date.now() - 10_000);
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: pastWakeAt,
              parentStepId: null
            });
            await state.storage.deleteAlarm();

            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: pastWakeAt,
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBeNull();
          });
        });

        it("does not append a second sleep_waiting step_events row when the sleep step already exists", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 30_000);
            await context.getOrCreateStep(createSleepStepId("sleep-1"), { type: "sleep", wakeAt, parentStepId: null });
            await context.getOrCreateStep(createSleepStepId("sleep-1"), { type: "sleep", wakeAt, parentStepId: null });
            const events = await instance.getStepEvents_experimental();
            expect(events).toHaveLength(1);
            expect(events[0]).toMatchObject({
              type: "sleep_waiting",
              stepId: "sleep-1",
              wakeAt: wakeAt,
              recordedAt: expect.any(Date)
            });
          });
        });

        it("does not set an alarm when re-reading an already elapsed sleep step", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: new Date(Date.now() + 60_000),
              parentStepId: null
            });
            await state.storage.deleteAlarm();
            const now = Date.now();
            state.storage.sql.exec(
              "UPDATE steps SET state = 'elapsed', wake_at = NULL, resolved_at = ? WHERE id = 'sleep-1'",
              now
            );
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: new Date(),
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBeNull();
          });
        });
      });

      describe("handleSleepStepEvent({ type: 'elapsed' })", () => {
        it("moves a sleep step from 'waiting' to 'elapsed'", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: new Date(),
              parentStepId: null
            });
            context.handleSleepStepEvent(createSleepStepId("sleep-1"), { type: "elapsed" });
            const updatedStep = await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: new Date(),
              parentStepId: null
            });
            expect(updatedStep).toMatchObject({
              state: "elapsed",
              resolvedAt: expect.any(Date)
            });
          });
        });

        it("writes a 'sleep_elapsed' step event when a sleep step is elapsed", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const wakeAt = new Date(Date.now() + 60_000);
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: wakeAt,
              parentStepId: null
            });
            context.handleSleepStepEvent(createSleepStepId("sleep-1"), { type: "elapsed" });
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "sleep_waiting",
                stepId: "sleep-1",
                wakeAt: wakeAt,
                recordedAt: expect.any(Date)
              },
              {
                type: "sleep_elapsed",
                stepId: "sleep-1",
                recordedAt: expect.any(Date)
              }
            ]);
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            expect(() => context.handleSleepStepEvent(createSleepStepId("nonexistent"), { type: "elapsed" })).toThrow(
              /not found/
            );
          });
        });

        it("throws when the step is already elapsed", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createSleepStepId("sleep-1"), {
              type: "sleep",
              wakeAt: new Date(),
              parentStepId: null
            });
            context.handleSleepStepEvent(createSleepStepId("sleep-1"), { type: "elapsed" });
            expect(() => context.handleSleepStepEvent(createSleepStepId("sleep-1"), { type: "elapsed" })).toThrow(
              /Expected 'waiting' but got elapsed/
            );
          });
        });
      });
    });

    describe("wait steps", () => {
      describe("getOrCreateStep()", () => {
        it("creates a wait step when no timeout is provided", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const step = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
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
            const step = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: timeoutAt
            });
            expect(step).toMatchObject({
              id: "wait-1",
              type: "wait",
              state: "waiting",
              eventName: "event-1",
              timeoutAt: timeoutAt
            });
          });
        });

        it("writes a 'wait_waiting' step event", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() + 60_000);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: timeoutAt
            });
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "wait_waiting",
                stepId: "wait-1",
                eventName: "event-1",
                timeoutAt: timeoutAt,
                recordedAt: expect.any(Date)
              }
            ]);
          });
        });

        it("creates a wait step once and returns the same durable row on subsequent reads", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const first = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: new Date(Date.now() + 60_000)
            });
            const second = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: new Date(Date.now() + 60_000)
            });
            expect(first).toEqual(second);
          });
        });

        it("schedules an alarm when a timeout is provided", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() + 60_000);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: timeoutAt
            });
            expect(await state.storage.getAlarm()).toBe(timeoutAt.getTime());
          });
        });

        it("replaces any existing alarm with a new alarm when a timeout is provided", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            await state.storage.setAlarm(Date.now() + 999_999);

            const context = new WorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() + 60_000);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: timeoutAt
            });
            expect(await state.storage.getAlarm()).toBe(timeoutAt.getTime());
          });
        });

        it("doesn't schedule an alarm when no timeout is provided", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            expect(await state.storage.getAlarm()).toBeNull();
          });
        });

        it("uses the stored timeout_at for the alarm, not the caller-provided timeoutAt", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const originalTimeout = new Date(Date.now() + 60_000);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: originalTimeout
            });
            await state.storage.deleteAlarm();

            const shiftedTimeout = new Date(Date.now() + 120_000);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: shiftedTimeout
            });
            expect(await state.storage.getAlarm()).toBe(originalTimeout.getTime());
          });
        });

        it("does not schedule an alarm when an existing wait step has a past timeout_at", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const pastTimeout = new Date(Date.now() - 10_000);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: pastTimeout
            });
            await state.storage.deleteAlarm();

            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: pastTimeout
            });
            expect(await state.storage.getAlarm()).toBeNull();
          });
        });
      });

      describe("handleInboundEvent()", () => {
        it("moves a wait step from 'waiting' to 'satisfied' when event is delivered", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            await instance.handleInboundEvent("event-1", "payload");
            const updatedStep = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            expect(updatedStep).toMatchObject({
              state: "satisfied",
              payload: JSON.stringify("payload"),
              resolvedAt: expect.any(Date)
            });
          });
        });

        it("writes a 'wait_satisfied' step event when a wait step is satisfied", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            await instance.handleInboundEvent("event-1", "payload");
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "wait_waiting",
                stepId: "wait-1",
                eventName: "event-1",
                recordedAt: expect.any(Date)
              },
              {
                type: "wait_satisfied",
                stepId: "wait-1",
                payload: JSON.stringify("payload"),
                recordedAt: expect.any(Date)
              }
            ]);
          });
        });

        it("queues the event when no matching wait step exists", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            await instance.handleInboundEvent("event-1", "queued-payload");
            const context = new WorkflowRuntimeContext(state.storage);
            const step = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            expect(step).toMatchObject({
              state: "satisfied",
              payload: JSON.stringify("queued-payload")
            });
          });
        });

        it("consumes queued inbound events in FIFO order when several waits are created", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            await instance.handleInboundEvent("event-1", "first");
            // Distinct `created_at` so FIFO ordering does not depend on random `inbound_events.id` when timestamps tie.
            await new Promise((r) => setTimeout(r, 2));
            await instance.handleInboundEvent("event-1", "second");
            const context = new WorkflowRuntimeContext(state.storage);
            const step1 = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            const step2 = await context.getOrCreateStep(createWaitStepId("wait-2"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            expect(step1).toMatchObject({
              state: "satisfied",
              payload: JSON.stringify("first")
            });
            expect(step2).toMatchObject({
              state: "satisfied",
              payload: JSON.stringify("second")
            });
          });
        });

        it("satisfies the earliest matching wait step when multiple are waiting", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            await context.getOrCreateStep(createWaitStepId("wait-2"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            await instance.handleInboundEvent("event-1", "payload");
            const step1 = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            const step2 = await context.getOrCreateStep(createWaitStepId("wait-2"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null
            });
            expect(step1).toMatchObject({
              state: "satisfied",
              payload: JSON.stringify("payload")
            });
            expect(step2).toMatchObject({ state: "waiting" });
          });
        });

        it("does not change steps when the workflow is terminal", async () => {
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

              const stepsBefore = instance.getSteps_experimental();
              const eventsBefore = instance.getStepEvents_experimental();

              await instance.handleInboundEvent("event-1", { ignored: true });

              expect(instance.getSteps_experimental()).toEqual(stepsBefore);
              expect(instance.getStepEvents_experimental()).toEqual(eventsBefore);
            });
          } finally {
            executeSpy.mockRestore();
          }
        });
      });

      describe("handleWaitStepEvent({ type: 'timed_out' })", () => {
        it("moves a wait step from 'waiting' to 'timed_out'", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() - 1000);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: timeoutAt
            });
            context.handleWaitStepEvent(createWaitStepId("wait-1"), { type: "timed_out" });
            const updatedStep = await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: timeoutAt
            });
            expect(updatedStep).toMatchObject({
              state: "timed_out",
              resolvedAt: expect.any(Date)
            });
          });
        });

        it("writes a 'wait_timed_out' step event when a wait step is timed out", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            const timeoutAt = new Date(Date.now() - 1000);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: timeoutAt
            });
            context.handleWaitStepEvent(createWaitStepId("wait-1"), { type: "timed_out" });
            expect(await instance.getStepEvents_experimental()).toMatchObject([
              {
                type: "wait_waiting",
                stepId: "wait-1",
                eventName: "event-1",
                timeoutAt: timeoutAt,
                recordedAt: expect.any(Date)
              },
              {
                type: "wait_timed_out",
                stepId: "wait-1",
                recordedAt: expect.any(Date)
              }
            ]);
          });
        });

        it("throws when the step does not exist", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            expect(() => context.handleWaitStepEvent(createWaitStepId("nonexistent"), { type: "timed_out" })).toThrow(
              /not found/
            );
          });
        });

        it("throws when the step is already timed out", async () => {
          const stub = env.TEST_WORKFLOW_RUNTIME.getByName(crypto.randomUUID());
          await runInDurableObject(stub, async (_instance, state) => {
            const context = new WorkflowRuntimeContext(state.storage);
            await context.getOrCreateStep(createWaitStepId("wait-1"), {
              type: "wait",
              eventName: "event-1",
              parentStepId: null,
              timeoutAt: new Date(Date.now() - 1000)
            });
            context.handleWaitStepEvent(createWaitStepId("wait-1"), { type: "timed_out" });
            expect(() => context.handleWaitStepEvent(createWaitStepId("wait-1"), { type: "timed_out" })).toThrow(
              /Expected 'waiting' but got timed_out/
            );
          });
        });
      });
    });
  });
});
