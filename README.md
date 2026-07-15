## workerflow

This is a user-land implementation of a workflow engine using Cloudflare primitives. The runtime is implemented as a Durable Object called `WorkflowRuntime`. It owns durable state in SQLite, including step state, step and workflow events, and inbound events, and it drives execution by repeatedly invoking your workflow definition, which is a normal Cloudflare WorkerEntrypoint.

One advantage of owning your own runtime as a Durable Object is that you can hook into the original state changes and extend the runtime’s capabilities however you want. For example, because the runtime is just a Durable Object, you can let clients connect over WebSocket or expose a method that streams step-state updates as they happen. The runtime does expect a few things, such as alarms to wake it up at the right time, but most of the implementation is open to extension.

## Installation

```bash
npm install workerflow
```

## Usage

Import **`WorkflowRuntime`** and **`WorkflowDefinition`**, then define two classes: a **Durable Object** subclass that resolves definition versions, and a **`WorkerEntrypoint`** subclass that implements **`execute()`** using **`run`**, **`sleep`**, and **`wait`**.

Pin SQLite-backed storage on the runtime class in **`wrangler.toml`** (or the equivalent config) so the DO can use **`SqlStorage`**. Set the **`nodejs_compat`** compatibility flag so **`node:async_hooks`** (**`AsyncLocalStorage`**, used by **`WorkflowDefinition`**) resolves in the Workers runtime.

```toml
# wrangler.toml (illustrative)
name = "example-worker"
main = "src/worker.ts"
compatibility_date = "2026-01-28"
compatibility_flags = [ "nodejs_compat" ]

[durable_objects]
bindings = [
  { name = "ORDER_WORKFLOW", class_name = "OrderWorkflowRuntime" }
]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["OrderWorkflowRuntime"]
```

In your Worker module, export the runtime, the definition, and a **`fetch`** handler (or queue consumer, cron trigger, and so on) that obtains a namespace stub and calls **`create`** to pin the workflow input:

```ts
// src/worker.ts
import { WorkflowDefinition, WorkflowRuntime, type WorkflowCompletionEvent } from "workerflow";

export class OrderWorkflowRuntime extends WorkflowRuntime<{ orderId: string }> {
  protected readonly definition = this.ctx.exports.OrderWorkflowDefinition;

  protected async completion(event: WorkflowCompletionEvent): Promise<void> {
    // Completion delivery is at least once. Use event.id as the idempotency key
    // when updating another database or calling an external API.
    console.log("Order workflow finished", event);
  }
}

export class OrderWorkflowDefinition extends WorkflowDefinition<{ orderId: string }> {
  async execute(): Promise<void> {
    const { orderId } = this.ctx.props.input;

    await this.run("reserve-inventory", async () => {
      // Durable: replay returns the stored result without re-running the callback.
      return { orderId, reserved: true };
    });

    await this.sleep("payment-window", 60_000);

    const payment = await this.wait<{ chargeId: string }>("capture-payment", "payment.received", {
      timeoutAt: Date.now() + 86_400_000
    });

    await this.run("fulfill", async () => {
      return { orderId, chargeId: payment.chargeId };
    });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/orders") {
      const orderId = "new-order";
      const stub = env.ORDER_WORKFLOW.getByName(orderId);
      await stub.create({ orderId });
      return Response.json({ id: orderId });
    }

    return new Response(null, { status: 404 });
  }
} satisfies ExportedHandler<Env>;
```

Workflow input is **`this.ctx.props.input`**, populated from **`create(input)`**. TypeScript requires an input argument when your runtime's **`TInput`** excludes **`undefined`**; no-input workflows can use **`WorkflowRuntime<undefined>`**, and optionally-input workflows can include **`undefined`** in the input type. The runtime also sets **`this.ctx.props.requestId`** (a new UUID each time the run loop invokes your definition) and **`this.ctx.props.runtimeInstanceId`** (this Durable Object’s id) for logs and correlation.

### Runtime control

From the Durable Object stub you can:

- **`create(input)`** — Pins the workflow input in SQLite the **first** time the instance is initialized, then starts execution. The input argument is required unless **`TInput`** includes **`undefined`**. **No-op** if the workflow is already **completed**, **failed**, **cancelled**, or **paused**.
- **`pause()`** — When status is **running**, moves to **paused**, clears alarms, and stops driving **`execute()`** until **`resume()`**. Inbound events are queued and applied when a matching **`wait`** runs again after resume.
- **`resume()`** — When status is **paused**, moves to **running** and continues the loop. Throws if the workflow is not paused.
- **`cancel(reason?)`** — Moves to terminal **cancelled** and stops workflow execution. If the runtime defines **`completion`**, it schedules an alarm for durable completion delivery; otherwise it clears the current alarm.

New instances start in **`pending`**. The first **`create()`** call moves the instance through the durable **`initialized`** state before execution enters **`running`**.

### Completion handler

Define the optional protected **`completion(event)`** method on the runtime to consume terminal workflow outcomes:

```ts
type WorkflowCompletionEvent = {
  id: string;
  status: "completed" | "failed" | "cancelled";
  finishedAt: Date;
};
```

Defining the method opts that runtime into completion delivery; it is a runtime consumer, not a method clients call through the Durable Object stub. The runtime durably records a pending delivery in the same transaction as the terminal status, then invokes the consumer. Returning acknowledges the event; throwing records the error and schedules another attempt with exponential backoff.

Delivery is **at least once**: the same event can be delivered again if the Durable Object stops after the consumer's side effect succeeds but before the acknowledgement is recorded. **`event.id`** is stable across attempts and should be used as an idempotency key. Failed attempts continue to be retried until the consumer returns successfully, without changing the workflow's terminal status. Runtimes that do not define **`completion`** do not create delivery records or schedule delivery alarms.

### Experimental introspection

For dashboards and debugging, the runtime exposes **`getSteps_experimental()`** and **`getWorkflowEvents_experimental()`**. These names are marked experimental because they may change as the API hardens.

## How it works

The library separates concerns into two main layers:

- **Runtime Layer**: Managed by the `WorkflowRuntime` Durable Object, this layer is responsible for orchestration and state persistence. It maintains all workflow metadata, tracks which steps have completed, what the workflow is currently waiting on, and stores any inbound events that may arrive while the workflow is paused. The runtime ensures workflow continuity across multiple invocations and restarts by persisting its state in SQLite via Durable Object storage.

- **Definition Layer**: This is where you author your workflow logic by subclassing `WorkflowDefinition` and implementing the `execute()` method. Here you describe, in a sequence of durable and replayable operations, how each step in your workflow should proceed. You use helpers like `run` (to perform an idempotent unit of work), `sleep` (to pause execution for a specific duration), and `wait` (to suspend progress until an inbound event or timeout). Each time the runtime's loop advances, your full `execute()` method is replayed deterministically, and the workflow engine ensures side effects are only performed when workflow state transitions allow.

### Replay and side effects

Each time the runtime advances, it calls `next()` on your `WorkflowDefinition`, which **runs `execute()` from the beginning again**. Steps that have already completed durably (`run`, elapsed `sleep`, resolved `wait`, and so on) **replay from stored state**: their callbacks are not re-invoked, and recorded results are returned as-is. New side effects happen only when the engine reaches a step that is not yet complete and the durable state allows that transition.

**Step ids must be unique** within one top-level **`execute()`** run (the same **`next()`** invocation): reuse the same id across **`run`**, **`sleep`**, or **`wait`** and the workflow fails fast.

**Sibling `run` calls.** At a given nesting level, after one **`run`** finishes successfully in the same **`next()`**, the next sibling **`run`** forces the runtime to **run the loop again immediately** (you still replay from the top; completed steps stay cached). For linear workflows this is invisible; if you place several **`run`** calls back-to-back at the same depth, expect an extra loop hop per step after the first. Nested **`run`** callbacks get a fresh frame, so children do not consume the parent’s sibling budget.

### When the loop runs and when it stops

The `WorkflowRuntime` Durable Object drives a **run loop** that repeatedly invokes `next()` until one of these happens:

- **Terminal**: `next()` reports the workflow is **done** (`completed` or `failed`), or the instance is **`cancelled`** via **`cancel()`** while the loop is idle or between iterations. The loop exits and the watchdog alarm is cleared. A workflow with a completion handler uses the alarm for durable terminal-outcome delivery until the handler acknowledges the event.
- **Immediate resume**: `next()` asks to **continue immediately** (for example, so another step in the same logical “tick” can run). The loop continues without leaving the Durable Object invocation.
- **Suspended**: `next()` asks to **suspend**—for example, a step is waiting on a **retry backoff**, a **sleep** until a future time, or a **wait** for an inbound event. The loop exits; the runtime relies on **alarms** and/or **incoming events** to call back into the run loop. A long **watchdog alarm** also exists as a safety net if progress stalls.

### Step kinds

- **`run`**: A named, durable unit of work. Callbacks return JSON-serializable values or `undefined`. Outcomes are persisted; failures can be **retried** with backoff up to **`maxAttempts`** (default **3** attempts per step unless you pass `{ maxAttempts: n }`).
- **`sleep`**: Pauses until a **scheduled wake time** stored in SQLite; the Durable Object is woken by an **alarm** when that time is reached.
- **`wait`**: Pauses until a matching **inbound event** (by name) or an optional **timeout**. Resolution is recorded in durable state so replay does not double-apply the branch that handled the event.

### Alarms

Alarms are the primary mechanism for waking the `WorkflowRuntime` Durable Object back up after it suspends. There are three kinds of precise alarm tied to steps, a completion-delivery alarm, and a long-running watchdog that acts as a safety net.

**Sleep wake-up.** When `execute()` calls `this.sleep("id", duration)`, the runtime records a `sleep` step in SQLite with a `wake_at` timestamp and immediately schedules an alarm for that exact moment. When the alarm fires, the run loop replays `execute()` from the top, reaches the sleep step, sees the wake time has passed, marks the step `elapsed`, and continues forward.

```ts
async execute(): Promise<void> {
  await this.run("charge", async () => { /* ... */ });

  // Schedules a Durable Object alarm 24 hours from now.
  // The DO hibernates; no CPU is consumed until the alarm fires.
  await this.sleep("cooling-off-period", 24 * 60 * 60 * 1_000);

  await this.run("ship", async () => { /* ... */ });
}
```

**Retry backoff.** When a `run` step fails but has attempts remaining, the runtime computes an exponential backoff delay (`250 ms → 500 ms → 1 s → 2 s → 4 s → 8 s → 10 s`), records `next_attempt_at` in SQLite, and schedules an alarm for that time. The DO goes idle; the run loop resumes only when the alarm fires.

```ts
await this.run(
  "call-payment-api",
  async () => {
    const res = await fetch("https://payments.example.com/charge", { method: "POST" });
    if (!res.ok) throw new Error(`Payment failed: ${res.status}`);
    return res.json();
  },
  { maxAttempts: 5 } // retries up to 4 more times with exponential backoff
);
```

**Wait timeout.** When `this.wait` is called with a `timeoutAt`, the runtime schedules an alarm for that deadline. If no matching inbound event has arrived by then, the alarm fires, the step transitions to `timed_out`, and execution continues past the wait.

```ts
// Suspend until "payment.received" is delivered or 24 hours elapse.
const payment = await this.wait<{ chargeId: string }>("capture-payment", "payment.received", {
  timeoutAt: Date.now() + 86_400_000
});
```

**Completion delivery.** When a workflow completes, fails, or is cancelled and its runtime defines **`completion`**, the pending delivery is stored before an immediate alarm is scheduled. Before invoking the handler, the runtime moves that alarm forward as a visibility timeout. A rejected handler is retried with exponential backoff; if the runtime stops while the handler is running, the visibility timeout makes the event eligible for redelivery.

#### The watchdog alarm

In addition to these precise alarms, the runtime sets a **30-minute watchdog alarm at the start of every run-loop iteration**, before delegating to the workflow definition. When an iteration ends cleanly—workflow terminal completion, suspend with a known **`wakeAt`**, or suspend waiting only on inbound events—the alarm is **cleared** or **replaced** by the next wake time when there is one. A **`wait`** with **no** `timeoutAt` has no step-specific alarm until an event arrives; the watchdog remains the backstop. The watchdog only fires if something goes wrong in the middle.

The problem it guards against is a `run` step that gets stuck in the `running` state. Before the user's callback executes, the runtime durably writes `state = 'running'` to SQLite. That write is intentional: it ensures that a later replay does not try to start a second concurrent attempt for the same step. But it creates a gap:

```
1. Runtime writes state = 'running' to SQLite.   ← durable
2. User's callback starts executing.
3. Durable Object is evicted or crashes.          ← no outcome recorded
4. SQLite still shows state = 'running'.          ← step is stuck
```

At this point there is no sleep alarm, no retry alarm, and no wait-timeout alarm; nothing scheduled to wake the runtime back up. Without the watchdog the workflow would stall indefinitely. The watchdog fires 30 minutes later, calls back into the run loop, replays `execute()`, reaches the stuck step, re-runs the callback, and records a proper outcome.

There is also a guard for the case where an alarm fires while the run loop is already active — for example, a sleep's precise alarm arriving while the loop is processing another step in the same Durable Object invocation. In that situation the alarm handler simply reschedules the watchdog for another 30 minutes rather than starting a second concurrent loop, keeping the safety net in place until the active loop finishes.

## Why this exists

Cloudflare Workflows is a strong managed option, and for many use cases it is the right tradeoff. I built `workerflow` for cases where I wanted tighter control over runtime behavior, replay semantics, and state projection than the managed model naturally gives me.

1. Explicit ownership of workflow state and lifecycle
2. Durable replay semantics that are explicit in userland code
3. Separation between workflow execution and external state synchronization
4. Extension points for streaming, WebSockets, and custom lifecycle consumers
5. Fewer surprises around long-lived execution and error handling

### Definition compatibility

One of the biggest concerns in long-running workflows is definition drift. A normal Worker request is typically bound to a single in-flight execution on one deployed version, but a Workflow is durable: it persists state and resumes across multiple executions over time. A workflow may start on one version of its definition and resume later after a deploy has changed or removed a step. That means the next invocation of the workflow entry point could repeat steps unsafely or leave the runtime in an invalid state.

`workerflow` keeps definition selection simple: each runtime points at one definition entrypoint, and the input is the only per-instance payload pinned by `create(input)`. If a workflow needs version-aware behavior, model that explicitly in your input shape and keep old branches compatible until the long-lived instances that need them have completed.

### Keeping workflow execution separate from state projection

In most real applications, workflows do not live in isolation. You usually have an external database that you want to keep in sync with workflow state so your application can query status, render UI, or trigger related behavior. It is tempting to model that synchronization as a final workflow step:

```ts
export class OrderWorkflowDefinition extends WorkflowDefinition<{ orderId: string }> {
  async execute(): Promise<void> {
    await this.run("fulfill-order", async () => {
      // Perform the workflow's business operation.
    });

    await this.run("project-completed-status", async () => {
      // Update an external database.
    });
  }
}
```

This looks reasonable at first, but it creates an important failure-mode problem. If the business operation succeeds but projection exhausts its retries, projection failure can affect the workflow's outcome even though these are not necessarily the same concern.

I think a cleaner design for terminal projection is to keep synchronization out of the definition and implement **`completion`** on the runtime instead:

```ts
export class OrderWorkflowRuntime extends WorkflowRuntime<{ orderId: string }> {
  protected readonly definition = this.ctx.exports.OrderWorkflowDefinition;

  protected async completion(event: WorkflowCompletionEvent): Promise<void> {
    // Project the terminal workflow status; use event.id as an idempotency key because this may be retried.
  }
}
```

The terminal status and pending delivery are recorded together. If projection fails, the runtime retries it independently without retroactively redefining the workflow's business outcome. Because delivery is at least once, the projection should make repeated calls with the same **`event.id`** safe—for example, by storing it in a column with a unique constraint.

The **`completion`** API only covers terminal outcomes. Applications that need live, non-terminal projection can poll the experimental introspection APIs or implement another runtime extension. A scheduled reconciliation job can also be useful as an independent audit and repair mechanism alongside completion delivery.

That is not the only valid approach, but I think it produces a better separation of concerns: the workflow runtime determines workflow outcome, and projection mechanisms consume that outcome.

### Error handling

Another friction point in Cloudflare Workflows is error handling. My understanding, based on using it in production and reading the [announcement materials](https://blog.cloudflare.com/building-workflows-durable-execution-on-workers/), is that the workflow runtime creates a step context and passes it into the Worker entry point. That step context is the step object you call methods like `do` and `sleep` on.

The `do` method is effectively an RPC call that accepts a step name, a callback, and optional configuration. It is invoked from the workflow entry point, but it runs inside the Worker where it was created. Since functions can be passed over RPC through stubs, the result is a chain of calls that crosses boundaries multiple times: the workflow engine Durable Object calls the Worker entry point, the Worker calls back into the Durable Object to update step state, and the Durable Object may then call back into the Worker again. Some of this is unavoidable, but it does have an unfortunate consequence: if you catch an error outside `step.do`, it is not necessarily the same error instance that was originally thrown inside the step, because it had to cross an RPC boundary. That might sound like an implementation detail, but in practice it affects how errors can be classified, rethrown, or inspected.

## Tradeoffs

Owning the runtime buys flexibility, but it also means giving up some of the benefits of a managed workflow product. The most obvious trade-off is the cost model. Cloudflare Workflows is priced like Workers Standard pricing: you are billed for workflow invocations, CPU time, and storage, and idle periods, such as waiting on an API response, do not consume CPU billing. Durable Objects have a different cost model. They are billed for requests, storage, and compute duration measured as wall-clock time while the object is active or idle in memory but unable to hibernate.
You also give up a fair amount of first-party tooling. Cloudflare Workflows comes with built-in observability and debugging, dashboard metrics, and a visualizer that can render your workflow definition as a diagram directly in the dashboard. It is possible to recreate in user land; in fact, a custom implementation could build a more application-specific control plane, but now you are responsible for building and maintaining it yourself.
