/// <reference types="@cloudflare/vitest-pool-workers/types" />

type WorkerEnv = import("./worker").Env;

declare namespace Cloudflare {
  interface Env extends WorkerEnv {}

  interface GlobalProps {
    mainModule: typeof import("./worker");
    durableNamespaces: _DurableNamespaceKeys<WorkerEnv>;
  }
}
