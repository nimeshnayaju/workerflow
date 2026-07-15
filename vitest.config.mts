import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: "test/worker.ts",
      miniflare: {
        compatibilityDate: "2026-01-28",
        compatibilityFlags: [
          "nodejs_compat",
          "enable_nodejs_tty_module",
          "enable_nodejs_fs_module",
          "enable_nodejs_http_modules",
          "enable_nodejs_perf_hooks_module",
          "enable_nodejs_v8_module",
          "enable_nodejs_process_v2"
        ],
        durableObjects: {
          TEST_WORKFLOW_RUNTIME: {
            className: "TestWorkflowRuntime",
            useSQLite: true
          },
          TEST_COMPLETION_WORKFLOW_RUNTIME: {
            className: "TestCompletionWorkflowRuntime",
            useSQLite: true
          }
        }
      }
    })
  ]
});
