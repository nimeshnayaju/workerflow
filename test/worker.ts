import { WorkflowDefinition } from "../src/definition";
import { WorkflowRuntime } from "../src/runtime";

export type Env = {
  TEST_WORKFLOW_RUNTIME: DurableObjectNamespace<TestWorkflowRuntime>;
};

export class TestWorkflowRuntime extends WorkflowRuntime {
  protected readonly definition = this.ctx.exports.TestWorkflowDefinition;
}
export class TestWorkflowDefinition extends WorkflowDefinition {
  async execute(): Promise<void> {}
}

export default {
  fetch() {
    return new Response(null);
  }
} satisfies ExportedHandler;
