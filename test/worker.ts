import { WorkflowDefinition } from "../src/definition";
import { WorkflowRuntime, type WorkflowCompletionEvent, type WorkflowStatus } from "../src/runtime";

export type Env = {
  TEST_WORKFLOW_RUNTIME: DurableObjectNamespace<TestWorkflowRuntime>;
  TEST_COMPLETION_WORKFLOW_RUNTIME: DurableObjectNamespace<TestCompletionWorkflowRuntime>;
};

export class TestWorkflowRuntime extends WorkflowRuntime {
  protected readonly definition = this.ctx.exports.TestWorkflowDefinition;

  public override onStatusChange?: (status: Exclude<WorkflowStatus, "pending" | "initialized">) => void;
}

export class TestCompletionWorkflowRuntime extends WorkflowRuntime {
  protected readonly definition = this.ctx.exports.TestWorkflowDefinition;

  public override onStatusChange?: (status: Exclude<WorkflowStatus, "pending" | "initialized">) => void;

  public override async completion(_event: WorkflowCompletionEvent): Promise<void> {}
}
export class TestWorkflowDefinition extends WorkflowDefinition {
  async execute(): Promise<void> {}
}

export default {
  fetch() {
    return new Response(null);
  }
} satisfies ExportedHandler;
