import { Workflow, WorkflowContext } from '@orcher/sdk';

@Workflow({ name: 'simpleWorkflow', version: '1.0' })
export class SimpleWorkflow {
  async run(_ctx: WorkflowContext, input: any): Promise<any> {
    // Simple workflow that just returns a result without tasks
    return {
      message: 'Workflow completed successfully',
      input: input,
      timestamp: new Date().toISOString(),
    };
  }
}
