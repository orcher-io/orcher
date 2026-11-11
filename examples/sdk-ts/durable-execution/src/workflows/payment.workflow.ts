import { Workflow, WorkflowContext } from '@orcher/sdk';

export interface PaymentWorkflowInput {
  orderId: string;
  customerId: string;
  amount: number;
  cardToken: string;
  customerEmail: string;
}

export interface PaymentWorkflowOutput {
  orderId: string;
  chargeId: string;
  status: 'completed' | 'failed';
  notificationSent: boolean;
  error?: string;
}

@Workflow({ name: 'paymentWorkflow', version: '1.0' })
export class PaymentWorkflow {
  async run(
    ctx: WorkflowContext,
    input: PaymentWorkflowInput
  ): Promise<PaymentWorkflowOutput> {
    let chargeResult: any;
    try {
      chargeResult = await ctx.execute('chargeCard', async () => {
        return { chargeId: `ch_${Date.now()}`, amount: input.amount, status: 'succeeded' };
      });
    } catch (error) {
      return {
        orderId: input.orderId,
        chargeId: '',
        status: 'failed',
        notificationSent: false,
        error: (error as Error).message || 'Payment processing failed',
      };
    }

    return {
      orderId: input.orderId,
      chargeId: chargeResult.chargeId,
      status: 'completed',
      notificationSent: true,
    };
  }
}
