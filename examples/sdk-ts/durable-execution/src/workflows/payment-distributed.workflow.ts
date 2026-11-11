import { Workflow, WorkflowContext, WorkflowError } from "@orcher/sdk";
import {
  distributedTasks,
  ChargeCardDistributedInput,
  ReserveInventoryDistributedInput,
  SendEmailDistributedInput,
} from "../tasks/distributed.tasks";

/**
 * Payment Workflow with Distributed Tasks
 *
 * This workflow mirrors the Rust SDK's payment_workflow structure using
 * ctx.executeTask() for TRUE distributed task execution:
 *
 * 1. chargeCardDistributed - Process payment via Stripe (distributed task)
 * 2. reserveInventoryDistributed - Reserve inventory items (distributed task)
 * 3. sendEmailDistributed - Send confirmation notification (distributed task)
 *
 * Each step uses ctx.executeTask() which:
 * - Generates a SCHEDULE_TASK command
 * - Suspends the workflow
 * - Task is executed by a task poller
 * - Workflow resumes with the result
 *
 * This matches the Rust SDK's ctx.execute_task() behavior for a fair
 * performance comparison between the two SDKs.
 */

export interface PaymentDistributedInput {
  orderId: string;
  amount: number;
  cardToken: string;
  customerEmail: string;
}

export interface PaymentDistributedOutput {
  orderId: string;
  chargeId: string;
  reservationId: string;
  notificationId: string;
  status: "completed" | "failed";
  apiCalls: {
    stripeCount: number;
    inventoryCount: number;
    emailCount: number;
  };
  error?: string;
}

@Workflow({ name: "paymentDistributedWorkflow2", version: "1.0" })
export class PaymentDistributedWorkflow {
  async run(
    ctx: WorkflowContext,
    input: PaymentDistributedInput,
  ): Promise<PaymentDistributedOutput> {
    console.log(
      `🚀 Starting distributed payment workflow for order: ${input.orderId}`,
    );

    // executeTask() uses distributed execution with full DI support (matching Rust SDK)

    let chargeId: string = "";
    let reservationId: string = "";
    let notificationId: string = "";
    let stripeCount = 0;
    let inventoryCount = 0;
    let emailCount = 0;

    try {
      // Step 1: Charge credit card via distributed task
      console.log("💳 Step 1: Scheduling chargeCardDistributed task...");
      const chargeInput: ChargeCardDistributedInput = {
        amount: input.amount,
        cardToken: input.cardToken,
        orderId: input.orderId,
      };

      const chargeResult = await ctx.executeTask(
        distributedTasks.chargeCardDistributed,
        chargeInput,
      );

      chargeId = chargeResult.chargeId;
      stripeCount = 1;
      console.log(`✅ Step 1 complete: Charge ID = ${chargeId}`);

      // Step 2: Reserve inventory via distributed task
      console.log("📦 Step 2: Scheduling reserveInventoryDistributed task...");
      const inventoryInput: ReserveInventoryDistributedInput = {
        orderId: input.orderId,
      };

      const inventoryResult = await ctx.executeTask(
        distributedTasks.reserveInventoryDistributed,
        inventoryInput,
      );

      reservationId = inventoryResult.reservationId;
      inventoryCount = 1;
      console.log(`✅ Step 2 complete: Reservation ID = ${reservationId}`);

      // Step 3: Send confirmation email via distributed task
      console.log("📧 Step 3: Scheduling sendEmailDistributed task...");
      const emailInput: SendEmailDistributedInput = {
        email: input.customerEmail,
        chargeId: chargeId,
        orderId: input.orderId,
      };

      const emailResult = await ctx.executeTask(
        distributedTasks.sendEmailDistributed,
        emailInput,
      );

      notificationId = emailResult.notificationId;
      emailCount = 1;
      console.log(`✅ Step 3 complete: Notification ID = ${notificationId}`);

      // Collect stats
      const apiCalls = {
        stripeCount,
        inventoryCount,
        emailCount,
      };

      console.log("🎉 Distributed payment workflow completed successfully");
      console.log(
        `📊 API Call Stats: Stripe=${apiCalls.stripeCount}, Inventory=${apiCalls.inventoryCount}, Email=${apiCalls.emailCount}`,
      );

      return {
        orderId: input.orderId,
        chargeId,
        reservationId,
        notificationId,
        status: "completed",
        apiCalls,
      };
    } catch (error) {
      // Re-throw WorkflowError (including suspension errors) - these are control flow signals
      // that must propagate to the executor, not be caught by the workflow
      if (error instanceof WorkflowError) {
        throw error;
      }

      console.error("❌ Distributed payment workflow failed:", error);
      return {
        orderId: input.orderId,
        chargeId,
        reservationId,
        notificationId,
        status: "failed",
        apiCalls: {
          stripeCount,
          inventoryCount,
          emailCount,
        },
        error: (error as Error).message || "Unknown error",
      };
    }
  }
}
