import { Tasks, Task, TaskContext, createTaskRefs } from "@orcher/sdk";
import { StripeService } from "../services/stripe.service";
import { EmailService } from "../services/email.service";
import { LoggerService } from "../services/logger.service";

/**
 * Payment task input interfaces
 */
export interface ChargeCardInput {
  amount: number;
  token: string;
  orderId: string;
  customerEmail: string;
  idempotencyKey?: string;
}

export interface RefundChargeInput {
  chargeId: string;
  orderId: string;
  amount?: number;
  customerEmail: string;
  reason: string;
}

export interface SendPaymentNotificationInput {
  type: "success" | "failure" | "refund";
  email: string;
  orderId: string;
  chargeId?: string;
  refundId?: string;
  amount?: number;
  reason?: string;
}

/**
 * PaymentTasks
 *
 * Task handler class for payment-related operations.
 * Demonstrates dependency injection in task handlers.
 *
 * Key DI concepts demonstrated:
 * - Constructor injection of services (Stripe, Email, Logger)
 * - Tasks are where side effects happen (payment processing, emails)
 * - Each task method is decorated with @Task() to create a TaskReference
 * - Use createTaskRefs() to get type-safe task references for workflows
 *
 * Architecture notes:
 * - Tasks CAN use DI (unlike workflows which cannot)
 * - Services are injected once when the task handler is instantiated
 * - Task methods are called for each task execution
 * - Task timeout, retry, and heartbeat are configured via @Task() decorator
 */
@Tasks()
export class PaymentTasks {
  constructor(
    private stripe: StripeService,
    private email: EmailService,
    private logger: LoggerService,
  ) {
    this.logger.info("PaymentTasks initialized with DI");
  }

  /**
   * Charge a credit card
   *
   * This task demonstrates:
   * - Using injected StripeService to process payments
   * - Proper error handling and logging
   * - Idempotency support for safe retries
   * - Type-safe input/output via createTaskRefs()
   *
   * Usage in workflows:
   *   const result = await ctx.executeTask(paymentTasks.chargeCard, input);
   */
  @Task({ name: "chargeCard", timeout: 30000 })
  async chargeCard(
    ctx: TaskContext,
    input: ChargeCardInput,
  ): Promise<{ chargeId: string; amount: number; status: string }> {
    this.logger.info("Executing chargeCard task", {
      taskId: (ctx as any).taskId || "default",
      orderId: input.orderId,
      amount: input.amount,
    });

    try {
      // Use injected StripeService to process payment
      const charge = await this.stripe.charge(
        input.amount,
        input.token,
        input.idempotencyKey || (ctx as any).taskId || "default", // Use taskId as idempotency key by default
      );

      if (charge.status !== "succeeded") {
        throw new Error(
          charge.failureMessage ||
            `Payment failed with status: ${charge.status}`,
        );
      }

      this.logger.info("Card charged successfully", {
        taskId: (ctx as any).taskId || "default",
        chargeId: charge.id,
        amount: charge.amount,
      });

      return {
        chargeId: charge.id,
        amount: charge.amount,
        status: charge.status,
      };
    } catch (error) {
      this.logger.error("Failed to charge card", error as Error, {
        taskId: (ctx as any).taskId || "default",
        orderId: input.orderId,
      });
      throw error;
    }
  }

  /**
   * Refund a charge
   *
   * This task demonstrates:
   * - Refund processing via injected StripeService
   * - Partial and full refund support
   * - Error handling for failed refunds
   */
  @Task({ name: "refundCharge", timeout: 30000 })
  async refundCharge(
    ctx: TaskContext,
    input: RefundChargeInput,
  ): Promise<{ refundId: string; amount: number; status: string }> {
    this.logger.info("Executing refundCharge task", {
      taskId: (ctx as any).taskId || "default",
      chargeId: input.chargeId,
      orderId: input.orderId,
      amount: input.amount || "full refund",
    });

    try {
      // Use injected StripeService to process refund
      const refund = await this.stripe.refund(input.chargeId, input.amount);

      if (refund.status !== "succeeded") {
        throw new Error(`Refund failed with status: ${refund.status}`);
      }

      this.logger.info("Charge refunded successfully", {
        taskId: (ctx as any).taskId || "default",
        refundId: refund.id,
        chargeId: input.chargeId,
        amount: refund.amount,
      });

      return {
        refundId: refund.id,
        amount: refund.amount,
        status: refund.status,
      };
    } catch (error) {
      this.logger.error("Failed to refund charge", error as Error, {
        taskId: (ctx as any).taskId || "default",
        chargeId: input.chargeId,
      });
      throw error;
    }
  }

  /**
   * Send payment notification email
   *
   * This task demonstrates:
   * - Using injected EmailService to send notifications
   * - Handling different notification types (success, failure, refund)
   * - Non-blocking email delivery
   */
  @Task({ name: "sendPaymentNotification", timeout: 20000 })
  async sendPaymentNotification(
    ctx: TaskContext,
    input: SendPaymentNotificationInput,
  ): Promise<{ messageId: string; sent: boolean }> {
    this.logger.info("Executing sendPaymentNotification task", {
      taskId: (ctx as any).taskId || "default",
      type: input.type,
      email: input.email,
      orderId: input.orderId,
    });

    try {
      let result;

      switch (input.type) {
        case "success":
          if (!input.chargeId || input.amount === undefined) {
            throw new Error(
              "chargeId and amount required for success notification",
            );
          }
          result = await this.email.sendPaymentSuccess(
            input.email,
            input.orderId,
            input.chargeId,
            input.amount,
          );
          break;

        case "failure":
          if (!input.reason) {
            throw new Error("reason required for failure notification");
          }
          result = await this.email.sendPaymentFailure(
            input.email,
            input.orderId,
            input.reason,
          );
          break;

        case "refund":
          if (!input.refundId || input.amount === undefined) {
            throw new Error(
              "refundId and amount required for refund notification",
            );
          }
          result = await this.email.sendRefundNotification(
            input.email,
            input.orderId,
            input.refundId,
            input.amount,
          );
          break;

        default:
          throw new Error(`Unknown notification type: ${input.type}`);
      }

      if (!result.success) {
        throw new Error(result.error || "Failed to send email");
      }

      this.logger.info("Payment notification sent successfully", {
        taskId: (ctx as any).taskId || "default",
        messageId: result.messageId,
        type: input.type,
      });

      return {
        messageId: result.messageId,
        sent: true,
      };
    } catch (error) {
      this.logger.error("Failed to send payment notification", error as Error, {
        taskId: (ctx as any).taskId || "default",
        type: input.type,
        email: input.email,
      });
      throw error;
    }
  }

  /**
   * Verify charge status
   *
   * This task demonstrates:
   * - Polling/checking external state
   * - Using injected services for read operations
   * - Returning status information for workflow decisions
   */
  @Task({ name: "verifyChargeStatus", timeout: 15000 })
  async verifyChargeStatus(
    ctx: TaskContext,
    input: { chargeId: string },
  ): Promise<{ chargeId: string; status: string; verified: boolean }> {
    this.logger.info("Executing verifyChargeStatus task", {
      taskId: (ctx as any).taskId || "default",
      chargeId: input.chargeId,
    });

    try {
      // Use injected StripeService to fetch charge details
      const charge = await this.stripe.getCharge(input.chargeId);

      if (!charge) {
        throw new Error(`Charge not found: ${input.chargeId}`);
      }

      const verified = charge.status === "succeeded";

      this.logger.info("Charge status verified", {
        taskId: (ctx as any).taskId || "default",
        chargeId: input.chargeId,
        status: charge.status,
        verified,
      });

      return {
        chargeId: input.chargeId,
        status: charge.status,
        verified,
      };
    } catch (error) {
      this.logger.error("Failed to verify charge status", error as Error, {
        taskId: (ctx as any).taskId || "default",
        chargeId: input.chargeId,
      });
      throw error;
    }
  }
}

/**
 * Type-safe task references for PaymentTasks
 *
 * Usage in workflows:
 *   import { paymentTasks } from '../tasks/payment.tasks';
 *   const result = await ctx.executeTask(paymentTasks.chargeCard, input);
 *
 * TypeScript automatically infers input/output types for each task.
 */
export const paymentTasks = createTaskRefs(PaymentTasks);
