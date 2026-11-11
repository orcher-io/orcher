import { Tasks, Task, TaskContext, createTaskRefs } from "@orcher/sdk";
import { StripeService } from "../services/stripe.service";
import { EmailService } from "../services/email.service";
import { LoggerService } from "../services/logger.service";

/**
 * Distributed Tasks
 *
 * These tasks are designed for true distributed execution via ctx.executeTask().
 *
 * With the updated SDK, distributed tasks now have FULL DI SUPPORT:
 *
 * 1. Constructor injection works - dependencies are resolved by the TaskExecutor
 * 2. Tasks are scheduled via SCHEDULE_TASK commands (always distributed)
 * 3. Task workers/pollers execute tasks with DI container resolution
 * 4. This mirrors the Rust SDK's ctx.execute_task() behavior
 *
 * **Type-Safe Task References:**
 * Use `createTaskRefs(ClassName)` to get a typed object with TaskReferences
 * for all @Task methods. Full input/output type inference!
 *
 * Each task simulates 100ms latency to match the Rust SDK examples.
 */

// API call counters for idempotency verification
let stripeApiCalls = 0;
let inventoryApiCalls = 0;
let emailApiCalls = 0;

export function resetDistributedApiCounters(): void {
  stripeApiCalls = 0;
  inventoryApiCalls = 0;
  emailApiCalls = 0;
}

export function getDistributedApiCounters(): {
  stripe: number;
  inventory: number;
  email: number;
} {
  return {
    stripe: stripeApiCalls,
    inventory: inventoryApiCalls,
    email: emailApiCalls,
  };
}

/**
 * Input for chargeCardDistributed task
 */
export interface ChargeCardDistributedInput {
  amount: number;
  cardToken: string;
  orderId: string;
}

/**
 * Output from chargeCardDistributed task
 */
export interface ChargeCardDistributedOutput {
  chargeId: string;
  amount: number;
  status: string;
}

/**
 * Input for reserveInventoryDistributed task
 */
export interface ReserveInventoryDistributedInput {
  orderId: string;
  items?: string[];
}

/**
 * Output from reserveInventoryDistributed task
 */
export interface ReserveInventoryDistributedOutput {
  reservationId: string;
  reserved: boolean;
}

/**
 * Input for sendEmailDistributed task
 */
export interface SendEmailDistributedInput {
  email: string;
  chargeId: string;
  orderId: string;
}

/**
 * Output from sendEmailDistributed task
 */
export interface SendEmailDistributedOutput {
  notificationId: string;
  sent: boolean;
}

/**
 * DistributedTasks
 *
 * Task handler class for distributed task execution WITH DI SUPPORT.
 *
 * These tasks use constructor injection just like any other task class.
 * When the task is picked up by a worker/poller, the TaskExecutor:
 * 1. Looks up the task in the global registry
 * 2. Resolves the handler class from the DI container
 * 3. Calls the task method with injected dependencies
 */
@Tasks()
export class DistributedTasks {
  /**
   * Constructor with DI - dependencies are injected by the TaskExecutor
   * when this task runs on a worker/poller.
   */
  constructor(
    private readonly stripeService: StripeService,
    private readonly emailService: EmailService,
    private readonly logger: LoggerService,
  ) {
    this.logger.debug("DistributedTasks initialized with DI");
  }

  /**
   * Charge credit card via Stripe API
   *
   * Uses injected StripeService for actual payment processing.
   * Increments stripeApiCalls counter for idempotency verification.
   */
  @Task({ name: "chargeCardDistributed", timeout: 30000 })
  async chargeCardDistributed(
    _ctx: TaskContext,
    input: ChargeCardDistributedInput,
  ): Promise<ChargeCardDistributedOutput> {
    this.logger.info("[TASK] Charging card for order", {
      orderId: input.orderId,
      amount: input.amount,
    });

    // Track API call for idempotency verification
    stripeApiCalls++;

    // Use injected StripeService (DI works!)
    const chargeResult = await this.stripeService.charge(
      input.amount,
      input.cardToken,
      `order_${input.orderId}`, // idempotency key
    );

    this.logger.info("[TASK] Charge complete", {
      chargeId: chargeResult.id,
      status: chargeResult.status,
    });

    return {
      chargeId: chargeResult.id,
      amount: chargeResult.amount,
      status: chargeResult.status,
    };
  }

  /**
   * Reserve inventory
   *
   * Simulates a 100ms database operation to match Rust SDK timing.
   * Increments inventoryApiCalls counter for idempotency verification.
   */
  @Task({ name: "reserveInventoryDistributed", timeout: 30000 })
  async reserveInventoryDistributed(
    _ctx: TaskContext,
    input: ReserveInventoryDistributedInput,
  ): Promise<ReserveInventoryDistributedOutput> {
    this.logger.info("[TASK] Reserving inventory for order", {
      orderId: input.orderId,
      items: input.items,
    });

    // Track API call for idempotency verification
    inventoryApiCalls++;

    // Simulate database latency (100ms like Rust SDK)
    await new Promise((resolve) => setTimeout(resolve, 100));

    const reservationId = `rsv_${Date.now().toString(36)}`;

    this.logger.info("[TASK] Reservation complete", { reservationId });

    return {
      reservationId,
      reserved: true,
    };
  }

  /**
   * Send confirmation email
   *
   * Uses injected EmailService for actual email sending.
   * Increments emailApiCalls counter for idempotency verification.
   */
  @Task({ name: "sendEmailDistributed", timeout: 30000 })
  async sendEmailDistributed(
    _ctx: TaskContext,
    input: SendEmailDistributedInput,
  ): Promise<SendEmailDistributedOutput> {
    this.logger.info("[TASK] Sending email", {
      email: input.email,
      orderId: input.orderId,
    });

    // Track API call for idempotency verification
    emailApiCalls++;

    // Use injected EmailService (DI works!)
    const emailResult = await this.emailService.sendPaymentSuccess(
      input.email,
      input.orderId,
      input.chargeId,
      0, // amount would come from order in real scenario
    );

    this.logger.info("[TASK] Email sent", {
      notificationId: emailResult.messageId,
      success: emailResult.success,
    });

    return {
      notificationId: emailResult.messageId,
      sent: emailResult.success,
    };
  }
}

/**
 * Type-safe task references for DistributedTasks
 *
 * Usage in workflows:
 *   await ctx.executeTask(distributedTasks.chargeCardDistributed, input);
 *
 * TypeScript automatically infers:
 *   - chargeCardDistributed: TaskReference<ChargeCardDistributedInput, ChargeCardDistributedOutput>
 *   - reserveInventoryDistributed: TaskReference<ReserveInventoryDistributedInput, ReserveInventoryDistributedOutput>
 *   - sendEmailDistributed: TaskReference<SendEmailDistributedInput, SendEmailDistributedOutput>
 */
export const distributedTasks = createTaskRefs(DistributedTasks);
