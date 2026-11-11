import { Injectable } from "@orcher/sdk";
import { ConfigService } from "./config.service";
import { LoggerService } from "./logger.service";

/**
 * Charge result interface
 */
export interface ChargeResult {
  id: string;
  amount: number;
  currency: string;
  status: "succeeded" | "failed" | "pending";
  created: number;
  failureCode?: string;
  failureMessage?: string;
}

/**
 * Refund result interface
 */
export interface RefundResult {
  id: string;
  chargeId: string;
  amount: number;
  status: "succeeded" | "failed" | "pending";
  created: number;
}

/**
 * StripeService
 *
 * Service for interacting with Stripe payment processing API.
 * Demonstrates dependency injection of ConfigService and LoggerService.
 *
 * This is a mock implementation for demonstration purposes.
 * In production, this would use the actual Stripe SDK.
 *
 * Key DI concepts demonstrated:
 * - Constructor injection of multiple dependencies
 * - Service composition (using config and logger)
 * - Separation of concerns (business logic vs infrastructure)
 */
@Injectable()
export class StripeService {
  private readonly apiKey: string;

  constructor(
    private config: ConfigService,
    private logger: LoggerService,
  ) {
    // Get Stripe API key from config service
    const stripeConfig = this.config.getStripeConfig();
    this.apiKey = stripeConfig.apiKey;

    this.logger.info("StripeService initialized", {
      hasApiKey: !!this.apiKey,
      environment: this.config.isProduction() ? "production" : "development",
    });
  }

  /**
   * Charge a credit card
   *
   * @param amount Amount in cents (e.g., 9999 = $99.99)
   * @param token Payment token from Stripe.js
   * @param idempotencyKey Optional idempotency key for safe retries
   */
  async charge(
    amount: number,
    token: string,
    idempotencyKey?: string,
  ): Promise<ChargeResult> {
    this.logger.info("Charging card", {
      amount,
      token: `${token.substring(0, 8)}...`,
      idempotencyKey,
    });

    try {
      // Simulate API call delay
      await this.simulateNetworkDelay();

      // Mock implementation - in production, use Stripe SDK:
      // const stripe = new Stripe(this.apiKey, { apiVersion: '2023-10-16' });
      // const charge = await stripe.charges.create({
      //   amount,
      //   currency: 'usd',
      //   source: token,
      // }, { idempotencyKey });

      // Simulate successful charge
      const charge: ChargeResult = {
        id: `ch_${this.generateId()}`,
        amount,
        currency: "usd",
        status: "succeeded",
        created: Math.floor(Date.now() / 1000),
      };

      this.logger.info("Card charged successfully", {
        chargeId: charge.id,
        amount: charge.amount,
      });

      return charge;
    } catch (error) {
      this.logger.error("Failed to charge card", error as Error, {
        amount,
        token: `${token.substring(0, 8)}...`,
      });

      // Return failed charge result
      return {
        id: `ch_${this.generateId()}`,
        amount,
        currency: "usd",
        status: "failed",
        created: Math.floor(Date.now() / 1000),
        failureCode: "card_declined",
        failureMessage: (error as Error).message || "Card was declined",
      };
    }
  }

  /**
   * Refund a charge
   *
   * @param chargeId The ID of the charge to refund
   * @param amount Optional partial refund amount (in cents)
   */
  async refund(chargeId: string, amount?: number): Promise<RefundResult> {
    this.logger.info("Refunding charge", {
      chargeId,
      amount: amount || "full refund",
    });

    try {
      // Simulate API call delay
      await this.simulateNetworkDelay();

      // Mock implementation - in production, use Stripe SDK:
      // const stripe = new Stripe(this.apiKey, { apiVersion: '2023-10-16' });
      // const refund = await stripe.refunds.create({
      //   charge: chargeId,
      //   amount,
      // });

      const refund: RefundResult = {
        id: `re_${this.generateId()}`,
        chargeId,
        amount: amount || 0, // In real implementation, fetch original charge amount
        status: "succeeded",
        created: Math.floor(Date.now() / 1000),
      };

      this.logger.info("Charge refunded successfully", {
        refundId: refund.id,
        chargeId,
        amount: refund.amount,
      });

      return refund;
    } catch (error) {
      this.logger.error("Failed to refund charge", error as Error, {
        chargeId,
        amount,
      });

      return {
        id: `re_${this.generateId()}`,
        chargeId,
        amount: amount || 0,
        status: "failed",
        created: Math.floor(Date.now() / 1000),
      };
    }
  }

  /**
   * Verify a webhook signature
   *
   * @param payload Raw webhook payload
   * @param signature Stripe-Signature header value
   */
  verifyWebhookSignature(_payload: string, _signature: string): boolean {
    this.logger.debug("Verifying webhook signature");

    try {
      // Mock implementation - in production, use Stripe SDK:
      // const stripe = new Stripe(this.apiKey, { apiVersion: '2023-10-16' });
      // const event = stripe.webhooks.constructEvent(
      //   payload,
      //   signature,
      //   this.config.getStripeConfig().webhookSecret
      // );

      // For demo purposes, just check if signature is present
      const isValid = !!_signature && _signature.length > 0;

      this.logger.debug("Webhook signature verification result", { isValid });

      return isValid;
    } catch (error) {
      this.logger.error(
        "Webhook signature verification failed",
        error as Error,
      );
      return false;
    }
  }

  /**
   * Get charge details
   *
   * @param chargeId The ID of the charge to retrieve
   */
  async getCharge(chargeId: string): Promise<ChargeResult | null> {
    this.logger.debug("Retrieving charge", { chargeId });

    try {
      // Simulate API call delay
      await this.simulateNetworkDelay();

      // Mock implementation - in production, use Stripe SDK:
      // const stripe = new Stripe(this.apiKey, { apiVersion: '2023-10-16' });
      // const charge = await stripe.charges.retrieve(chargeId);

      // For demo, return mock data
      return {
        id: chargeId,
        amount: 9999,
        currency: "usd",
        status: "succeeded",
        created: Math.floor(Date.now() / 1000),
      };
    } catch (error) {
      this.logger.error("Failed to retrieve charge", error as Error, {
        chargeId,
      });
      return null;
    }
  }

  /**
   * Simulate network delay for realistic demo
   */
  private async simulateNetworkDelay(): Promise<void> {
    // Add realistic network latency (100-300ms)
    const delay = 100 + Math.random() * 200;
    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  /**
   * Generate a random ID for demo purposes
   */
  private generateId(): string {
    return (
      Math.random().toString(36).substring(2, 15) +
      Math.random().toString(36).substring(2, 15)
    );
  }
}
