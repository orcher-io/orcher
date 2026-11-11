import { Injectable } from '@orcher/sdk';
import { ConfigService } from './config.service';
import { LoggerService } from './logger.service';

/**
 * Email send result interface
 */
export interface EmailResult {
  messageId: string;
  success: boolean;
  error?: string;
}

/**
 * Email options interface
 */
export interface EmailOptions {
  to: string;
  subject: string;
  body: string;
  html?: string;
  cc?: string[];
  bcc?: string[];
}

/**
 * EmailService
 *
 * Service for sending emails via SMTP.
 * Demonstrates dependency injection of ConfigService and LoggerService.
 *
 * This is a mock implementation for demonstration purposes.
 * In production, this would use a real email service like SendGrid, AWS SES, or nodemailer.
 *
 * Key DI concepts demonstrated:
 * - Constructor injection of configuration and logging dependencies
 * - Service reusability across multiple task handlers
 * - Clean separation between business logic and infrastructure
 */
@Injectable()
export class EmailService {
  private readonly fromAddress: string;
  private readonly smtpHost: string;
  private readonly smtpPort: number;

  constructor(
    private config: ConfigService,
    private logger: LoggerService
  ) {
    // Get email configuration from config service
    const emailConfig = this.config.getEmailConfig();
    this.fromAddress = emailConfig.from;
    this.smtpHost = emailConfig.smtpHost;
    this.smtpPort = emailConfig.smtpPort;

    this.logger.info('EmailService initialized', {
      from: this.fromAddress,
      smtpHost: this.smtpHost,
      smtpPort: this.smtpPort,
    });
  }

  /**
   * Send a confirmation email
   *
   * @param to Recipient email address
   * @param orderId Order ID to include in the email
   * @param amount Order amount
   */
  async sendOrderConfirmation(
    to: string,
    orderId: string,
    amount: number
  ): Promise<EmailResult> {
    this.logger.info('Sending order confirmation email', {
      to,
      orderId,
      amount,
    });

    const subject = `Order Confirmation - ${orderId}`;
    const body = this.buildOrderConfirmationBody(orderId, amount);
    const html = this.buildOrderConfirmationHtml(orderId, amount);

    return this.sendEmail({
      to,
      subject,
      body,
      html,
    });
  }

  /**
   * Send a payment success email
   *
   * @param to Recipient email address
   * @param orderId Order ID
   * @param chargeId Stripe charge ID
   * @param amount Amount charged
   */
  async sendPaymentSuccess(
    to: string,
    orderId: string,
    chargeId: string,
    amount: number
  ): Promise<EmailResult> {
    this.logger.info('Sending payment success email', {
      to,
      orderId,
      chargeId,
      amount,
    });

    const subject = `Payment Received - ${orderId}`;
    const body = this.buildPaymentSuccessBody(orderId, chargeId, amount);
    const html = this.buildPaymentSuccessHtml(orderId, chargeId, amount);

    return this.sendEmail({
      to,
      subject,
      body,
      html,
    });
  }

  /**
   * Send a payment failure email
   *
   * @param to Recipient email address
   * @param orderId Order ID
   * @param reason Failure reason
   */
  async sendPaymentFailure(
    to: string,
    orderId: string,
    reason: string
  ): Promise<EmailResult> {
    this.logger.info('Sending payment failure email', {
      to,
      orderId,
      reason,
    });

    const subject = `Payment Failed - ${orderId}`;
    const body = this.buildPaymentFailureBody(orderId, reason);
    const html = this.buildPaymentFailureHtml(orderId, reason);

    return this.sendEmail({
      to,
      subject,
      body,
      html,
    });
  }

  /**
   * Send a refund notification email
   *
   * @param to Recipient email address
   * @param orderId Order ID
   * @param refundId Stripe refund ID
   * @param amount Refund amount
   */
  async sendRefundNotification(
    to: string,
    orderId: string,
    refundId: string,
    amount: number
  ): Promise<EmailResult> {
    this.logger.info('Sending refund notification email', {
      to,
      orderId,
      refundId,
      amount,
    });

    const subject = `Refund Processed - ${orderId}`;
    const body = this.buildRefundNotificationBody(orderId, refundId, amount);
    const html = this.buildRefundNotificationHtml(orderId, refundId, amount);

    return this.sendEmail({
      to,
      subject,
      body,
      html,
    });
  }

  /**
   * Send a generic email
   *
   * @param options Email options
   */
  async sendEmail(options: EmailOptions): Promise<EmailResult> {
    this.logger.debug('Sending email', {
      to: options.to,
      subject: options.subject,
    });

    try {
      // Simulate network delay
      await this.simulateNetworkDelay();

      // Mock implementation - in production, use a real email service:
      // const nodemailer = require('nodemailer');
      // const transporter = nodemailer.createTransport({
      //   host: this.smtpHost,
      //   port: this.smtpPort,
      //   secure: this.smtpPort === 465,
      //   auth: {
      //     user: process.env.SMTP_USER,
      //     pass: process.env.SMTP_PASS,
      //   },
      // });
      //
      // const info = await transporter.sendMail({
      //   from: this.fromAddress,
      //   to: options.to,
      //   subject: options.subject,
      //   text: options.body,
      //   html: options.html,
      //   cc: options.cc,
      //   bcc: options.bcc,
      // });

      // For demo purposes, simulate successful send
      const messageId = `msg_${this.generateId()}`;

      this.logger.info('Email sent successfully', {
        messageId,
        to: options.to,
        subject: options.subject,
      });

      return {
        messageId,
        success: true,
      };
    } catch (error) {
      this.logger.error('Failed to send email', error as Error, {
        to: options.to,
        subject: options.subject,
      });

      return {
        messageId: '',
        success: false,
        error: (error as Error).message || 'Failed to send email',
      };
    }
  }

  /**
   * Build plain text order confirmation body
   */
  private buildOrderConfirmationBody(orderId: string, amount: number): string {
    return `
Thank you for your order!

Order Details:
- Order ID: ${orderId}
- Amount: $${(amount / 100).toFixed(2)}

Your order has been confirmed and is being processed.

Thank you for your business!
    `.trim();
  }

  /**
   * Build HTML order confirmation body
   */
  private buildOrderConfirmationHtml(orderId: string, amount: number): string {
    return `
      <html>
        <body style="font-family: Arial, sans-serif;">
          <h2>Thank you for your order!</h2>
          <div style="background-color: #f5f5f5; padding: 20px; border-radius: 5px;">
            <h3>Order Details</h3>
            <p><strong>Order ID:</strong> ${orderId}</p>
            <p><strong>Amount:</strong> $${(amount / 100).toFixed(2)}</p>
          </div>
          <p>Your order has been confirmed and is being processed.</p>
          <p>Thank you for your business!</p>
        </body>
      </html>
    `.trim();
  }

  /**
   * Build plain text payment success body
   */
  private buildPaymentSuccessBody(orderId: string, chargeId: string, amount: number): string {
    return `
Payment Received

Your payment has been processed successfully!

Payment Details:
- Order ID: ${orderId}
- Charge ID: ${chargeId}
- Amount: $${(amount / 100).toFixed(2)}

Thank you for your payment!
    `.trim();
  }

  /**
   * Build HTML payment success body
   */
  private buildPaymentSuccessHtml(orderId: string, chargeId: string, amount: number): string {
    return `
      <html>
        <body style="font-family: Arial, sans-serif;">
          <h2 style="color: #28a745;">Payment Received</h2>
          <p>Your payment has been processed successfully!</p>
          <div style="background-color: #f5f5f5; padding: 20px; border-radius: 5px;">
            <h3>Payment Details</h3>
            <p><strong>Order ID:</strong> ${orderId}</p>
            <p><strong>Charge ID:</strong> ${chargeId}</p>
            <p><strong>Amount:</strong> $${(amount / 100).toFixed(2)}</p>
          </div>
          <p>Thank you for your payment!</p>
        </body>
      </html>
    `.trim();
  }

  /**
   * Build plain text payment failure body
   */
  private buildPaymentFailureBody(orderId: string, reason: string): string {
    return `
Payment Failed

Unfortunately, we were unable to process your payment.

Order Details:
- Order ID: ${orderId}
- Reason: ${reason}

Please verify your payment information and try again.

If you continue to experience issues, please contact our support team.
    `.trim();
  }

  /**
   * Build HTML payment failure body
   */
  private buildPaymentFailureHtml(orderId: string, reason: string): string {
    return `
      <html>
        <body style="font-family: Arial, sans-serif;">
          <h2 style="color: #dc3545;">Payment Failed</h2>
          <p>Unfortunately, we were unable to process your payment.</p>
          <div style="background-color: #f8d7da; padding: 20px; border-radius: 5px; border: 1px solid #f5c6cb;">
            <h3>Order Details</h3>
            <p><strong>Order ID:</strong> ${orderId}</p>
            <p><strong>Reason:</strong> ${reason}</p>
          </div>
          <p>Please verify your payment information and try again.</p>
          <p>If you continue to experience issues, please contact our support team.</p>
        </body>
      </html>
    `.trim();
  }

  /**
   * Build plain text refund notification body
   */
  private buildRefundNotificationBody(orderId: string, refundId: string, amount: number): string {
    return `
Refund Processed

Your refund has been processed successfully!

Refund Details:
- Order ID: ${orderId}
- Refund ID: ${refundId}
- Amount: $${(amount / 100).toFixed(2)}

The refund will appear on your statement within 5-10 business days.
    `.trim();
  }

  /**
   * Build HTML refund notification body
   */
  private buildRefundNotificationHtml(orderId: string, refundId: string, amount: number): string {
    return `
      <html>
        <body style="font-family: Arial, sans-serif;">
          <h2 style="color: #17a2b8;">Refund Processed</h2>
          <p>Your refund has been processed successfully!</p>
          <div style="background-color: #f5f5f5; padding: 20px; border-radius: 5px;">
            <h3>Refund Details</h3>
            <p><strong>Order ID:</strong> ${orderId}</p>
            <p><strong>Refund ID:</strong> ${refundId}</p>
            <p><strong>Amount:</strong> $${(amount / 100).toFixed(2)}</p>
          </div>
          <p>The refund will appear on your statement within 5-10 business days.</p>
        </body>
      </html>
    `.trim();
  }

  /**
   * Simulate network delay for realistic demo
   */
  private async simulateNetworkDelay(): Promise<void> {
    // Add realistic network latency (50-200ms for email)
    const delay = 50 + Math.random() * 150;
    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  /**
   * Generate a random ID for demo purposes
   */
  private generateId(): string {
    return Math.random().toString(36).substring(2, 15) +
           Math.random().toString(36).substring(2, 15);
  }
}
