import { Injectable } from '@orcher/sdk';

/**
 * ConfigService
 *
 * Centralized configuration management service.
 * Provides access to application settings and environment variables.
 *
 * This is a singleton service that gets injected into other services
 * that need configuration values.
 */
@Injectable()
export class ConfigService {
  private readonly config: {
    stripe: {
      apiKey: string;
      webhookSecret: string;
    };
    email: {
      from: string;
      smtpHost: string;
      smtpPort: number;
    };
    app: {
      environment: string;
      logLevel: string;
    };
  };

  constructor() {
    // In production, load from environment variables or config files
    this.config = {
      stripe: {
        apiKey: process.env.STRIPE_API_KEY || 'sk_test_dummy',
        webhookSecret: process.env.STRIPE_WEBHOOK_SECRET || 'whsec_dummy',
      },
      email: {
        from: process.env.EMAIL_FROM || 'noreply@example.com',
        smtpHost: process.env.SMTP_HOST || 'smtp.example.com',
        smtpPort: parseInt(process.env.SMTP_PORT || '587', 10),
      },
      app: {
        environment: process.env.NODE_ENV || 'development',
        logLevel: process.env.LOG_LEVEL || 'info',
      },
    };
  }

  /**
   * Get Stripe configuration
   */
  getStripeConfig() {
    return this.config.stripe;
  }

  /**
   * Get email configuration
   */
  getEmailConfig() {
    return this.config.email;
  }

  /**
   * Get application configuration
   */
  getAppConfig() {
    return this.config.app;
  }

  /**
   * Check if running in production
   */
  isProduction(): boolean {
    return this.config.app.environment === 'production';
  }

  /**
   * Check if running in development
   */
  isDevelopment(): boolean {
    return this.config.app.environment === 'development';
  }

  /**
   * Get log level
   */
  getLogLevel(): string {
    return this.config.app.logLevel;
  }
}
