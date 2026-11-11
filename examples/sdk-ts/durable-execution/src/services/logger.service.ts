import { Injectable } from '@orcher/sdk';
import { ConfigService } from './config.service';

/**
 * LogLevel enum
 */
export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
}

/**
 * LoggerService
 *
 * Centralized logging service with level-based filtering.
 * Injects ConfigService to determine the appropriate log level.
 *
 * This service demonstrates:
 * - Constructor-based dependency injection
 * - Service-to-service dependencies
 * - Structured logging with context
 */
@Injectable()
export class LoggerService {
  private logLevel: LogLevel;

  constructor(private config: ConfigService) {
    // Determine log level from config
    const configuredLevel = this.config.getLogLevel().toLowerCase();
    this.logLevel = this.parseLogLevel(configuredLevel);
  }

  /**
   * Parse log level string to enum
   */
  private parseLogLevel(level: string): LogLevel {
    switch (level) {
      case 'debug':
        return LogLevel.DEBUG;
      case 'info':
        return LogLevel.INFO;
      case 'warn':
        return LogLevel.WARN;
      case 'error':
        return LogLevel.ERROR;
      default:
        return LogLevel.INFO;
    }
  }

  /**
   * Format log message with timestamp and context
   */
  private formatMessage(level: string, message: string, context?: Record<string, any>): string {
    const timestamp = new Date().toISOString();
    const env = this.config.isProduction() ? 'prod' : 'dev';
    const contextStr = context ? ` ${JSON.stringify(context)}` : '';
    return `[${timestamp}] [${env}] [${level}] ${message}${contextStr}`;
  }

  /**
   * Log debug message
   */
  debug(message: string, context?: Record<string, any>): void {
    if (this.logLevel <= LogLevel.DEBUG) {
      console.debug(this.formatMessage('DEBUG', message, context));
    }
  }

  /**
   * Log info message
   */
  info(message: string, context?: Record<string, any>): void {
    if (this.logLevel <= LogLevel.INFO) {
      console.info(this.formatMessage('INFO', message, context));
    }
  }

  /**
   * Log warning message
   */
  warn(message: string, context?: Record<string, any>): void {
    if (this.logLevel <= LogLevel.WARN) {
      console.warn(this.formatMessage('WARN', message, context));
    }
  }

  /**
   * Log error message
   */
  error(message: string, error?: Error, context?: Record<string, any>): void {
    if (this.logLevel <= LogLevel.ERROR) {
      const errorContext = error
        ? { ...context, error: error.message, stack: error.stack }
        : context;
      console.error(this.formatMessage('ERROR', message, errorContext));
    }
  }

  /**
   * Log with explicit level
   */
  log(level: LogLevel, message: string, context?: Record<string, any>): void {
    switch (level) {
      case LogLevel.DEBUG:
        this.debug(message, context);
        break;
      case LogLevel.INFO:
        this.info(message, context);
        break;
      case LogLevel.WARN:
        this.warn(message, context);
        break;
      case LogLevel.ERROR:
        this.error(message, undefined, context);
        break;
    }
  }
}
