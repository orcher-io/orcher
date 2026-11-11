# ORCHER SDK - Durable Execution Example with Dependency Injection

This example demonstrates how to build a payment processing workflow
using the ORCHER TypeScript SDK with full **Dependency Injection (DI)**
support.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Key Concepts](#key-concepts)
- [Getting Started](#getting-started)
- [Running the Example](#running-the-example)
- [Code Walkthrough](#code-walkthrough)
- [Auto-Discovery vs Explicit Imports](#auto-discovery-vs-explicit-imports)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

This example showcases a production-ready payment processing system that demonstrates:

- ✅ **Dependency Injection** for services and task handlers
- ✅ **Type-safe workflow orchestration** using TaskReferences
- ✅ **Durable execution** with automatic retries and recovery
- ✅ **Clean architecture** with separation of concerns
- ✅ **Two bootstrap modes**: Auto-discovery and explicit imports

### What's Included

- **Services**: ConfigService, LoggerService, StripeService, EmailService
- **Task Handlers**: PaymentTasks with charge, refund, notification, and verification tasks
- **Workflows**: PaymentWorkflow and RefundWorkflow
- **Bootstrap Options**: Auto-discovery mode and explicit import mode

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         ORCHER Worker                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌────────────────────────────────────────────────────────┐   │
│  │                    Workflows                            │   │
│  │  (Orchestration - NO DI)                               │   │
│  │                                                         │   │
│  │  • PaymentWorkflow    ──────┐                          │   │
│  │  • RefundWorkflow     ──────┤                          │   │
│  └─────────────────────────────┼───────────────────────────┘   │
│                                 │                               │
│                                 │ executeTask(TaskReference)    │
│                                 │                               │
│  ┌─────────────────────────────▼───────────────────────────┐   │
│  │                    Task Handlers                         │   │
│  │  (Side Effects - WITH DI)                               │   │
│  │                                                          │   │
│  │  • PaymentTasks  ┬─→ chargeCard()                       │   │
│  │                  ├─→ refundCharge()                      │   │
│  │                  ├─→ sendPaymentNotification()          │   │
│  │                  └─→ verifyChargeStatus()               │   │
│  └──────────────────┬──────────┬──────────┬─────────────────┘   │
│                     │          │          │                     │
│                     │          │          │                     │
│  ┌──────────────────▼──────────▼──────────▼─────────────────┐   │
│  │                    Services (DI Container)                │   │
│  │                                                           │   │
│  │  • ConfigService  ────────────────────┐                  │   │
│  │  • LoggerService  ◄──── ConfigService │                  │   │
│  │  • StripeService  ◄──── ConfigService, LoggerService     │   │
│  │  • EmailService   ◄──── ConfigService, LoggerService     │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Key Architectural Decisions

1. **Workflows are deterministic** - No DI, no constructor dependencies
2. **Tasks handle side effects** - Full DI support for external integrations
3. **Services are reusable** - Injected across multiple task handlers
4. **Type-safe orchestration** - TaskReferences provide compile-time safety

## Project Structure

```
durable-execution/
├── src/
│   ├── services/              # Injectable services
│   │   ├── config.service.ts  # Configuration management
│   │   ├── logger.service.ts  # Logging (injects ConfigService)
│   │   ├── stripe.service.ts  # Payment processing (injects Config + Logger)
│   │   └── email.service.ts   # Email notifications (injects Config + Logger)
│   │
│   ├── tasks/                 # Task handlers with DI
│   │   └── payment.tasks.ts   # Payment tasks (injects services)
│   │
│   ├── workflows/             # Workflow orchestration (NO DI)
│   │   └── payment.workflow.ts
│   │
│   ├── main.ts                # Entry point (auto-discovery mode)
│   └── main-explicit.ts       # Entry point (explicit import mode)
│
├── package.json
├── tsconfig.json
└── README.md
```

## Key Concepts

### 1. Workflows Cannot Use DI

**❌ WRONG - Do NOT do this:**

```typescript
@Workflow({ name: 'paymentWorkflow', version: '1.0' })
@Injectable()  // ❌ ERROR: Workflows must NOT be @Injectable
export class PaymentWorkflow {
  constructor(
    private stripe: StripeService  // ❌ ERROR: No constructor DI in workflows
  ) {}

  async run(ctx: WorkflowContext, input: any) {
    // ❌ This breaks deterministic replay!
    await this.stripe.charge(...);
  }
}
```

**✅ CORRECT - Workflows orchestrate via tasks:**

```typescript
@Workflow({ name: "paymentWorkflow", version: "1.0" })
export class PaymentWorkflow {
  // ✅ No @Injectable, no constructor

  async run(ctx: WorkflowContext, input: PaymentInput) {
    // ✅ Execute tasks using TaskReferences
    const result = await ctx.executeTask(PaymentTasks.chargeCard, {
      amount: input.amount,
      token: input.cardToken,
    });
    return result;
  }
}
```

### 2. Tasks Use DI for Side Effects

```typescript
@Tasks() // ✅ Task handler class
export class PaymentTasks {
  constructor(
    private stripe: StripeService, // ✅ DI injection
    private email: EmailService, // ✅ DI injection
    private logger: LoggerService, // ✅ DI injection
  ) {}

  @Task({ name: "chargeCard", timeout: 30000 })
  async chargeCard(ctx: TaskContext, input: ChargeCardInput) {
    // ✅ Use injected services for side effects
    return await this.stripe.charge(input.amount, input.token);
  }
}
```

### 3. Type-Safe Task Execution

```typescript
// ✅ TaskReferences are auto-generated static properties
// They provide full type safety for task execution

// In workflow:
const result = await ctx.executeTask(
  PaymentTasks.chargeCard, // Static TaskReference
  {
    amount: 9999, // Input is type-checked
    token: "tok_abc", // TypeScript knows the shape
    orderId: "order-123",
    customerEmail: "user@example.com",
  },
);

// result.chargeId  // Output is type-checked too!
// result.amount
// result.status
```

### 4. Service Dependency Graph

```typescript
// ConfigService has no dependencies
@Injectable()
export class ConfigService {
  constructor() {
    /* load config */
  }
}

// LoggerService depends on ConfigService
@Injectable()
export class LoggerService {
  constructor(private config: ConfigService) {
    // config is automatically injected
  }
}

// StripeService depends on Config + Logger
@Injectable()
export class StripeService {
  constructor(
    private config: ConfigService,
    private logger: LoggerService,
  ) {
    // Both are automatically injected
  }
}
```

## Getting Started

### Prerequisites

- Node.js 18+
- TypeScript 5.3+
- ORCHER server running (or use mock mode)

### Installation

```bash
# From the example directory
cd examples/durable-execution

# Install dependencies
npm install

# Build
npm run build
```

### Environment Variables

Create a `.env` file in the example directory:

```bash
# ORCHER Server
ORCHER_SERVER_URL=http://localhost:50051
TASK_QUEUE=payment-processing

# Stripe Configuration
STRIPE_API_KEY=sk_test_your_stripe_key
STRIPE_WEBHOOK_SECRET=whsec_your_webhook_secret

# Email Configuration
EMAIL_FROM=noreply@example.com
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USER=your_smtp_user
SMTP_PASS=your_smtp_password

# Application
NODE_ENV=development
LOG_LEVEL=info
```

## Running the Example

### Option 1: Auto-Discovery Mode (Recommended for Development)

Auto-discovery automatically scans and imports files based on glob patterns:

```bash
# Development mode with hot reload
npm run dev

# Or production mode
npm run build
npm run start
```

**How it works:**

- Scans `./src/services/**/*.ts`, `./src/tasks/**/*.ts`, `./src/workflows/**/*.ts`
- Automatically imports matching files
- Decorators register components in GlobalRegistry
- Fast feedback loop for development

### Option 2: Explicit Import Mode (Recommended for Production)

Explicit imports require manual file imports but offer better performance:

```bash
# Development mode
npm run dev:explicit

# Or production mode
npm run build
npm run start:explicit
```

**How it works:**

- Files are explicitly imported in `main-explicit.ts`
- No file system scanning at runtime
- Faster startup, better for bundlers
- Ideal for serverless and production

## Code Walkthrough

### Service Layer

#### ConfigService

```typescript
@Injectable()
export class ConfigService {
  // Centralized configuration management
  // Loaded once, injected everywhere
  getStripeConfig() {
    /* ... */
  }
  getEmailConfig() {
    /* ... */
  }
  isProduction() {
    /* ... */
  }
}
```

#### LoggerService

```typescript
@Injectable()
export class LoggerService {
  constructor(private config: ConfigService) {
    // Injects ConfigService to determine log level
  }

  info(message: string, context?: Record<string, any>) {
    /* ... */
  }
  error(message: string, error?: Error, context?: Record<string, any>) {
    /* ... */
  }
}
```

#### StripeService

```typescript
@Injectable()
export class StripeService {
  constructor(
    private config: ConfigService,
    private logger: LoggerService,
  ) {
    // Both dependencies automatically injected
  }

  async charge(amount: number, token: string) {
    this.logger.info("Charging card", { amount });
    // Call Stripe API
    return result;
  }
}
```

### Task Layer

#### PaymentTasks

```typescript
@Tasks()  // Marks as task handler class
export class PaymentTasks {
  constructor(
    private stripe: StripeService,
    private email: EmailService,
    private logger: LoggerService
  ) {}

  // Each @Task() creates a static TaskReference
  @Task({ name: 'chargeCard', timeout: 30000 })
  async chargeCard(ctx: TaskContext, input: ChargeCardInput) {
    // Use injected services
    return await this.stripe.charge(input.amount, input.token);
  }

  @Task({ name: 'sendPaymentNotification', timeout: 20000 })
  async sendPaymentNotification(ctx: TaskContext, input: NotificationInput) {
    return await this.email.sendPaymentSuccess(...);
  }
}
```

**Generated TaskReferences:**

- `PaymentTasks.chargeCard` - Static property for type-safe execution
- `PaymentTasks.sendPaymentNotification` - Static property

### Workflow Layer

#### PaymentWorkflow

```typescript
@Workflow({ name: "paymentWorkflow", version: "1.0" })
export class PaymentWorkflow implements IWorkflow<Input, Output> {
  // ❌ NO constructor
  // ❌ NO @Injectable()

  async run(ctx: WorkflowContext, input: PaymentInput) {
    // Step 1: Charge card
    const charge = await ctx.executeTask(PaymentTasks.chargeCard, {
      amount: input.amount,
      token: input.cardToken,
      orderId: input.orderId,
      customerEmail: input.customerEmail,
    });

    // Step 2: Verify charge
    const verify = await ctx.executeTask(PaymentTasks.verifyChargeStatus, {
      chargeId: charge.chargeId,
    });

    // Step 3: Send notification
    await ctx.executeTask(PaymentTasks.sendPaymentNotification, {
      type: "success",
      email: input.customerEmail,
      orderId: input.orderId,
      chargeId: charge.chargeId,
      amount: input.amount,
    });

    return {
      orderId: input.orderId,
      chargeId: charge.chargeId,
      status: "completed",
    };
  }
}
```

## Auto-Discovery vs Explicit Imports

### Auto-Discovery Mode

**When to use:**

- ✅ Development and local testing
- ✅ Monolithic deployments
- ✅ When you want fast iteration

**Pros:**

- Automatic file detection
- No manual import management
- Easy to add new components

**Cons:**

- Runtime file scanning overhead
- Requires `fast-glob` dependency
- Not ideal for serverless/edge

**Example:**

```typescript
const service = await Service.builder()
  .serverUrl("http://localhost:50051")
  .autoDiscover({
    patterns: ["./src/**/*.ts"],
    exclude: ["**/*.test.ts"],
  })
  .build();
```

### Explicit Import Mode

**When to use:**

- ✅ Production deployments
- ✅ Serverless (Lambda, CloudFlare Workers, etc.)
- ✅ When using bundlers (webpack, esbuild)
- ✅ When startup performance matters

**Pros:**

- Faster startup (no globbing)
- Better tree-shaking
- Works everywhere (no fs access needed)
- Clear dependency graph

**Cons:**

- Must manually import each file
- More verbose

**Example:**

```typescript
// Explicit imports
import "./services/config.service";
import "./services/logger.service";
import "./tasks/payment.tasks";
import "./workflows/payment.workflow";

const service = await Service.builder()
  .serverUrl("http://localhost:50051")
  .build(); // No .autoDiscover() needed
```

## Best Practices

### 1. Service Design

✅ **Do:**

- Keep services focused and single-purpose
- Use constructor injection for dependencies
- Return meaningful error messages
- Add comprehensive logging

❌ **Don't:**

- Put business logic in services (use task handlers)
- Create circular service dependencies
- Mix concerns (e.g., database + email in one service)

### 2. Task Handler Design

✅ **Do:**

- Use descriptive task names
- Set appropriate timeouts
- Handle errors gracefully
- Use TaskContext for metadata
- Return structured results

❌ **Don't:**

- Put orchestration logic in tasks
- Create tasks that depend on other tasks
- Use global state or singletons outside DI

### 3. Workflow Design

✅ **Do:**

- Keep workflows pure orchestration
- Use TaskReferences for type safety
- Handle task failures explicitly
- Return structured workflow results

❌ **Don't:**

- Use DI in workflows (@Injectable or constructors)
- Call services directly from workflows
- Use non-deterministic operations (Date.now(), Math.random())
- Rely on external state

### 4. Error Handling

```typescript
// ✅ Good: Handle task failures in workflows
try {
  const result = await ctx.executeTask(PaymentTasks.chargeCard, input);
} catch (error) {
  // Send failure notification
  await ctx.executeTask(PaymentTasks.sendPaymentNotification, {
    type: "failure",
    reason: error.message,
  });
  return { status: "failed", error: error.message };
}
```

### 5. Configuration Management

```typescript
// ✅ Good: Use ConfigService for all configuration
@Injectable()
export class StripeService {
  constructor(private config: ConfigService) {
    this.apiKey = config.getStripeConfig().apiKey;
  }
}

// ❌ Bad: Direct env access
@Injectable()
export class StripeService {
  private apiKey = process.env.STRIPE_API_KEY; // ❌ Not testable
}
```

## Troubleshooting

### "Cannot resolve dependency" Error

**Problem:** Service not registered in DI container

**Solutions:**

1. Ensure service has `@Injectable()` decorator
2. Check file is imported (explicit mode) or matched by glob pattern (auto-discovery)
3. Verify no circular dependencies

### "Circular dependency detected" Error

**Problem:** Service A depends on B, and B depends on A

**Solutions:**

1. Refactor to extract shared logic into a third service
2. Use provider factories to break the cycle
3. Reconsider service boundaries

### Workflow Not Executing

**Problem:** Workflow not registered

**Solutions:**

1. Ensure workflow has `@Workflow()` decorator
2. Check file is imported (explicit mode) or matched by pattern (auto-discovery)
3. Verify `getStats()` shows workflow is registered

### Task Not Found

**Problem:** TaskReference not resolving

**Solutions:**

1. Ensure task method has `@Task()` decorator
2. Verify task handler class has `@Tasks()` decorator
3. Check TaskReference is accessed as static property: `PaymentTasks.chargeCard`

### Slow Startup with Auto-Discovery

**Problem:** File scanning takes too long

**Solutions:**

1. Narrow glob patterns to specific directories
2. Add more exclude patterns
3. Switch to explicit import mode for production

## Additional Resources

- [ORCHER SDK Documentation](../../README.md)
- [Dependency Injection Guide](../../docs/DI_GUIDE.md)
- [Workflow Best Practices](../../docs/WORKFLOWS.md)
- [Task Handler Guide](../../docs/TASKS.md)

## Support

For questions or issues:

- GitHub Issues: <https://github.com/orcher/orcher>
- Documentation: <https://docs.orcher.io>
- Discord: <https://discord.gg/orcher>

---

## 🧪 Testing

### Running E2E Tests

We provide comprehensive testing scripts to validate the example works correctly with the ORCHER orchestrator.

**Quick Start**:

```bash
# Run complete E2E test (cleans DB, builds, runs test)
./run-e2e-test.sh
```

**Available Scripts**:

1. **`run-e2e-test.sh`** - Complete end-to-end test

   - Cleans database tables
   - Checks PostgreSQL and orchestrator
   - Installs dependencies and builds
   - Starts worker and runs test workflow
   - Shows comprehensive results

2. **`clean-database.sh`** - Standalone database cleanup
   - Truncates workflow execution tables
   - Safe reset for testing
   - Confirmation prompt (use `--yes` to skip)

**Test Options**:

```bash
# Skip database cleanup
./run-e2e-test.sh --skip-db-clean

# Skip build (faster iteration)
./run-e2e-test.sh --skip-build

# Test explicit import mode
./run-e2e-test.sh --explicit

# Show help
./run-e2e-test.sh --help
```

**Prerequisites**:

- PostgreSQL running (localhost:5432)
- ORCHER orchestrator running (localhost:50051)
- Node.js 18+

**Environment Variables**:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=postgres
export DB_NAME=orcher
export ORCHER_SERVER_URL=http://localhost:50051
export TASK_QUEUE=payment-processing
```

For detailed testing documentation, see [TESTING.md](./TESTING.md).

---

**Happy coding with ORCHER! 🚀**
