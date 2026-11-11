import "reflect-metadata";
import { Worker, globalRegistry } from "@orcher/sdk";

/**
 * Main Entry Point - Explicit Import Mode
 *
 * This example demonstrates how to bootstrap the ORCHER SDK with explicit imports.
 * This mode requires manually importing all service, task, and workflow files.
 *
 * Key concepts:
 * - Explicit imports trigger decorator registration
 * - No file system scanning at runtime
 * - Faster startup time (no globbing overhead)
 * - Better tree-shaking and bundling for production
 * - Recommended for production, serverless, and packaged deployments
 *
 * How it works:
 * 1. Import statements execute, loading class definitions
 * 2. Decorators (@Injectable, @Tasks, @Workflow) execute during import
 * 3. Decorators register components in GlobalRegistry
 * 4. Worker.builder().build() pulls from GlobalRegistry into DI container
 * 5. No need for .autoDiscover() when using explicit imports
 *
 * Pros:
 * - ✅ Faster startup (no file system scanning)
 * - ✅ Better for bundlers (webpack, esbuild, etc.)
 * - ✅ Explicit dependency graph (easier to understand)
 * - ✅ No runtime globbing dependencies
 * - ✅ Works in serverless/edge environments
 *
 * Cons:
 * - ❌ Must manually update imports when adding new files
 * - ❌ More verbose than auto-discovery
 */

// ============================================================================
// Explicit Imports - Trigger Decorator Registration
// ============================================================================

// Import services
// These @Injectable() decorators will register in GlobalRegistry
import "./services/config.service";
import "./services/logger.service";
import "./services/stripe.service";
import "./services/email.service";

// Import task handlers
// These @Tasks() and @Task() decorators will register in GlobalRegistry
import "./tasks/payment.tasks";

// Import workflows
// These @Workflow() decorators will register in GlobalRegistry
import "./workflows/payment.workflow";

// ============================================================================
// Main Function
// ============================================================================

async function main() {
  console.log("🚀 Starting ORCHER SDK Worker with Explicit Imports...\n");

  try {
    // Build service WITHOUT auto-discovery
    // The DI container will use components already registered in GlobalRegistry
    // via the import statements above
    const worker = await Worker.builder()
      // Server connection settings
      .serverUrl(process.env.ORCHER_SERVER_URL || "http://localhost:50051")
      .namespace(process.env.NAMESPACE || "default")
      .taskQueue(process.env.TASK_QUEUE || "payment-processing")
      // NOTE: No .autoDiscover() call needed!
      // Components are already registered via explicit imports above
      .build();

    // Log registered components
    console.log("📦 Registered Components:");
    const diStats = globalRegistry.getStats();
    console.log(`   - Services: ${diStats.services}`);
    console.log(`   - Task Handlers: ${diStats.taskHandlers}`);
    console.log(`   - Tasks: ${diStats.tasks}`);
    console.log(`   - Workflows: ${diStats.workflows}\n`);

    // Warn if nothing was registered (likely import issue)
    if (diStats.services === 0) {
      console.warn(
        "⚠️  No services registered. Ensure service files are imported above.",
      );
    }
    if (diStats.taskHandlers === 0) {
      console.warn(
        "⚠️  No task handlers registered. Ensure task handler files are imported above.",
      );
    }
    if (diStats.workflows === 0) {
      console.warn(
        "⚠️  No workflows registered. Ensure workflow files are imported above.",
      );
    }

    // Start the service (begins processing)
    await worker.run();

    console.log("✅ Worker started successfully!");
    console.log("📡 Listening for workflows and tasks...\n");
    console.log("💡 Press Ctrl+C to shutdown gracefully\n");

    // Handle graceful shutdown
    const shutdown = async (signal: string) => {
      console.log(`\n🛑 Received ${signal}, shutting down gracefully...`);
      await worker.shutdown();
      console.log("✅ Worker stopped successfully");
      process.exit(0);
    };

    process.on("SIGINT", () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));

    // Keep the process alive
    await new Promise(() => {});
  } catch (error) {
    console.error("❌ Failed to start worker:", error);
    process.exit(1);
  }
}

// Bootstrap the application
main().catch((error) => {
  console.error("❌ Fatal error:", error);
  process.exit(1);
});
