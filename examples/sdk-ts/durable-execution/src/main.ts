import "reflect-metadata";
import { Worker, globalRegistry } from "@orcher/sdk";

/**
 * Main Entry Point - Auto-Discovery Mode
 *
 * This example demonstrates how to bootstrap the ORCHER SDK with auto-discovery.
 * Auto-discovery automatically scans and imports service, task, and workflow files.
 *
 * Key concepts:
 * - Auto-discovery is opt-in via .autoDiscover() method
 * - Scans files matching specified patterns
 * - Automatically imports files to trigger decorator registration
 * - Best for development and monolithic deployments
 * - For production/serverless, consider explicit imports (see main-explicit.ts)
 *
 * How it works:
 * 1. Worker.builder() creates a service builder instance
 * 2. .autoDiscover() enables automatic file scanning
 * 3. Files are scanned using fast-glob based on patterns
 * 4. Matching files are imported, triggering @Injectable(), @Tasks(), @Workflow() decorators
 * 5. Decorators register components in GlobalRegistry
 * 6. .build() pulls registered components from GlobalRegistry into the DI container
 * 7. .start() begins processing workflows and tasks
 */

async function main() {
  console.log("🚀 Starting ORCHER SDK Worker with Auto-Discovery...\n");

  try {
    // Build service with auto-discovery enabled
    const worker = await Worker.builder()
      // Server connection settings
      .serverUrl(process.env.ORCHER_SERVER_URL || "http://localhost:50051")
      .namespace(process.env.NAMESPACE || "default")
      .taskQueue(process.env.TASK_QUEUE || "payment-processing")

      // Polling configuration
      // Multi-slot FFI execution contexts (keyed by run_id) enable concurrent polling.
      // Higher poller counts increase throughput for concurrent workflow/task execution.
      .workflowPollerCount(
        parseInt(process.env.WORKFLOW_POLLER_COUNT || "4", 10),
      )
      .taskPollerCount(parseInt(process.env.TASK_POLLER_COUNT || "4", 10))
      .maxConcurrentWorkflows(50)
      .maxConcurrentTasks(100)

      // Enable auto-discovery
      // This will scan and import all JavaScript files from the dist folder
      .autoDiscover({
        // Patterns to include (relative to project root or cwd)
        patterns: [
          "./dist/services/**/*.js",
          "./dist/tasks/**/*.js",
          "./dist/workflows/**/*.js",
        ],
        // Patterns to exclude
        exclude: ["**/*.test.js", "**/*.spec.js", "**/__tests__/**"],
      })
      .build();

    // Log registered components
    console.log("📦 Registered Components:");
    const diStats = globalRegistry.getStats();
    console.log(`   - Services: ${diStats.services}`);
    console.log(`   - Task Handlers: ${diStats.taskHandlers}`);
    console.log(`   - Tasks: ${diStats.tasks}`);
    console.log(`   - Workflows: ${diStats.workflows}\n`);

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
