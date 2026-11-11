import "reflect-metadata";
import { Worker, globalRegistry } from "@orcher/sdk";

/**
 * Main Entry Point - Distributed Tasks Mode
 *
 * This worker registers the distributed tasks and workflow for testing
 * ctx.executeTask() which should create proper task records like Rust SDK.
 */

// Import services (same as before)
import "./services/config.service";
import "./services/logger.service";
import "./services/stripe.service";
import "./services/email.service";

// Import DISTRIBUTED task handlers (uses ctx.executeTask)
import "./tasks/distributed.tasks";

// Import DISTRIBUTED workflow (uses ctx.executeTask)
import "./workflows/payment-distributed.workflow";

async function main() {
  console.log("🚀 Starting ORCHER SDK Worker with DISTRIBUTED Tasks...\n");

  try {
    const worker = await Worker.builder()
      .serverUrl(process.env.ORCHER_SERVER_URL || "http://localhost:50051")
      .namespace(process.env.NAMESPACE || "default")
      .taskQueue(process.env.TASK_QUEUE || "payment-processing")
      .build();

    console.log("📦 Registered Components:");
    const diStats = globalRegistry.getStats();
    console.log(`   - Services: ${diStats.services}`);
    console.log(`   - Task Handlers: ${diStats.taskHandlers}`);
    console.log(`   - Tasks: ${diStats.tasks}`);
    console.log(`   - Workflows: ${diStats.workflows}\n`);

    await worker.run();

    console.log("✅ Worker started successfully!");
    console.log("📡 Listening for workflows and tasks...\n");
    console.log("💡 Press Ctrl+C to shutdown gracefully\n");

    const shutdown = async (signal: string) => {
      console.log(`\n🛑 Received ${signal}, shutting down gracefully...`);
      await worker.shutdown();
      console.log("✅ Worker stopped successfully");
      process.exit(0);
    };

    process.on("SIGINT", () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));

    await new Promise(() => {});
  } catch (error) {
    console.error("❌ Failed to start worker:", error);
    process.exit(1);
  }
}

main().catch((error) => {
  console.error("❌ Fatal error:", error);
  process.exit(1);
});
