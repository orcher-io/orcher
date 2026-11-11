import "reflect-metadata";
import { Client } from "@orcher/sdk";

interface PaymentDistributedInput {
  orderId: string;
  amount: number;
  cardToken: string;
  customerEmail: string;
}

interface PaymentDistributedOutput {
  orderId: string;
  chargeId: string;
  reservationId: string;
  notificationId: string;
  status: "completed" | "failed";
  apiCalls: {
    stripeCount: number;
    inventoryCount: number;
    emailCount: number;
  };
  error?: string;
}

const NUM_WORKFLOWS = parseInt(process.env.NUM_WORKFLOWS || "10", 10);
const SERVER_URL = process.env.ORCHER_SERVER_URL || "http://localhost:50051";
const TASK_QUEUE = process.env.TASK_QUEUE || "payment-processing";
const NAMESPACE = process.env.NAMESPACE || "default";

async function main() {
  console.log("╔════════════════════════════════════════════════════════════╗");
  console.log(
    "║     ORCHER TypeScript SDK - Load Test                       ║",
  );
  console.log(
    "╚════════════════════════════════════════════════════════════╝\n",
  );

  console.log(`Configuration:`);
  console.log(`  - Number of workflows: ${NUM_WORKFLOWS}`);
  console.log(`  - Server URL: ${SERVER_URL}`);
  console.log(`  - Namespace: ${NAMESPACE}`);
  console.log(`  - Task Queue: ${TASK_QUEUE}\n`);

  const startTime = Date.now();

  const client = new Client({
    serverUrl: SERVER_URL,
    namespace: NAMESPACE,
  });

  await client.connect();
  console.log("📡 Connected to ORCHER server\n");

  const workflowStartTimes: number[] = [];
  const workflowEndTimes: number[] = [];
  const promises: Promise<PaymentDistributedOutput>[] = [];

  console.log("🚀 Starting workflows...\n");

  for (let i = 0; i < NUM_WORKFLOWS; i++) {
    const orderId = `order-${Date.now()}-${i}`;
    const input: PaymentDistributedInput = {
      orderId,
      amount: 9999,
      cardToken: "tok_visa_4242",
      customerEmail: `test${i}@example.com`,
    };

    const workflowStartTime = Date.now();
    workflowStartTimes.push(workflowStartTime);

    const p = client
      .startWorkflow({
        workflowType: "paymentDistributedWorkflow2",
        workflowId: `load-test-${Date.now()}-${i}`,
        taskQueue: TASK_QUEUE,
        args: [input],
      })
      .then(async (handle) => {
        console.log(
          `  ✓ Workflow ${i + 1}/${NUM_WORKFLOWS} started: ${handle.workflowId}`,
        );
        const result = (await handle.result()) as PaymentDistributedOutput;
        workflowEndTimes.push(Date.now());
        console.log(
          `  ✓ Workflow ${i + 1}/${NUM_WORKFLOWS} completed: ${result.status}`,
        );
        return result;
      })
      .catch((error) => {
        workflowEndTimes.push(Date.now());
        console.error(
          `  ✗ Workflow ${i + 1}/${NUM_WORKFLOWS} failed:`,
          error.message,
        );
        return {
          orderId,
          chargeId: "",
          reservationId: "",
          notificationId: "",
          status: "failed" as const,
          apiCalls: { stripeCount: 0, inventoryCount: 0, emailCount: 0 },
          error: error.message,
        };
      });

    promises.push(p);
  }

  const results = await Promise.all(promises);
  const endTime = Date.now();
  const duration = endTime - startTime;

  const completed = results.filter((r) => r.status === "completed").length;
  const failed = results.filter((r) => r.status === "failed").length;

  // Calculate individual workflow times
  const workflowDurations = workflowStartTimes.map(
    (start, i) => (workflowEndTimes[i] ?? Date.now()) - start,
  );
  const avgWorkflowDuration =
    workflowDurations.length > 0
      ? workflowDurations.reduce((a, b) => a + b, 0) / workflowDurations.length
      : 0;
  const minWorkflowDuration = Math.min(...workflowDurations);
  const maxWorkflowDuration = Math.max(...workflowDurations);

  // Sort for percentile calculation
  const sortedDurations = [...workflowDurations].sort((a, b) => a - b);
  const p50 = sortedDurations[Math.floor(sortedDurations.length * 0.5)] || 0;
  const p95 = sortedDurations[Math.floor(sortedDurations.length * 0.95)] || 0;
  const p99 = sortedDurations[Math.floor(sortedDurations.length * 0.99)] || 0;

  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("                  LOAD TEST RESULTS                         ");
  console.log("═══════════════════════════════════════════════════════════\n");

  console.log("📊 Summary:");
  console.log(`   Total workflows:     ${NUM_WORKFLOWS}`);
  console.log(`   Completed:           ${completed}`);
  console.log(`   Failed:              ${failed}`);
  console.log(
    `   Success rate:        ${((completed / NUM_WORKFLOWS) * 100).toFixed(1)}%\n`,
  );

  console.log("⏱️  Timing:");
  console.log(`   Total test time:     ${duration}ms`);
  console.log(`   Avg workflow time:   ${avgWorkflowDuration.toFixed(2)}ms`);
  console.log(`   Min workflow time:   ${minWorkflowDuration}ms`);
  console.log(`   Max workflow time:   ${maxWorkflowDuration}ms\n`);

  console.log("📈 Percentiles:");
  console.log(`   P50:                 ${p50}ms`);
  console.log(`   P95:                 ${p95}ms`);
  console.log(`   P99:                 ${p99}ms\n`);

  console.log("🚀 Throughput:");
  console.log(
    `   Workflows/sec:       ${(NUM_WORKFLOWS / (duration / 1000)).toFixed(2)}`,
  );
  console.log(
    `   Tasks/sec:           ${((NUM_WORKFLOWS * 3) / (duration / 1000)).toFixed(2)}`,
  );
  console.log("   (3 tasks per workflow: charge, reserve, email)\n");

  console.log("═══════════════════════════════════════════════════════════\n");

  if (completed === NUM_WORKFLOWS) {
    console.log(
      "🎉 Load test PASSED - All workflows completed successfully!\n",
    );
  } else {
    console.log(`⚠️  Load test completed with ${failed} failures\n`);
  }

  await client.close();
}

main().catch((error) => {
  console.error("❌ Fatal error:", error);
  process.exit(1);
});
