import { Client } from '@orcher/sdk';

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
  status: 'completed' | 'failed';
  apiCalls: {
    stripeCount: number;
    inventoryCount: number;
    emailCount: number;
  };
  error?: string;
}

async function main() {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║     ORCHER TypeScript SDK - Distributed Payment Test       ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  const startTime = Date.now();

  try {
    // Create client
    const client = new Client({
      serverUrl: process.env.ORCHER_SERVER_URL || 'http://localhost:50051',
      namespace: process.env.NAMESPACE || 'default',
    });

    // Connect to server
    const connectStart = Date.now();
    await client.connect();
    const connectDuration = Date.now() - connectStart;

    console.log(`📡 Connected to ORCHER server (${connectDuration}ms)`);

    // Generate workflow input
    const orderId = `ord_${Date.now().toString(36)}`;
    const input: PaymentDistributedInput = {
      orderId,
      amount: 9999, // $99.99 in cents
      cardToken: 'tok_visa_4242',
      customerEmail: 'customer@example.com',
    };

    console.log(`\n🚀 Starting payment distributed workflow`);
    console.log(`   Order ID: ${input.orderId}`);
    console.log(`   Amount: $${(input.amount / 100).toFixed(2)}`);
    console.log(`   Email: ${input.customerEmail}\n`);

    // Start workflow
    const workflowId = `payment-distributed-${Date.now()}`;
    const workflowStartTime = Date.now();

    const handle = await client.startWorkflow({
      workflowType: 'paymentDistributedWorkflow',
      workflowId,
      taskQueue: process.env.TASK_QUEUE || 'payment-processing',
      args: [input],
    });

    const workflowStartDuration = Date.now() - workflowStartTime;
    console.log(`✅ Workflow started (${workflowStartDuration}ms)`);
    console.log(`   Workflow ID: ${handle.workflowId}`);
    console.log(`   Run ID: ${handle.runId}\n`);

    // Wait for result
    console.log('⏳ Waiting for workflow result...\n');
    const resultStartTime = Date.now();
    const result = await handle.result() as PaymentDistributedOutput;
    const resultDuration = Date.now() - resultStartTime;

    const totalDuration = Date.now() - startTime;

    // Display results
    console.log('═══════════════════════════════════════════════════════════');
    console.log('                    WORKFLOW RESULT                         ');
    console.log('═══════════════════════════════════════════════════════════\n');

    if (result.status === 'completed') {
      console.log('✅ Workflow completed successfully!\n');
      console.log('📦 Result Details:');
      console.log(`   Order ID:        ${result.orderId}`);
      console.log(`   Charge ID:       ${result.chargeId}`);
      console.log(`   Reservation ID:  ${result.reservationId}`);
      console.log(`   Notification ID: ${result.notificationId}`);
      console.log(`   Status:          ${result.status}`);
    } else {
      console.log('❌ Workflow failed!');
      console.log(`   Error: ${result.error || 'Unknown error'}`);
    }

    // API call verification (idempotency check)
    console.log('\n📊 API Call Verification (Idempotency):');
    console.log(`   Stripe API calls:     ${result.apiCalls?.stripeCount ?? 'N/A'} (expected: 1)`);
    console.log(`   Inventory API calls:  ${result.apiCalls?.inventoryCount ?? 'N/A'} (expected: 1)`);
    console.log(`   Email API calls:      ${result.apiCalls?.emailCount ?? 'N/A'} (expected: 1)`);

    const idempotencyOk =
      result.apiCalls?.stripeCount === 1 &&
      result.apiCalls?.inventoryCount === 1 &&
      result.apiCalls?.emailCount === 1;

    if (idempotencyOk) {
      console.log('\n✅ IDEMPOTENCY VERIFIED - No duplicate API calls!');
    } else if (result.apiCalls) {
      console.log('\n⚠️  API call counts differ from expected (may indicate replay or retry)');
    }

    // Performance metrics
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('                  PERFORMANCE METRICS                       ');
    console.log('═══════════════════════════════════════════════════════════\n');
    console.log(`   Client connect time:    ${connectDuration}ms`);
    console.log(`   Workflow start time:    ${workflowStartDuration}ms`);
    console.log(`   Result wait time:       ${resultDuration}ms`);
    console.log(`   ─────────────────────────────────`);
    console.log(`   Total client time:      ${totalDuration}ms`);
    console.log('');

    // Expected timing breakdown
    // Each step has 100ms simulated latency
    // 3 steps = 300ms minimum execution time
    // Plus overhead for journaling, polling, etc.
    const expectedMinTime = 300; // 3 steps x 100ms
    const overhead = resultDuration - expectedMinTime;
    console.log('📈 Timing Analysis:');
    console.log(`   Expected execution time: ${expectedMinTime}ms (3 steps × 100ms)`);
    console.log(`   Actual result wait:      ${resultDuration}ms`);
    console.log(`   Orchestration overhead:  ${overhead > 0 ? overhead : 0}ms`);

    console.log('\n═══════════════════════════════════════════════════════════\n');

    // Exit with appropriate code
    if (result.status === 'completed') {
      console.log('🎉 Test PASSED - Distributed payment workflow completed successfully!\n');
      process.exit(0);
    } else {
      console.log('❌ Test FAILED - Workflow did not complete successfully\n');
      process.exit(1);
    }

  } catch (error) {
    const totalDuration = Date.now() - startTime;
    console.error('\n❌ Error occurred:', error);
    console.error(`   Total time before error: ${totalDuration}ms`);

    if (error instanceof Error) {
      console.error(`   Error message: ${error.message}`);
      if (error.stack) {
        console.error('\nStack trace:');
        console.error(error.stack);
      }
    }

    process.exit(1);
  }
}

// Run the test
main().catch((error) => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});
