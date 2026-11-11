import { Client } from '@orcher/sdk';

async function main() {
  console.log('🚀 Starting payment workflow client...\n');

  try {
    // Create client
    const client = new Client({
      serverUrl: process.env.ORCHER_SERVER_URL || 'http://localhost:50051',
      namespace: process.env.NAMESPACE || 'default',
    });

    // Connect to server
    await client.connect();

    console.log('📡 Connected to ORCHER server');

    // Start payment workflow
    const workflowId = `payment-${Date.now()}`;
    console.log(`\n🔄 Starting payment workflow: ${workflowId}`);

    const handle = await client.startWorkflow({
      workflowType: 'paymentWorkflow',
      workflowId,
      taskQueue: process.env.TASK_QUEUE || 'payment-processing',
      args: [{
        orderId: `order-${Date.now()}`,
        customerId: 'cust-12345',
        amount: 99.99,
        cardToken: 'tok_visa_test',
        customerEmail: 'test@example.com',
      }],
    });

    console.log(`✅ Workflow started successfully!`);
    console.log(`   Workflow ID: ${handle.workflowId}`);
    console.log(`   Run ID: ${handle.runId}\n`);

    // Wait for result
    console.log('⏳ Waiting for workflow result...\n');
    const result = await handle.result();

    console.log('✅ Workflow completed!');
    console.log('📦 Result:', JSON.stringify(result, null, 2));

    // Validate result structure
    if (result && typeof result === 'object') {
      const paymentResult = result as {
        orderId?: string;
        chargeId?: string;
        status?: string;
        notificationSent?: boolean;
        error?: string;
      };

      console.log('\n📊 Result Validation:');
      console.log(`   Order ID: ${paymentResult.orderId || 'N/A'}`);
      console.log(`   Charge ID: ${paymentResult.chargeId || 'N/A'}`);
      console.log(`   Status: ${paymentResult.status || 'N/A'}`);
      console.log(`   Notification Sent: ${paymentResult.notificationSent ?? 'N/A'}`);
      if (paymentResult.error) {
        console.log(`   Error: ${paymentResult.error}`);
      }
    }
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  }
}

main().catch((error) => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});
