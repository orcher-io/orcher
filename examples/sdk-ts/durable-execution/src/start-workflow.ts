import { Client } from '@orcher/sdk';

async function main() {
  console.log('🚀 Starting workflow client...\n');

  try {
    // Create client
    const client = new Client({
      serverUrl: process.env.ORCHER_SERVER_URL || 'http://localhost:50051',
      namespace: process.env.NAMESPACE || 'default',
    });

    // Connect to server
    await client.connect();

    console.log('📡 Connected to ORCHER server');

    // Start workflow
    const workflowId = `simple-${Date.now()}`;
    console.log(`\n🔄 Starting workflow: ${workflowId}`);

    const handle = await client.startWorkflow({
      workflowType: 'simpleWorkflow',
      workflowId,
      taskQueue: process.env.TASK_QUEUE || 'payment-processing',
      args: [{
        message: 'Hello from TypeScript client!',
        timestamp: new Date().toISOString(),
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
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  }
}

main().catch((error) => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});
