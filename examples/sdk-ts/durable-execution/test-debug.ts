import 'reflect-metadata';
import { Client } from '@orcher/sdk';

async function main() {
  const client = new Client({
    serverUrl: 'http://localhost:50051',
    namespace: 'benchmark',
  });

  await client.connect();
  console.log('Connected to server');

  try {
    // Try with a simple string instead of object
    const handle = await client.startWorkflow({
      workflowType: 'simpleWorkflow',
      workflowId: `test-debug-${Date.now()}`,
      taskQueue: 'benchmark-queue',
      args: ["hello"],  // Simple string instead of object
    });

    console.log(`Workflow started: ${handle.workflowId}`);
    const result = await handle.result();
    console.log('Workflow result:', result);
  } catch (error) {
    console.error('Error:', error);
  }

  await client.close();
}

main().catch(console.error);
