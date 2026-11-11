"""Benchmark test for durable-execution workflow using native client."""

import asyncio
import os
import sys
import time

# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import workflows and tasks to trigger registration
from orcher import Worker
from orcher._native import Client, ClientConfig

from tasks.distributed import (
    email_send,
    inventory_release,
    inventory_reserve,
    stripe_charge,
    stripe_refund,
)
from workflows.payment_distributed import payment_distributed_workflow


async def run_benchmark(num_workflows: int = 20, timeout_secs: int = 90):
    """Run a benchmark that submits workflows and measures completion."""

    # Build the worker
    worker = (
        Worker.builder()
        .server_url("http://localhost:50051")
        .namespace("default")
        .task_queue("payment-processing")
        .workflow_poller_count(4)
        .task_poller_count(4)
        .build()
    )

    # Create native client for submitting workflows
    config = ClientConfig(
        server_url="http://localhost:50051",
        namespace="default",
    )
    client = Client()
    await client.connect(config)

    print(f"Submitting {num_workflows} workflows...")
    print(f"Timeout: {timeout_secs} seconds")

    # Start worker in background
    worker_task = asyncio.create_task(worker.run())

    # Give worker time to connect
    await asyncio.sleep(2)

    start_time = time.time()
    submitted = 0
    completed = 0
    errors = 0

    try:
        # Submit workflows
        handles = []
        for i in range(num_workflows):
            try:
                handle = await client.start_workflow(
                    workflow_id=f"bench-payment-{i}-{int(time.time() * 1000)}",
                    workflow_type="paymentDistributedWorkflow2",
                    task_queue="payment-processing",
                    input={
                        "order_id": f"order-bench-{i}",
                        "amount": 9999,
                        "card_token": "tok_visa_4242",
                        "customer_email": f"bench{i}@example.com",
                    },
                )
                handles.append(handle)
                submitted += 1
            except Exception as e:
                print(f"Failed to submit workflow {i}: {e}")
                errors += 1

        print(f"Submitted {submitted} workflows")

        # Wait for workflows to complete - checking service stats
        deadline = time.time() + timeout_secs - 2  # Leave 2s for cleanup
        last_count = 0
        stall_count = 0

        while time.time() < deadline:
            stats = service.get_stats()
            current_count = stats.workflows_executed

            if current_count >= submitted:
                completed = current_count
                break

            if current_count == last_count:
                stall_count += 1
                if stall_count > 50:  # 5 seconds of no progress
                    print(f"Progress stalled at {current_count}/{submitted}")
                    break
            else:
                stall_count = 0

            last_count = current_count

            # Print progress every 2 seconds
            if int(time.time()) % 2 == 0 and current_count > 0:
                elapsed = time.time() - start_time
                print(
                    f"Progress: {current_count}/{submitted} completed in {elapsed:.1f}s"
                )

            await asyncio.sleep(0.1)

        completed = service.get_stats().workflows_executed

    finally:
        # Shutdown gracefully with longer timeout for durable workflows
        print("Shutting down service...")
        await service.shutdown(force=False, timeout_ms=10000)
        service_task.cancel()
        try:
            await service_task
        except asyncio.CancelledError:
            pass
        await client.close()

    elapsed = time.time() - start_time

    print(f"\n=== Benchmark Results ===")
    print(f"Duration: {elapsed:.2f}s")
    print(f"Submitted: {submitted}")
    print(f"Completed: {completed}")
    print(f"Errors: {errors}")
    print(f"Success rate: {(completed / submitted * 100) if submitted > 0 else 0:.1f}%")

    if elapsed > 0 and completed > 0:
        wf_per_sec = completed / elapsed
        print(f"Throughput: {wf_per_sec:.2f} workflows/second")
        print(f"Tasks/second: {wf_per_sec * 3:.2f} (3 tasks per workflow)")


if __name__ == "__main__":
    num_wf = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    asyncio.run(run_benchmark(num_workflows=num_wf, timeout_secs=timeout))
