#!/usr/bin/env python3
"""
Load Test - ORCHER Python SDK

This script runs a load test against the ORCHER server by starting
multiple workflows concurrently and measuring throughput and latency.

Similar to the TypeScript load-test.ts, this allows benchmarking
the Python SDK alongside the TypeScript SDK.

Usage:
    python load_test.py

Environment variables:
    NUM_WORKFLOWS        - Number of workflows to run (default: 10)
    ORCHER_SERVER_URL    - Server URL (default: http://localhost:50051)
    NAMESPACE            - Namespace (default: default)
    TASK_QUEUE           - Task queue (default: payment-processing)
"""

import asyncio
import os
import statistics
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Use native client directly for actual server communication
from orcher._native import Client, ClientConfig


@dataclass
class PaymentDistributedInput:
    """Input for the distributed payment workflow."""

    order_id: str
    amount: int
    card_token: str
    customer_email: str


@dataclass
class WorkflowResult:
    """Result from a workflow execution."""

    workflow_id: str
    order_id: str
    status: str
    duration_ms: float
    charge_id: Optional[str] = None
    reservation_id: Optional[str] = None
    notification_id: Optional[str] = None
    error: Optional[str] = None


async def run_workflow(
    client: "Client",
    workflow_num: int,
    total_workflows: int,
    task_queue: str,
) -> WorkflowResult:
    """Run a single workflow and return the result."""

    order_id = f"order-{int(time.time() * 1000)}-{workflow_num}"
    workflow_id = f"load-test-py-{int(time.time() * 1000)}-{workflow_num}"

    start_time = time.time()

    try:
        # Start the workflow (native client API)
        handle = await client.start_workflow(
            workflow_id=workflow_id,
            workflow_type="paymentDistributedWorkflow2",
            task_queue=task_queue,
            input={
                "order_id": order_id,
                "amount": 9999,
                "card_token": "tok_visa_4242",
                "customer_email": f"test{workflow_num}@example.com",
            },
        )

        print(
            f"  [OK] Workflow {workflow_num + 1}/{total_workflows} started: {workflow_id}"
        )

        # Wait for result
        result = await handle.result()

        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000

        print(
            f"  [OK] Workflow {workflow_num + 1}/{total_workflows} completed: {result.get('status', 'unknown')}"
        )

        return WorkflowResult(
            workflow_id=workflow_id,
            order_id=order_id,
            status=result.get("status", "completed"),
            duration_ms=duration_ms,
            charge_id=result.get("charge_id"),
            reservation_id=result.get("reservation_id"),
            notification_id=result.get("notification_id"),
        )

    except Exception as e:
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000

        print(f"  [FAIL] Workflow {workflow_num + 1}/{total_workflows} failed: {e}")

        return WorkflowResult(
            workflow_id=workflow_id,
            order_id=order_id,
            status="failed",
            duration_ms=duration_ms,
            error=str(e),
        )


async def main():
    """Main entry point for the load test."""

    print("=" * 60)
    print("     ORCHER Python SDK - Load Test")
    print("=" * 60)
    print()

    # Get configuration
    num_workflows = int(os.environ.get("NUM_WORKFLOWS", "10"))
    server_url = os.environ.get("ORCHER_SERVER_URL", "http://localhost:50051")
    namespace = os.environ.get("NAMESPACE", "default")
    task_queue = os.environ.get("TASK_QUEUE", "payment-processing")

    print("Configuration:")
    print(f"  Number of workflows: {num_workflows}")
    print(f"  Server URL:          {server_url}")
    print(f"  Namespace:           {namespace}")
    print(f"  Task Queue:          {task_queue}")
    print()

    # Create client (native client has different API)
    config = ClientConfig(
        server_url=server_url,
        namespace=namespace,
    )
    client = Client()

    try:
        await client.connect(config)
        print("Connected to ORCHER server")
        print()
    except Exception as e:
        print(f"Failed to connect to server: {e}")
        print()
        print("Make sure the ORCHER server is running at", server_url)
        sys.exit(1)

    print("Starting workflows...")
    print()

    start_time = time.time()

    # Start all workflows concurrently
    tasks = [
        run_workflow(client, i, num_workflows, task_queue) for i in range(num_workflows)
    ]

    results: List[WorkflowResult] = await asyncio.gather(*tasks)

    end_time = time.time()
    total_duration_ms = (end_time - start_time) * 1000

    # Calculate statistics
    completed = [r for r in results if r.status == "completed"]
    failed = [r for r in results if r.status == "failed"]

    durations = [r.duration_ms for r in results]
    sorted_durations = sorted(durations)

    avg_duration = statistics.mean(durations) if durations else 0
    min_duration = min(durations) if durations else 0
    max_duration = max(durations) if durations else 0

    # Calculate percentiles
    def percentile(data: List[float], p: float) -> float:
        if not data:
            return 0
        idx = int(len(data) * p)
        return data[min(idx, len(data) - 1)]

    p50 = percentile(sorted_durations, 0.50)
    p95 = percentile(sorted_durations, 0.95)
    p99 = percentile(sorted_durations, 0.99)

    # Print results
    print()
    print("=" * 60)
    print("                  LOAD TEST RESULTS")
    print("=" * 60)
    print()

    print("Summary:")
    print(f"   Total workflows:     {num_workflows}")
    print(f"   Completed:           {len(completed)}")
    print(f"   Failed:              {len(failed)}")
    print(f"   Success rate:        {(len(completed) / num_workflows * 100):.1f}%")
    print()

    print("Timing:")
    print(f"   Total test time:     {total_duration_ms:.2f}ms")
    print(f"   Avg workflow time:   {avg_duration:.2f}ms")
    print(f"   Min workflow time:   {min_duration:.2f}ms")
    print(f"   Max workflow time:   {max_duration:.2f}ms")
    print()

    print("Percentiles:")
    print(f"   P50:                 {p50:.2f}ms")
    print(f"   P95:                 {p95:.2f}ms")
    print(f"   P99:                 {p99:.2f}ms")
    print()

    throughput_wf = (
        num_workflows / (total_duration_ms / 1000) if total_duration_ms > 0 else 0
    )
    throughput_tasks = (
        (num_workflows * 3) / (total_duration_ms / 1000) if total_duration_ms > 0 else 0
    )

    print("Throughput:")
    print(f"   Workflows/sec:       {throughput_wf:.2f}")
    print(f"   Tasks/sec:           {throughput_tasks:.2f}")
    print("   (3 tasks per workflow: charge, reserve, email)")
    print()

    print("=" * 60)
    print()

    if len(completed) == num_workflows:
        print("Load test PASSED - All workflows completed successfully!")
    else:
        print(f"Load test completed with {len(failed)} failures")
        if failed:
            print()
            print("Failed workflows:")
            for r in failed[:5]:  # Show first 5 failures
                print(f"  - {r.workflow_id}: {r.error}")
            if len(failed) > 5:
                print(f"  ... and {len(failed) - 5} more")

    print()

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
