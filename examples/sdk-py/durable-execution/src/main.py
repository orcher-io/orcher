#!/usr/bin/env python3
"""
Main Entry Point - ORCHER Python SDK Worker

This example demonstrates how to bootstrap the ORCHER SDK worker
with workflow and task handlers.

Key concepts:
- @workflow and @task decorators register handlers at import time
- Worker.builder() creates a worker builder instance
- Handlers are auto-discovered from GlobalRegistry
- .build() creates the worker with registered components
- .run() begins processing workflows and tasks

Usage:
    python -m src.main

Environment variables:
    ORCHER_SERVER_URL    - Server URL (default: http://localhost:50051)
    NAMESPACE            - Namespace (default: default)
    TASK_QUEUE           - Task queue (default: payment-processing)
    WORKFLOW_POLLER_COUNT - Number of workflow pollers (default: 4)
    TASK_POLLER_COUNT    - Number of task pollers (default: 4)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from typing import TYPE_CHECKING

# Add src directory to path for module imports when running as python -m src.main
src_dir = os.path.dirname(os.path.abspath(__file__))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Import the SDK
from orcher import Worker  # noqa: E402, I001
from orcher.decorators import GlobalRegistry  # noqa: E402

# Import workflows and tasks to trigger decorator registration
# These imports populate the GlobalRegistry automatically
# The imports appear "unused" but are required for side-effect registration
from tasks.distributed import (  # noqa: E402, F401
    email_send,
    inventory_release,
    inventory_reserve,
    stripe_charge,
    stripe_refund,
)
from tasks.payment import (  # noqa: E402, F401
    charge_card,
    refund_charge,
    send_payment_notification,
    verify_charge_status,
)
from workflows.payment import payment_workflow  # noqa: E402, F401
from workflows.payment_distributed import (  # noqa: E402, F401
    payment_distributed_workflow,
)
from workflows.simple import simple_workflow  # noqa: E402, F401

if TYPE_CHECKING:
    # Prevent "unused import" warnings while keeping type hints available
    _ = (
        email_send,
        inventory_release,
        inventory_reserve,
        stripe_charge,
        stripe_refund,
        charge_card,
        refund_charge,
        send_payment_notification,
        verify_charge_status,
        payment_workflow,
        payment_distributed_workflow,
        simple_workflow,
    )


async def main() -> None:
    """Main entry point for the ORCHER Python SDK worker."""

    print("=" * 60)
    print("     ORCHER Python SDK - Durable Execution Worker")
    print("=" * 60)
    print()

    # Get configuration from environment
    server_url = os.environ.get("ORCHER_SERVER_URL", "http://localhost:50051")
    namespace = os.environ.get("NAMESPACE", "default")
    task_queue = os.environ.get("TASK_QUEUE", "payment-processing")
    workflow_poller_count = int(os.environ.get("WORKFLOW_POLLER_COUNT", "4"))
    task_poller_count = int(os.environ.get("TASK_POLLER_COUNT", "4"))

    print("Configuration:")
    print(f"  Server URL:             {server_url}")
    print(f"  Namespace:              {namespace}")
    print(f"  Task Queue:             {task_queue}")
    print(f"  Workflow Poller Count:  {workflow_poller_count}")
    print(f"  Task Poller Count:      {task_poller_count}")
    print()

    try:
        # Build the worker
        worker = (
            Worker.builder()
            .server_url(server_url)
            .namespace(namespace)
            .task_queue(task_queue)
            .workflow_poller_count(workflow_poller_count)
            .task_poller_count(task_poller_count)
            .max_concurrent_workflows(50)
            .max_concurrent_tasks(100)
            .build()
        )

        # Log registered components
        registry = GlobalRegistry.get_instance()
        print("Registered Components:")
        print(f"  Workflows: {registry.workflow_count}")
        for wf in registry.list_workflows():
            fn_name = getattr(wf.handler, "__name__", str(wf.handler))
            print(f"    - {wf.name} (v{wf.version}) -> {fn_name}")
        print(f"  Tasks: {registry.task_count}")
        for task in registry.list_tasks():
            fn_name = getattr(task.handler, "__name__", str(task.handler))
            print(f"    - {task.name} -> {fn_name}")
        print()

        # Setup graceful shutdown
        shutdown_event = asyncio.Event()

        def signal_handler(sig: signal.Signals) -> None:
            logger.info(f"Received signal {sig}, initiating shutdown...")
            shutdown_event.set()

        # Register signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))

        print("Starting worker...")
        print("Press Ctrl+C to shutdown gracefully")
        print()

        # Run the worker
        # The service will poll for work and execute workflows/tasks
        async with worker:
            # Wait for shutdown signal
            await shutdown_event.wait()

            print()
            print("Shutting down gracefully...")
            await worker.shutdown()

        print("Worker stopped successfully")

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        logger.exception(f"Failed to run worker: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
