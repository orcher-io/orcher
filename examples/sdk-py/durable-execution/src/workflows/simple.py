"""
Simple Workflow Example

This is a basic workflow that demonstrates the workflow structure
with ctx.execute() for inline deterministic operations.
"""

from datetime import datetime
from typing import Any

from orcher import WorkflowContext, workflow


async def fetch_current_time() -> str:
    """Simulated external API call to get current time."""
    return datetime.now().isoformat()


async def compute_greeting(name: str) -> str:
    """Simulated computation that should be deterministic on replay."""
    return f"Hello, {name}! Welcome to ORCHER."


@workflow(name="simpleWorkflow", version="1.0")
async def simple_workflow(
    ctx: WorkflowContext, input: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    A simple workflow demonstrating ctx.execute() for inline operations.

    This demonstrates:
    - Basic workflow structure
    - Using the @workflow decorator on a function
    - Using ctx.execute() for one-off deterministic operations
    - Accessing workflow context
    - Returning results

    ctx.execute() is used for:
    - One-off API calls that shouldn't be separate tasks
    - Database queries
    - Any side-effect that should be recorded in the journal

    Args:
        ctx: The workflow context providing access to workflow operations.
        input: The input data passed when starting the workflow.

    Returns:
        A dict containing the result of the workflow.
    """
    input = input or {}
    name = input.get("name", "World")

    # Use ctx.execute() for inline deterministic operations
    # These are recorded in the journal and replayed on recovery
    timestamp = await ctx.execute("get_timestamp", lambda: fetch_current_time())

    greeting = await ctx.execute("compute_greeting", lambda: compute_greeting(name))

    return {
        "message": "Workflow completed successfully",
        "greeting": greeting,
        "timestamp": timestamp,
        "input": input,
        "workflow_id": ctx.info.workflow_id,
        "run_id": ctx.info.run_id,
    }
