# ORCHER Python SDK Examples

This directory contains examples demonstrating the ORCHER Python SDK for durable workflow execution.

## Structure

```
durable-execution/
├── pyproject.toml             # Python project configuration
├── README.md                  # This file
└── src/
    ├── __init__.py
    ├── main.py                # Service worker entry point
    ├── load_test.py           # Load testing script
    ├── workflows/
    │   ├── __init__.py
    │   ├── simple.py          # Simple workflow example
    │   ├── payment.py         # Payment workflow example
    │   └── payment_distributed.py # Distributed payment workflow (saga pattern)
    ├── tasks/
    │   ├── __init__.py
    │   ├── payment.py         # Payment-related tasks
    │   └── distributed.py     # Tasks for distributed workflow
    └── services/
        └── __init__.py        # (Future) Service implementations
```

## Prerequisites

1. Install the ORCHER Python SDK:
   ```bash
   # Create virtual environment
   cd examples/sdk-py/durable-execution
   python3 -m venv .venv
   source .venv/bin/activate
   
   # Install from wheel (after building with maturin)
   pip install ../../../../sdk-py/target/wheels/orcher-*.whl
   
   # Or install in development mode
   pip install -e ../../../../sdk-py
   ```

2. Start the ORCHER server:
   ```bash
   # From the project root
   cargo run --release --bin orchestrator -- --config services/orchestrator/config/benchmark.toml
   ```

## Running the Service Worker

The service worker polls the ORCHER server for workflow and task executions:

```bash
# Activate virtual environment
source .venv/bin/activate

# Default configuration
python -m src.main

# Custom configuration via environment variables
ORCHER_SERVER_URL=http://localhost:50051 \
NAMESPACE=default \
TASK_QUEUE=payment-processing \
WORKFLOW_POLLER_COUNT=4 \
TASK_POLLER_COUNT=4 \
python -m src.main
```

## Running the Load Test

The load test starts multiple workflows concurrently and measures throughput:

```bash
# Activate virtual environment
source .venv/bin/activate

# Default: 10 workflows
python -m src.load_test

# Custom number of workflows
NUM_WORKFLOWS=100 python -m src.load_test

# Full configuration
NUM_WORKFLOWS=50 \
ORCHER_SERVER_URL=http://localhost:50051 \
NAMESPACE=default \
TASK_QUEUE=payment-processing \
python -m src.load_test
```

## Running with the Benchmark Script

The unified benchmark script supports Python SDK testing:

```bash
# From project root
./scripts/benchmark.sh --sdk python --workers 1,2

# Test all SDKs
./scripts/benchmark.sh --sdk all --workers 1,2

# Quick test
./scripts/benchmark.sh --sdk python --quick
```

## Workflows

### SimpleWorkflow

A basic workflow that returns a result without executing any tasks:

```python
@workflow(name="simpleWorkflow", version="1.0")
class SimpleWorkflow:
    async def run(self, ctx: WorkflowContext, input: dict) -> dict:
        return {
            "message": "Workflow completed",
            "input": input,
            "workflow_id": ctx.info.workflow_id,
        }
```

### PaymentWorkflow

Processes a payment with card charging and notification:

```python
@workflow(name="paymentWorkflow", version="1.0")
class PaymentWorkflow:
    async def run(self, ctx, order_id, amount, card_token, customer_email):
        # Execute tasks sequentially
        charge = await ctx.execute_task(PaymentTasks.charge_card, ...)
        notification = await ctx.execute_task(PaymentTasks.send_notification, ...)
        return {"status": "completed", ...}
```

### PaymentDistributedWorkflow

Demonstrates the saga pattern for distributed transactions:

```python
@workflow(name="paymentDistributedWorkflow2", version="1.0")
class PaymentDistributedWorkflow:
    async def run(self, ctx, order_id, amount, card_token, customer_email):
        # Step 1: Charge card
        charge = await ctx.execute_task(DistributedTasks.stripe_charge, ...)
        
        # Step 2: Reserve inventory (with compensation on failure)
        try:
            reservation = await ctx.execute_task(DistributedTasks.inventory_reserve, ...)
        except Exception:
            # Compensation: Refund the charge
            await ctx.execute_task(DistributedTasks.stripe_refund, charge.charge_id)
            raise
        
        # Step 3: Send confirmation email
        email = await ctx.execute_task(DistributedTasks.email_send, ...)
        
        return {"status": "completed", ...}
```

## Tasks

Tasks are defined using the `@task_handler` and `@task` decorators:

```python
@task_handler
class PaymentTasks:
    @task(name="chargeCard", timeout=30.0)
    async def charge_card(self, ctx: TaskContext, amount: int, token: str, ...) -> ChargeResult:
        # Process payment via external API
        return ChargeResult(charge_id="...", amount=amount, status="succeeded")
    
    @task(name="sendPaymentNotification", timeout=20.0)
    async def send_notification(self, ctx: TaskContext, email: str, ...) -> NotificationResult:
        # Send email notification
        return NotificationResult(message_id="...", sent=True)
```

## Comparing with TypeScript SDK

The Python SDK follows the same patterns as the TypeScript SDK:

| TypeScript | Python |
|------------|--------|
| `@Workflow({ name: "..." })` | `@workflow(name="...")` |
| `@Tasks()` | `@task_handler` |
| `@Task({ name: "..." })` | `@task(name="...")` |
| `Service.builder()...build()` | `Service.builder()...build()` |
| `client.startWorkflow(...)` | `client.start_workflow(...)` |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ORCHER_SERVER_URL` | ORCHER server URL | `http://localhost:50051` |
| `NAMESPACE` | Namespace for workflows | `default` |
| `TASK_QUEUE` | Task queue to poll | `payment-processing` |
| `WORKFLOW_POLLER_COUNT` | Number of workflow pollers | `4` |
| `TASK_POLLER_COUNT` | Number of task pollers | `4` |
| `NUM_WORKFLOWS` | Number of workflows (load test) | `10` |
