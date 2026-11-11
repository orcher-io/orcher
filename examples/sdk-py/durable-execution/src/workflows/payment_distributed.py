"""
Payment Distributed Workflow Example

This workflow demonstrates a more complex payment flow that coordinates
multiple microservices (payment, inventory, notification) with proper
error handling and compensation using ctx.execute_task() for registered tasks.
"""

from dataclasses import dataclass
from typing import Any

# Import task functions for ctx.execute_task()
from tasks.distributed import (
    email_send,
    inventory_release,
    inventory_reserve,
    stripe_charge,
    stripe_refund,
)

from orcher import WorkflowContext, workflow


@dataclass
class PaymentDistributedInput:
    """Input for the distributed payment workflow."""

    order_id: str
    amount: int
    card_token: str
    customer_email: str


@dataclass
class PaymentDistributedResult:
    """Result of the distributed payment workflow."""

    order_id: str
    charge_id: str
    reservation_id: str
    notification_id: str
    status: str
    api_calls: dict[str, int]
    error: str | None = None


@workflow(name="paymentDistributedWorkflow2", version="1.0")
async def payment_distributed_workflow(
    ctx: WorkflowContext,
    order_id: str,
    amount: int,
    card_token: str,
    customer_email: str,
) -> dict[str, Any]:
    """
    Distributed payment workflow coordinating multiple services.

    This workflow demonstrates:
    - Coordinating multiple microservices using ctx.execute_task()
    - Saga pattern for distributed transactions
    - Compensation on failure
    - Tracking API call counts

    Flow:
    1. Charge customer via Stripe
    2. Reserve inventory
    3. Send confirmation email

    Compensation (on failure):
    - If inventory fails: Refund the charge
    - If email fails: Release inventory + refund

    This pattern ensures data consistency across services
    without requiring distributed transactions.

    Args:
        ctx: Workflow context
        order_id: Unique order identifier
        amount: Amount to charge in cents
        card_token: Payment token (e.g., tok_visa_4242)
        customer_email: Customer's email for notifications

    Returns:
        Dict containing the workflow result with all IDs and status
    """
    print(f"[PaymentDistributedWorkflow] Starting for order {order_id}")
    print(f"[PaymentDistributedWorkflow] Amount: ${amount / 100:.2f}")

    # Track API calls for metrics
    api_calls = {
        "stripe_count": 0,
        "inventory_count": 0,
        "email_count": 0,
    }

    charge_id = None
    reservation_id = None

    # Step 1: Charge the card
    charge_result = await ctx.execute_task(
        stripe_charge,
        amount=amount,
        card_token=card_token,
        order_id=order_id,
    )
    api_calls["stripe_count"] += 1
    # Note: Result is a dict after JSON serialization/deserialization
    charge_id = (
        charge_result["charge_id"]
        if isinstance(charge_result, dict)
        else charge_result.charge_id
    )
    print(f"[PaymentDistributedWorkflow] Charge successful: {charge_id}")

    # Step 2: Reserve inventory
    try:
        reservation = await ctx.execute_task(
            inventory_reserve,
            order_id=order_id,
        )
        api_calls["inventory_count"] += 1
        reservation_id = (
            reservation["reservation_id"]
            if isinstance(reservation, dict)
            else reservation.reservation_id
        )
        print(f"[PaymentDistributedWorkflow] Inventory reserved: {reservation_id}")
    except Exception as e:
        # Compensation: Refund the charge
        print(f"[PaymentDistributedWorkflow] Inventory failed, refunding charge: {e}")
        await ctx.execute_task(
            stripe_refund,
            charge_id=charge_id,
        )
        api_calls["stripe_count"] += 1
        raise

    # Step 3: Send confirmation email
    try:
        email_result = await ctx.execute_task(
            email_send,
            to_email=customer_email,
            subject=f"Order {order_id} Confirmed",
            template="order_confirmation",
        )
        api_calls["email_count"] += 1
        notification_id = (
            email_result["message_id"]
            if isinstance(email_result, dict)
            else email_result.message_id
        )
        print(f"[PaymentDistributedWorkflow] Email sent: {notification_id}")
    except Exception as e:
        # Compensation: Release inventory + refund
        print(f"[PaymentDistributedWorkflow] Email failed, compensating: {e}")
        await ctx.execute_task(
            inventory_release,
            reservation_id=reservation_id,
        )
        api_calls["inventory_count"] += 1
        await ctx.execute_task(
            stripe_refund,
            charge_id=charge_id,
        )
        api_calls["stripe_count"] += 1
        raise

    print("[PaymentDistributedWorkflow] Completed successfully")

    return {
        "order_id": order_id,
        "charge_id": charge_id,
        "reservation_id": reservation_id,
        "notification_id": notification_id,
        "status": "completed",
        "api_calls": api_calls,
        "workflow_id": ctx.info.workflow_id,
        "run_id": ctx.info.run_id,
    }
