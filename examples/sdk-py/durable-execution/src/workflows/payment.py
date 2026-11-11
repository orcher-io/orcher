"""
Payment Workflow Example

This workflow demonstrates a simple payment processing flow with
task execution for charging a card and sending a notification.
"""

from dataclasses import dataclass
from typing import Any

# Import task functions for ctx.execute_task()
from tasks.payment import charge_card, send_payment_notification

from orcher import WorkflowContext, workflow


@dataclass
class PaymentInput:
    """Input for the payment workflow."""

    order_id: str
    amount: int
    card_token: str
    customer_email: str


@dataclass
class PaymentResult:
    """Result of the payment workflow."""

    order_id: str
    charge_id: str
    notification_id: str
    status: str
    error: str | None = None


@workflow(name="paymentWorkflow", version="1.0")
async def payment_workflow(
    ctx: WorkflowContext,
    order_id: str,
    amount: int,
    card_token: str,
    customer_email: str,
) -> dict[str, Any]:
    """
    Payment processing workflow.

    This workflow demonstrates:
    - Sequential task execution using ctx.execute_task()
    - Error handling and compensation
    - Returning structured results

    Flow:
    1. Charge the customer's card
    2. Send success notification
    3. Return result

    On failure:
    - Send failure notification
    - Return error result

    Args:
        ctx: Workflow context
        order_id: Unique order identifier
        amount: Amount to charge in cents
        card_token: Payment token
        customer_email: Customer's email for notifications

    Returns:
        Dict containing the payment result
    """
    print(f"[PaymentWorkflow] Starting payment for order {order_id}")
    print(f"[PaymentWorkflow] Amount: ${amount / 100:.2f}")

    # Step 1: Charge the card
    charge_result = await ctx.execute_task(
        charge_card,
        amount=amount,
        token=card_token,
        order_id=order_id,
        customer_email=customer_email,
    )
    charge_id = charge_result.charge_id
    print(f"[PaymentWorkflow] Charge successful: {charge_id}")

    # Step 2: Send success notification
    notification = await ctx.execute_task(
        send_payment_notification,
        notification_type="success",
        email=customer_email,
        order_id=order_id,
        charge_id=charge_id,
        amount=amount,
    )
    notification_id = notification.message_id
    print(f"[PaymentWorkflow] Notification sent: {notification_id}")

    return {
        "order_id": order_id,
        "charge_id": charge_id,
        "notification_id": notification_id,
        "status": "completed",
        "workflow_id": ctx.info.workflow_id,
        "run_id": ctx.info.run_id,
    }
