"""
Payment Tasks Example

This module demonstrates task definitions with the @task decorator.
Tasks are where side effects happen - payment processing, sending emails,
calling external APIs, etc.
"""

import asyncio
import uuid
from dataclasses import dataclass

from orcher import TaskContext, task


@dataclass
class ChargeResult:
    """Result of a card charge operation."""

    charge_id: str
    amount: int
    status: str


@dataclass
class RefundResult:
    """Result of a refund operation."""

    refund_id: str
    amount: int
    status: str


@dataclass
class NotificationResult:
    """Result of sending a notification."""

    message_id: str
    sent: bool


@task(name="chargeCard", timeout=30.0)
async def charge_card(
    ctx: TaskContext,
    amount: int,
    token: str,
    order_id: str,
    customer_email: str,
    idempotency_key: str | None = None,
) -> ChargeResult:
    """
    Charge a credit card.

    This task simulates a payment processor (like Stripe) charging a card.
    In production, this would call the actual payment API.

    Args:
        ctx: Task context
        amount: Amount to charge in cents
        token: Payment token (e.g., from Stripe.js)
        order_id: Order identifier
        customer_email: Customer's email address
        idempotency_key: Optional key for idempotent retries

    Returns:
        ChargeResult with charge_id, amount, and status
    """
    print(
        f"[chargeCard] Processing payment for order {order_id}, amount: ${amount / 100:.2f}"
    )

    # Simulate API call delay
    await asyncio.sleep(0.1)

    # Generate charge ID
    charge_id = f"ch_{uuid.uuid4().hex[:16]}"

    print(f"[chargeCard] Payment successful: {charge_id}")

    return ChargeResult(
        charge_id=charge_id,
        amount=amount,
        status="succeeded",
    )


@task(name="refundCharge", timeout=30.0)
async def refund_charge(
    ctx: TaskContext,
    charge_id: str,
    order_id: str,
    customer_email: str,
    reason: str,
    amount: int | None = None,
) -> RefundResult:
    """
    Refund a charge.

    Args:
        ctx: Task context
        charge_id: The charge ID to refund
        order_id: Order identifier
        customer_email: Customer's email
        reason: Reason for refund
        amount: Optional partial refund amount (full refund if not specified)

    Returns:
        RefundResult with refund_id, amount, and status
    """
    print(f"[refundCharge] Processing refund for charge {charge_id}")

    # Simulate API call delay
    await asyncio.sleep(0.1)

    refund_id = f"re_{uuid.uuid4().hex[:16]}"
    refund_amount = amount or 9999  # Default to full refund

    print(f"[refundCharge] Refund successful: {refund_id}")

    return RefundResult(
        refund_id=refund_id,
        amount=refund_amount,
        status="succeeded",
    )


@task(name="sendPaymentNotification", timeout=20.0)
async def send_payment_notification(
    ctx: TaskContext,
    notification_type: str,
    email: str,
    order_id: str,
    charge_id: str | None = None,
    refund_id: str | None = None,
    amount: int | None = None,
    reason: str | None = None,
) -> NotificationResult:
    """
    Send a payment notification email.

    Args:
        ctx: Task context
        notification_type: Type of notification ("success", "failure", "refund")
        email: Recipient email address
        order_id: Order identifier
        charge_id: Charge ID (for success notifications)
        refund_id: Refund ID (for refund notifications)
        amount: Amount (for success/refund notifications)
        reason: Reason (for failure notifications)

    Returns:
        NotificationResult with message_id and sent status
    """
    print(f"[sendPaymentNotification] Sending {notification_type} email to {email}")

    # Simulate email sending delay
    await asyncio.sleep(0.05)

    message_id = f"msg_{uuid.uuid4().hex[:16]}"

    print(f"[sendPaymentNotification] Email sent: {message_id}")

    return NotificationResult(
        message_id=message_id,
        sent=True,
    )


@task(name="verifyChargeStatus", timeout=15.0)
async def verify_charge_status(
    ctx: TaskContext,
    charge_id: str,
) -> dict:
    """
    Verify the status of a charge.

    Args:
        ctx: Task context
        charge_id: The charge ID to verify

    Returns:
        Dict with charge_id, status, and verified flag
    """
    print(f"[verifyChargeStatus] Verifying charge {charge_id}")

    # Simulate API call
    await asyncio.sleep(0.05)

    return {
        "charge_id": charge_id,
        "status": "succeeded",
        "verified": True,
    }
