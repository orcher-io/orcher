"""
Distributed Tasks Example

This module provides tasks for the distributed payment workflow,
simulating multiple microservices (payment, inventory, notification).
"""

import asyncio
import uuid
from dataclasses import dataclass

from orcher import TaskContext, task


@dataclass
class StripeChargeResult:
    """Result from Stripe charge."""

    charge_id: str
    amount: int
    status: str


@dataclass
class InventoryReservation:
    """Result from inventory reservation."""

    reservation_id: str
    order_id: str
    status: str


@dataclass
class EmailResult:
    """Result from email service."""

    message_id: str
    status: str


@task(name="stripeCharge", timeout=30.0)
async def stripe_charge(
    ctx: TaskContext,
    amount: int,
    card_token: str,
    order_id: str,
    idempotency_key: str | None = None,
) -> StripeChargeResult:
    """
    Process payment via Stripe.

    This simulates the Stripe API call. In production:
    - Use the actual Stripe SDK
    - Handle various error cases (card declined, insufficient funds, etc.)
    - Use idempotency keys for safe retries

    Args:
        ctx: Task context
        amount: Amount in cents
        card_token: Stripe card token
        order_id: Order identifier
        idempotency_key: Key for idempotent requests

    Returns:
        StripeChargeResult with charge details
    """
    print(f"[stripeCharge] Processing Stripe charge for order {order_id}")
    print(f"[stripeCharge] Amount: ${amount / 100:.2f}, Token: {card_token[:10]}...")

    # Simulate Stripe API latency
    await asyncio.sleep(0.15)

    charge_id = f"ch_{uuid.uuid4().hex[:24]}"

    print(f"[stripeCharge] Charge successful: {charge_id}")

    return StripeChargeResult(
        charge_id=charge_id,
        amount=amount,
        status="succeeded",
    )


@task(name="inventoryReserve", timeout=20.0)
async def inventory_reserve(
    ctx: TaskContext,
    order_id: str,
    items: list | None = None,
) -> InventoryReservation:
    """
    Reserve inventory for an order.

    This simulates an inventory management system call.
    In production, this would:
    - Check item availability
    - Create a reservation with timeout
    - Decrement available stock

    Args:
        ctx: Task context
        order_id: Order identifier
        items: List of items to reserve (optional)

    Returns:
        InventoryReservation with reservation details
    """
    print(f"[inventoryReserve] Reserving inventory for order {order_id}")

    # Simulate inventory system latency
    await asyncio.sleep(0.1)

    reservation_id = f"res_{uuid.uuid4().hex[:16]}"

    print(f"[inventoryReserve] Reservation created: {reservation_id}")

    return InventoryReservation(
        reservation_id=reservation_id,
        order_id=order_id,
        status="reserved",
    )


@task(name="emailSend", timeout=15.0)
async def email_send(
    ctx: TaskContext,
    to_email: str,
    subject: str,
    template: str,
    template_data: dict | None = None,
) -> EmailResult:
    """
    Send an email notification.

    This simulates an email service (SendGrid, SES, etc.).
    In production, this would:
    - Render the email template
    - Queue the email for delivery
    - Handle delivery status

    Args:
        ctx: Task context
        to_email: Recipient email address
        subject: Email subject
        template: Template name
        template_data: Data to populate template

    Returns:
        EmailResult with message details
    """
    print(f"[emailSend] Sending email to {to_email}")
    print(f"[emailSend] Subject: {subject}, Template: {template}")

    # Simulate email service latency
    await asyncio.sleep(0.05)

    message_id = f"msg_{uuid.uuid4().hex[:20]}"

    print(f"[emailSend] Email queued: {message_id}")

    return EmailResult(
        message_id=message_id,
        status="queued",
    )


@task(name="stripeRefund", timeout=30.0)
async def stripe_refund(
    ctx: TaskContext,
    charge_id: str,
    amount: int | None = None,
    reason: str = "requested_by_customer",
) -> dict:
    """
    Process refund via Stripe (compensation task).

    This is used as a compensating action when later steps fail.
    For example, if inventory reservation fails after payment,
    we need to refund the customer.

    Args:
        ctx: Task context
        charge_id: The charge to refund
        amount: Partial refund amount (None for full refund)
        reason: Refund reason for records

    Returns:
        Dict with refund details
    """
    print(f"[stripeRefund] Processing refund for charge {charge_id}")

    # Simulate Stripe API latency
    await asyncio.sleep(0.1)

    refund_id = f"re_{uuid.uuid4().hex[:24]}"

    print(f"[stripeRefund] Refund successful: {refund_id}")

    return {
        "refund_id": refund_id,
        "charge_id": charge_id,
        "status": "succeeded",
    }


@task(name="inventoryRelease", timeout=15.0)
async def inventory_release(
    ctx: TaskContext,
    reservation_id: str,
    reason: str = "order_cancelled",
) -> dict:
    """
    Release inventory reservation (compensation task).

    This is used when an order is cancelled or fails after
    inventory was reserved.

    Args:
        ctx: Task context
        reservation_id: The reservation to release
        reason: Reason for release

    Returns:
        Dict with release confirmation
    """
    print(f"[inventoryRelease] Releasing reservation {reservation_id}")

    # Simulate inventory system latency
    await asyncio.sleep(0.05)

    print("[inventoryRelease] Reservation released")

    return {
        "reservation_id": reservation_id,
        "status": "released",
        "reason": reason,
    }
