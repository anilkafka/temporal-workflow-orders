"""
Order Processing Workflow — Temporal SDK (Python)
==================================================
Author: Anil Dosapati
Use Case: E-commerce order processing with durable execution

Background:
    At Confluent, I worked with customers like Henry Schein building
    event-driven order processing on Kafka. The core problem was always
    the same — Kafka moves data reliably but doesn't track WHERE a specific
    order is in its lifecycle. When payment succeeded but warehouse
    notification failed, engineers pieced together state from logs across
    multiple services at 2am.

    This workflow shows how Temporal solves that problem with durable
    execution — every step is checkpointed, failures are retried
    automatically, and the Web UI shows full execution history in one place.

Architecture:
    Order Placed
        │
        ▼
    Check Inventory ──── fail ──► Raise error (order cancelled)
        │
        ▼
    Generate Invoice
        │
        ▼
    Process Payment ──── fail ──► Compensate: Restock Inventory
        │
        ▼
    Notify Warehouse
        │
        ▼
    ┌───┴────────────────────┐
    │  Parallel notifications │   ← asyncio.gather
    │  Email + SMS + Slack   │
    └────────────────────────┘
        │
        ▼
    Order Complete
"""

import asyncio
import sys
from datetime import timedelta

from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError


# ─────────────────────────────────────────────────────────────
# ACTIVITIES — individual steps of the order flow
# In production each activity would connect to a real service.
# Simulated here with print statements and realistic logic.
# ─────────────────────────────────────────────────────────────

@activity.defn
async def check_inventory(order: dict) -> dict:
    """
    Step 1 — Verify stock is available before charging the customer.
    In production: query inventory DB or microservice.
    """
    print(f"\n[INVENTORY] Checking stock for {order['sku']}...")
    print(f"[INVENTORY] Requested qty: {order['qty']}")

    # Simulate inventory levels
    available_stock = {"sku_999": 100, "sku_888": 3}
    stock = available_stock.get(order['sku'], 0)

    if stock < order['qty']:
        # Business error — not enough stock
        # Temporal will NOT retry this by default (non-retryable)
        raise ValueError(
            f"Insufficient stock for {order['sku']}: "
            f"have {stock}, need {order['qty']}"
        )

    remaining = stock - order['qty']
    print(f"[INVENTORY] Stock OK — {stock} available, {remaining} after order")
    return {"status": "ok", "remaining_stock": remaining, "sku": order['sku']}


@activity.defn
async def generate_invoice(order: dict) -> dict:
    """
    Step 2 — Generate invoice before charging.
    In production: call billing service, store in DB.
    """
    print(f"\n[INVOICE] Generating invoice for order {order['order_id']}...")
    unit_price = 49.99
    total = round(order['qty'] * unit_price, 2)
    invoice = {
        "invoice_id": f"INV-{order['order_id']}",
        "order_id":   order['order_id'],
        "sku":        order['sku'],
        "qty":        order['qty'],
        "unit_price": unit_price,
        "total":      total,
        "currency":   "USD"
    }
    print(f"[INVOICE] Invoice generated: {invoice['invoice_id']} — ${total}")
    return invoice


@activity.defn
async def process_payment(order: dict) -> dict:
    """
    Step 3 — Charge the customer.
    In production: call Stripe/payment gateway.
    Simulates failure when force_fail=True for demo purposes.
    """
    print(f"\n[PAYMENT] Processing payment for order {order['order_id']}...")
    amount = round(order['qty'] * 49.99, 2)
    print(f"[PAYMENT] Charging ${amount} to card ending {order.get('card_last4', '4242')}")

    # Simulate payment failure for demo
    if order.get('force_fail_payment'):
        raise ValueError(
            f"Payment declined for order {order['order_id']} — "
            f"insufficient funds"
        )

    print(f"[PAYMENT] Payment approved — transaction ID: TXN-{order['order_id']}")
    return {
        "status":         "approved",
        "transaction_id": f"TXN-{order['order_id']}",
        "amount":         amount
    }


@activity.defn
async def restock_inventory(order: dict) -> dict:
    """
    Compensation activity — runs ONLY if payment fails.
    Reverses the inventory reservation from Step 1.
    In production: call inventory service to release reserved stock.
    This is the saga pattern — undo what was done.
    """
    print(f"\n[COMPENSATION] Payment failed — restocking inventory...")
    print(f"[COMPENSATION] Returning {order['qty']} units of {order['sku']} to stock")
    print(f"[COMPENSATION] Inventory restored — order {order['order_id']} cancelled")
    return {"status": "restocked", "qty_returned": order['qty']}


@activity.defn
async def notify_warehouse(order: dict) -> dict:
    """
    Step 4 — Tell warehouse to pick, pack, ship.
    In production: call WMS (Warehouse Management System).
    """
    print(f"\n[WAREHOUSE] Sending fulfillment request for order {order['order_id']}...")
    print(f"[WAREHOUSE] Pick {order['qty']} x {order['sku']} from shelf A-12")
    print(f"[WAREHOUSE] Ship to: {order.get('shipping_address', '123 Main St, Boston MA')}")
    return {
        "status":          "fulfillment_requested",
        "fulfillment_id":  f"FUL-{order['order_id']}",
        "estimated_ship":  "2 business days"
    }


# ── Parallel notification activities ──────────────────────────
# These three run simultaneously after payment clears

@activity.defn
async def send_email_confirmation(order: dict) -> str:
    print(f"\n[EMAIL] Sending order confirmation to {order.get('email', 'customer@example.com')}")
    print(f"[EMAIL] Subject: Your order {order['order_id']} is confirmed!")
    return "email_sent"


@activity.defn
async def send_sms_update(order: dict) -> str:
    print(f"\n[SMS] Sending SMS to {order.get('phone', '+1-555-0100')}")
    print(f"[SMS] Message: Order {order['order_id']} confirmed. Ships in 2 days.")
    return "sms_sent"


@activity.defn
async def update_finance_report(order: dict) -> str:
    """
    In production: write to financial reporting DB or data lake.
    Keeping this separate from operational DB — same pattern as
    Henry Schein where we separated operational and analytical workloads.
    """
    amount = round(order['qty'] * 49.99, 2)
    print(f"\n[FINANCE] Recording ${amount} revenue for order {order['order_id']}")
    print(f"[FINANCE] Finance report updated — operational/analytical workloads separated")
    return "finance_updated"


# ─────────────────────────────────────────────────────────────
# WORKFLOW — orchestrates all activities in the correct order
# This is the durable execution layer Temporal provides.
# If this process crashes after Step 2, it replays from Step 3.
# No hand-rolled idempotency, no log archaeology.
# ─────────────────────────────────────────────────────────────

@workflow.defn
class OrderWorkflow:

    @workflow.run
    async def run(self, order: dict) -> dict:
        print(f"\n{'='*60}")
        print(f"WORKFLOW STARTED: Order {order['order_id']}")
        print(f"{'='*60}")

        # Retry policy — Temporal retries activities automatically
        # Non-retryable errors (ValueError) stop retrying immediately
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            non_retryable_error_types=["ValueError"]
        )

        timeout = timedelta(seconds=30)

        # ── Step 1: Check inventory ──────────────────────────
        print("\n--- Step 1: Check Inventory ---")
        inv_result = await workflow.execute_activity(
            check_inventory,
            order,
            start_to_close_timeout=timeout,
            retry_policy=retry_policy
        )
        print(f"Step 1 complete: {inv_result['status']}")

        # ── Step 2: Generate invoice ─────────────────────────
        print("\n--- Step 2: Generate Invoice ---")
        invoice = await workflow.execute_activity(
            generate_invoice,
            order,
            start_to_close_timeout=timeout,
            retry_policy=retry_policy
        )
        print(f"Step 2 complete: {invoice['invoice_id']} — ${invoice['total']}")

        # ── Step 3: Process payment (with compensation) ──────
        print("\n--- Step 3: Process Payment ---")
        try:
            payment = await workflow.execute_activity(
                process_payment,
                order,
                start_to_close_timeout=timeout,
                retry_policy=retry_policy
            )
            print(f"Step 3 complete: {payment['transaction_id']}")

        except ActivityError as e:
            # Payment failed — run compensation to restock inventory
            # This is the saga pattern: undo Step 1 since payment didn't go through
            print(f"\nPayment failed: {e}")
            print("Running compensation — restocking inventory...")
            await workflow.execute_activity(
                restock_inventory,
                order,
                start_to_close_timeout=timeout,
            )
            return {
                "status":   "cancelled",
                "order_id": order['order_id'],
                "reason":   "payment_failed"
            }

        # ── Step 4: Notify warehouse ─────────────────────────
        print("\n--- Step 4: Notify Warehouse ---")
        warehouse = await workflow.execute_activity(
            notify_warehouse,
            order,
            start_to_close_timeout=timeout,
            retry_policy=retry_policy
        )
        print(f"Step 4 complete: {warehouse['fulfillment_id']}")

        # ── Step 5: Parallel notifications ───────────────────
        # Email, SMS, and finance report are independent
        # Run simultaneously with asyncio.gather for speed
        print("\n--- Step 5: Parallel Notifications ---")
        email, sms, finance = await asyncio.gather(
            workflow.execute_activity(
                send_email_confirmation, order,
                start_to_close_timeout=timeout
            ),
            workflow.execute_activity(
                send_sms_update, order,
                start_to_close_timeout=timeout
            ),
            workflow.execute_activity(
                update_finance_report, order,
                start_to_close_timeout=timeout
            ),
        )
        print(f"Step 5 complete: {email}, {sms}, {finance}")

        print(f"\n{'='*60}")
        print(f"WORKFLOW COMPLETE: Order {order['order_id']}")
        print(f"{'='*60}\n")

        return {
            "status":         "completed",
            "order_id":       order['order_id'],
            "invoice":        invoice['invoice_id'],
            "transaction_id": payment['transaction_id'],
            "fulfillment_id": warehouse['fulfillment_id'],
            "notifications":  [email, sms, finance]
        }


# ─────────────────────────────────────────────────────────────
# WORKER — polls Temporal for work and executes it
# Think of this as the Kafka consumer loop —
# always running, always ready to process the next message
# ─────────────────────────────────────────────────────────────

async def run_worker():
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue="orders",
        workflows=[OrderWorkflow],
        activities=[
            check_inventory,
            generate_invoice,
            process_payment,
            restock_inventory,
            notify_warehouse,
            send_email_confirmation,
            send_sms_update,
            update_finance_report,
        ]
    )
    print("Worker started — polling for orders on task queue: orders")
    print("Open http://localhost:8233 to see workflows in the Web UI\n")
    await worker.run()


# ─────────────────────────────────────────────────────────────
# STARTER — submits orders to the workflow
# ─────────────────────────────────────────────────────────────

async def run_starter(scenario: str = "success"):
    client = await Client.connect("localhost:7233")

    # Scenario 1 — successful order
    if scenario == "success":
        order = {
            "order_id":        "ord_8821",
            "sku":             "sku_999",
            "qty":             5,
            "email":           "customer@example.com",
            "phone":           "+1-555-0100",
            "shipping_address":"123 Main St, Boston MA",
            "card_last4":      "4242"
        }

    # Scenario 2 — payment failure (triggers compensation)
    elif scenario == "payment_fail":
        order = {
            "order_id":           "ord_9999",
            "sku":                "sku_999",
            "qty":                2,
            "email":              "customer2@example.com",
            "phone":              "+1-555-0200",
            "shipping_address":   "456 Oak Ave, Cambridge MA",
            "card_last4":         "0002",
            "force_fail_payment": True    # triggers payment failure
        }

    # Scenario 3 — out of stock
    elif scenario == "out_of_stock":
        order = {
            "order_id": "ord_1111",
            "sku":      "sku_888",
            "qty":      99,           # more than available stock
            "email":    "customer3@example.com",
            "phone":    "+1-555-0300",
        }

    print(f"\nSubmitting order: {order['order_id']} (scenario: {scenario})")

    result = await client.execute_workflow(
        OrderWorkflow.run,
        order,
        id=f"order-{order['order_id']}",
        task_queue="orders",
    )

    print(f"\nFinal result: {result}")


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# Usage:
#   python3 workflow.py worker
#   python3 workflow.py starter success
#   python3 workflow.py starter payment_fail
#   python3 workflow.py starter out_of_stock
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 workflow.py [worker|starter] [success|payment_fail|out_of_stock]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "worker":
        asyncio.run(run_worker())
    elif command == "starter":
        scenario = sys.argv[2] if len(sys.argv) > 2 else "success"
        asyncio.run(run_starter(scenario))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
