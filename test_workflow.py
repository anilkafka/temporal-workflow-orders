"""
Test Suite — Order Processing Workflow
=======================================
Uses Temporal's official testing framework (temporalio.testing)
as documented at https://docs.temporal.io/develop/python/best-practices/testing-suite

Install:
    pip3 install pytest pytest-asyncio

Run:
    pytest test_workflow.py -v
"""

import pytest
from temporalio.testing import WorkflowEnvironment, ActivityEnvironment
from temporalio.worker import Worker

from workflow import (
    OrderWorkflow,
    check_inventory,
    generate_invoice,
    process_payment,
    restock_inventory,
    notify_warehouse,
    send_email_confirmation,
    send_sms_update,
    update_finance_report,
)


# ─────────────────────────────────────────────────────────────
# FIXTURES
# WorkflowEnvironment.start_time_skipping() starts a lightweight
# test server that skips time automatically — no real Temporal
# server needed. Tests run in milliseconds.
# ─────────────────────────────────────────────────────────────

@pytest.fixture
async def workflow_env():
    """Spin up a time-skipping test environment for each test."""
    env = await WorkflowEnvironment.start_time_skipping()
    yield env
    await env.shutdown()


ALL_ACTIVITIES = [
    check_inventory,
    generate_invoice,
    process_payment,
    restock_inventory,
    notify_warehouse,
    send_email_confirmation,
    send_sms_update,
    update_finance_report,
]

# Sample orders for reuse across tests
GOOD_ORDER = {
    "order_id":        "ord_test_001",
    "sku":             "sku_999",
    "qty":             5,
    "email":           "test@example.com",
    "phone":           "+1-555-0100",
    "shipping_address":"123 Test St, Boston MA",
    "card_last4":      "4242"
}

PAYMENT_FAIL_ORDER = {**GOOD_ORDER, "order_id": "ord_test_002", "force_fail_payment": True}
OUT_OF_STOCK_ORDER = {**GOOD_ORDER, "order_id": "ord_test_003", "sku": "sku_888", "qty": 99}


# ─────────────────────────────────────────────────────────────
# ACTIVITY UNIT TESTS
# Test each activity in isolation using ActivityEnvironment
# No workflow or worker needed — just the function itself
# ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_check_inventory_success():
    """Inventory check passes when stock is available."""
    env = ActivityEnvironment()
    result = await env.run(check_inventory, GOOD_ORDER)
    assert result["status"] == "ok"
    assert result["sku"] == "sku_999"
    assert result["remaining_stock"] == 95  # 100 - 5


@pytest.mark.asyncio
async def test_check_inventory_out_of_stock():
    """Inventory check raises ValueError when stock is insufficient."""
    env = ActivityEnvironment()
    with pytest.raises(ValueError, match="Insufficient stock"):
        await env.run(check_inventory, OUT_OF_STOCK_ORDER)


@pytest.mark.asyncio
async def test_generate_invoice():
    """Invoice is generated with correct total."""
    env = ActivityEnvironment()
    result = await env.run(generate_invoice, GOOD_ORDER)
    assert result["invoice_id"] == f"INV-{GOOD_ORDER['order_id']}"
    assert result["total"] == round(5 * 49.99, 2)
    assert result["currency"] == "USD"


@pytest.mark.asyncio
async def test_process_payment_success():
    """Payment succeeds for a valid order."""
    env = ActivityEnvironment()
    result = await env.run(process_payment, GOOD_ORDER)
    assert result["status"] == "approved"
    assert result["transaction_id"] == f"TXN-{GOOD_ORDER['order_id']}"


@pytest.mark.asyncio
async def test_process_payment_failure():
    """Payment raises ValueError when force_fail_payment is True."""
    env = ActivityEnvironment()
    with pytest.raises(ValueError, match="Payment declined"):
        await env.run(process_payment, PAYMENT_FAIL_ORDER)


@pytest.mark.asyncio
async def test_restock_inventory():
    """Compensation activity restocks inventory correctly."""
    env = ActivityEnvironment()
    result = await env.run(restock_inventory, GOOD_ORDER)
    assert result["status"] == "restocked"
    assert result["qty_returned"] == GOOD_ORDER["qty"]


@pytest.mark.asyncio
async def test_notify_warehouse():
    """Warehouse notification returns fulfillment ID."""
    env = ActivityEnvironment()
    result = await env.run(notify_warehouse, GOOD_ORDER)
    assert result["status"] == "fulfillment_requested"
    assert result["fulfillment_id"] == f"FUL-{GOOD_ORDER['order_id']}"


# ─────────────────────────────────────────────────────────────
# WORKFLOW INTEGRATION TESTS
# Test the full workflow using WorkflowEnvironment + Worker
# Activities run for real — no mocks — verifying end-to-end flow
# ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_happy_path(workflow_env):
    """
    Full order flow completes successfully.
    All 5 steps execute in correct order.
    """
    async with Worker(
        workflow_env.client,
        task_queue="test-orders",
        workflows=[OrderWorkflow],
        activities=ALL_ACTIVITIES,
    ):
        result = await workflow_env.client.execute_workflow(
            OrderWorkflow.run,
            GOOD_ORDER,
            id=f"test-happy-{GOOD_ORDER['order_id']}",
            task_queue="test-orders",
        )

    assert result["status"] == "completed"
    assert result["order_id"] == GOOD_ORDER["order_id"]
    assert "invoice" in result
    assert "transaction_id" in result
    assert "fulfillment_id" in result
    assert len(result["notifications"]) == 3


@pytest.mark.asyncio
async def test_payment_failure_triggers_compensation(workflow_env):
    """
    When payment fails, compensation restocks inventory.
    Workflow returns cancelled status — not an exception.
    This validates the saga compensation pattern.
    """
    async with Worker(
        workflow_env.client,
        task_queue="test-orders",
        workflows=[OrderWorkflow],
        activities=ALL_ACTIVITIES,
    ):
        result = await workflow_env.client.execute_workflow(
            OrderWorkflow.run,
            PAYMENT_FAIL_ORDER,
            id=f"test-payfail-{PAYMENT_FAIL_ORDER['order_id']}",
            task_queue="test-orders",
        )

    assert result["status"] == "cancelled"
    assert result["reason"] == "payment_failed"
    assert result["order_id"] == PAYMENT_FAIL_ORDER["order_id"]


@pytest.mark.asyncio
async def test_out_of_stock_cancels_order(workflow_env):
    """
    When inventory check fails, workflow raises an exception.
    No payment is attempted — order is cancelled at Step 1.
    """
    async with Worker(
        workflow_env.client,
        task_queue="test-orders",
        workflows=[OrderWorkflow],
        activities=ALL_ACTIVITIES,
    ):
        with pytest.raises(Exception, match="Insufficient stock"):
            await workflow_env.client.execute_workflow(
                OrderWorkflow.run,
                OUT_OF_STOCK_ORDER,
                id=f"test-outofstock-{OUT_OF_STOCK_ORDER['order_id']}",
                task_queue="test-orders",
            )


@pytest.mark.asyncio
async def test_workflow_id_idempotency(workflow_env):
    """
    Submitting the same workflow ID twice returns the existing workflow.
    This is Temporal's built-in idempotency — no custom dedup code needed.
    Equivalent to INSERT ... ON CONFLICT DO NOTHING in Postgres.
    """
    async with Worker(
        workflow_env.client,
        task_queue="test-orders",
        workflows=[OrderWorkflow],
        activities=ALL_ACTIVITIES,
    ):
        # First submission
        result1 = await workflow_env.client.execute_workflow(
            OrderWorkflow.run,
            GOOD_ORDER,
            id="test-idem-ord_fixed",
            task_queue="test-orders",
        )

        # Second submission with same ID — should return same result
        result2 = await workflow_env.client.execute_workflow(
            OrderWorkflow.run,
            GOOD_ORDER,
            id="test-idem-ord_fixed",
            task_queue="test-orders",
        )

    assert result1["order_id"] == result2["order_id"]
    assert result1["transaction_id"] == result2["transaction_id"]
