# Order Processing Workflow — Temporal SDK (Python)

## Background

At Confluent, I worked with enterprise customers building event-driven order 
processing on Kafka. The core problem was always the same — Kafka moves data 
reliably but doesn't track WHERE a specific order is in its lifecycle.

At a customer like Henry Schein, when payment succeeded but warehouse 
notification failed, engineers had to piece together state from logs across 
multiple services at 2am. No single source of truth. No automatic retry. 
No visibility.

This workflow shows how Temporal solves that problem with durable execution.

---

## The Problem — Kafka Alone

```
Customer places order
        │
        ▼
Order Service ──► Kafka Topic ──► Email Consumer
                              ──► Payment Consumer     ← if this crashes...
                              ──► Inventory Consumer   ← does this still run?
                              ──► Warehouse Consumer   ← nobody knows
```

**What breaks:**
- Payment succeeds, inventory update fails — order in inconsistent state
- No visibility into where the order is in its lifecycle
- Engineers hand-roll retry logic, dead letter queues, saga patterns
- Every team rebuilds the same infrastructure plumbing

---

## The Solution — Temporal

```
Customer places order
        │
        ▼
OrderWorkflow (durable, stateful, visible)
        │
        ├── Step 1: Check Inventory     ← retried automatically if fails
        ├── Step 2: Generate Invoice    ← skipped on replay if already done  
        ├── Step 3: Process Payment     ← if fails → compensation runs
        │       └── [fail] Restock Inventory  ← saga compensation
        ├── Step 4: Notify Warehouse
        └── Step 5: Parallel notifications
                ├── Email confirmation
                ├── SMS update
                └── Finance report
```

**What Temporal gives you:**
- Full execution history in Web UI — no log archaeology
- Automatic retry with configurable retry policy
- Durable state — workflow survives process crashes
- Saga compensation — restock inventory if payment fails
- Parallel execution where steps are independent

---

## Architecture

| Concern | DIY Kafka | Temporal |
|---|---|---|
| Retry on failure | Hand-rolled | Built in |
| Workflow state visibility | Piece together logs | Web UI |
| Compensation on failure | Saga pattern (complex) | Native |
| Idempotency | Custom dedup table | Workflow ID |
| Parallel steps | Complex coordination | asyncio.gather |

---

## Workflow Scenarios

Three scenarios included to demonstrate different paths:

| Scenario | Command | What happens |
|---|---|---|
| Happy path | `starter success` | All steps complete successfully |
| Payment failure | `starter payment_fail` | Payment fails, inventory restocked |
| Out of stock | `starter out_of_stock` | Inventory check fails, order cancelled |

---

## How to Run

**Prerequisites:**
```bash
pip3 install temporalio
brew install temporal
```

**Terminal 1 — Start Temporal server:**
```bash
temporal server start-dev
```

**Terminal 2 — Start the worker:**
```bash
python3 workflow.py worker
```

**Terminal 3 — Run a scenario:**
```bash
# Happy path
python3 workflow.py starter success

# Payment failure with compensation
python3 workflow.py starter payment_fail

# Out of stock
python3 workflow.py starter out_of_stock
```

**Web UI:** Open `http://localhost:8233` to see full execution history

---

## Key Concepts Demonstrated

**Durable execution** — if the worker crashes after Step 2, Temporal replays 
from Step 3. Steps 1 and 2 are not re-executed.

**Saga compensation** — if payment fails, `restock_inventory` runs automatically 
to undo the inventory reservation. No manual rollback logic.

**Parallel activities** — email, SMS, and finance reporting run simultaneously 
with `asyncio.gather`. Independent steps don't wait for each other.

**Retry policy** — activities retry up to 3 times automatically. ValueError 
(business errors like insufficient funds) stops retrying immediately.

**Workflow ID as idempotency key** — submitting the same order ID twice returns 
the existing workflow instead of creating a duplicate. Same guarantee as 
hand-rolling `INSERT ... ON CONFLICT` in Postgres, but at the framework level.
