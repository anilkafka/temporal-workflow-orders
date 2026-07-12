"""
FastAPI backend — bridges the HTML frontend to Temporal
=======================================================
Run this alongside the Temporal worker:
    pip3 install fastapi uvicorn
    python3 api.py
"""

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy

# Import the workflow from workflow.py
from workflow import OrderWorkflow

app = FastAPI()

# Allow frontend to talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files (index.html)
app.mount("/static", StaticFiles(directory="."), name="static")


class OrderRequest(BaseModel):
    order_id: str
    sku: str
    qty: int
    email: str
    phone: str
    shipping_address: str
    card_last4: str = "4242"
    force_fail_payment: bool = False


@app.get("/")
async def root():
    return FileResponse("index.html")


@app.post("/api/order")
async def submit_order(order: OrderRequest):
    """Submit a new order to Temporal workflow"""
    try:
        client = await Client.connect("localhost:7233")
        order_dict = order.dict()

        handle = await client.start_workflow(
            OrderWorkflow.run,
            order_dict,
            id=f"order-{order.order_id}",
            task_queue="orders",
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY
        )

        return {
            "status":      "started",
            "workflow_id": handle.id,
            "run_id":      handle.result_run_id
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/order/{order_id}")
async def get_order_status(order_id: str):
    """Poll workflow status from Temporal"""
    try:
        client = await Client.connect("localhost:7233")
        handle = client.get_workflow_handle(f"order-{order_id}")
        desc = await handle.describe()

        return {
            "workflow_id": desc.id,
            "status":      str(desc.status).replace("WorkflowExecutionStatus.", ""),
            "start_time":  str(desc.start_time),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
