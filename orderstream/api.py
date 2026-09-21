from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, StrictInt

from orderstream.storage import cancel_order, connect, create_order, summary

app = FastAPI(title="Order Stream", version="1.0.0")


class NewOrder(BaseModel):
    request_key: str = Field(min_length=1, max_length=100)
    amount_cents: StrictInt = Field(ge=1, le=100_000_000)


@app.get("/", response_class=HTMLResponse)
def home():
    return Path(__file__).with_name("index.html").read_text()


@app.get("/api/health")
def health():
    with connect() as database:
        database.execute("SELECT 1")
    return {"database": "ready"}


@app.post("/api/orders")
def create(body: NewOrder):
    try:
        return create_order(body.request_key, body.amount_cents)
    except ValueError as error:
        raise HTTPException(409, str(error))


@app.post("/api/orders/{order_id}/cancel")
def cancel(order_id: UUID):
    try:
        return cancel_order(order_id)
    except LookupError as error:
        raise HTTPException(404, str(error))


@app.get("/api/orders")
def orders():
    with connect() as database:
        return database.execute(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT 50"
        ).fetchall()


@app.get("/api/summary")
def totals(projection: str = "live"):
    return summary(projection)


@app.get("/api/failures")
def failures(projection: str = "live"):
    with connect() as database:
        return database.execute(
            """SELECT id,topic,partition_id,offset_id,error,attempts,created_at
            FROM failed_events WHERE projection=%s ORDER BY id DESC LIMIT 50""",
            (projection,),
        ).fetchall()
