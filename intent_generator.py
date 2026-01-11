"""
intent_generator.py

Step 2 of your pipeline:
Natural language question -> Intent JSON (validated)

Uses OpenAI Structured Outputs (JSON Schema) so the model must return
valid JSON matching your schema.

Install:
  pip install openai pydantic

Set env:
  export OPENAI_API_KEY="..."

Run:
  python intent_generator.py --q "Show pending orders from Colombo outlet in December with customer name and product names"
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Literal, Union

from pydantic import BaseModel, Field, ValidationError, ConfigDict
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# -----------------------------
# 1) Intent JSON schema (Pydantic)
# -----------------------------

Op = Literal[
    "eq", "ne", "gt", "gte", "lt", "lte",
    "between", "in",
    "contains", "starts_with", "ends_with"
]

SortDir = Literal["asc", "desc"]


class Filter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pathHint: str = Field(
        ...,
        description="Field path requested by user, e.g. 'status', 'outlet.name', 'items.product.name'"
    )
    op: Op
    value: Union[str, int, float, bool, List[Union[str, int, float, bool]]]


class Sort(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pathHint: str
    dir: SortDir


class Intent(BaseModel):
    """
    WHAT the user wants (no DB-specific logic).
    """
    model_config = ConfigDict(extra="forbid")

    root: str = Field(..., description="Root collection name, e.g. 'orders'")
    select: List[str] = Field(..., description="Fields to return")
    filters: List[Filter] = Field(...)
    sort: List[Sort] = Field(...)
    limit: int = Field(..., ge=1, le=200, description="Result size limit")


# -----------------------------
# 2) Schema context
# -----------------------------

DEFAULT_SCHEMA_TEXT = """
MongoDB schema (prototype)

Collections:
- users(_id, name, role, outletId, orgId)
- customers(_id, name, phone, tags[], orgId)
- outlets(_id, name, city, orgId)
- products(_id, sku, name, category, price, active, orgId)
- orders(_id, orderNo, customerId, outletId, createdByUserId, status, createdAt, needDelivery, items[], customization, orgId)
  - items[]: { productId, qty, unitPrice }
- payments(_id, orderId, paidByCustomerId, method, amount, paidAt, status, orgId)
- deliveries(_id, orderId, assignedToUserId, deliveryStatus, address, pinCode, deliveredAt, orgId)
- inventory_moves(_id, productId, outletId, type, qty, ref{orderId,deliveryId}, createdAt, orgId)

Role mappings (aliases):
- "person who created the order" -> createdBy
- "created by user" -> createdBy
- "order creator" -> createdBy
- "delivery driver" -> driver
- "assigned driver" -> driver
- "driver of order" -> driver
- "customer" -> customer
- "outlet" -> outlet
- "product" -> product
- "payment" -> payment
- "delivery" -> delivery


Enums:
- orders.status: PENDING, IN_PRODUCTION, READY, DELIVERED, CANCELLED
- payments.status: SUCCESS, FAILED, REFUNDED
- deliveries.deliveryStatus: PENDING, OUT_FOR_DELIVERY, DELIVERED, FAILED
- inventory_moves.type: IN, OUT, WASTAGE, TRANSFER
"""


# -----------------------------
# 3) LLM call
# -----------------------------

SYSTEM_PROMPT = """You convert user requests into an Intent JSON object.

Rules:
- -When user asks for human-readable names of roles, map foreign key fields to their aliases in the schema.
- Output MUST match the provided JSON schema (no extra keys).
- root must be one of the known collections.
- select includes only explicitly requested fields.
- filters must be explicit with op/value.
- Date ranges like "December" must use 'between' with ISO dates (YYYY-MM-DD).
- Never output MongoDB aggregation stages or SQL.
- Never use foreign key fields for human text.
- Never use fields ending with "Id" unless the user explicitly provides an ID.
- When the user refers to an entity by name, city, or label,
  use dotted paths like outlet.name or outlet.city.
  - select MUST include only fields explicitly requested by the user.
- If the user does not ask for fields, select MUST be ["orderNo"].
- Location phrases like "from Colombo" always refer to outlet.city unless explicitly stated otherwise.
- Do not invent delivery.address subfields.
- Do not invent limit values unless the user specifies one.
- If a month is mentioned without a year, assume the most recent past month relative to today.
- Use "deliveries.*" for delivery-related fields (never "delivery.*").



"""


def build_response_format_json_schema() -> Dict[str, Any]:
    """
    Build OpenAI Structured Outputs schema wrapper.
    """
    schema = Intent.model_json_schema()

    return {
        "type": "json_schema",
        "json_schema": {
            "name": "intent_schema",
            "schema": schema,
            "strict": True,
        },
    }


def generate_intent_json(
    question: str,
    schema_text: str = DEFAULT_SCHEMA_TEXT,
    model: str = "gpt-4.1-mini",
) -> Dict[str, Any]:
    """
    Returns validated Intent JSON.
    """
    client = OpenAI()  # API key from env

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"SCHEMA CONTEXT:\n{schema_text}\n\nUSER QUESTION:\n{question}",
        },
    ]

    response = client.chat.completions.create(
        model=model,
        messages=messages,
        response_format=build_response_format_json_schema(),
        temperature=0.0,
    )

    raw = response.choices[0].message.content
    if not raw:
        raise ValueError("Empty model response")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Model returned invalid JSON:\n{raw}") from e

    try:
        intent = Intent.model_validate(data)
    except ValidationError as e:
        raise ValueError(f"Intent validation failed:\n{e}\nRaw:\n{raw}") from e

    return intent.model_dump()


# -----------------------------
# 4) CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--q", required=True, help="Natural language question")
    ap.add_argument("--model", default="gpt-4.1-mini", help="OpenAI model name")
    ap.add_argument("--schema_file", default="", help="Optional schema text file")
    args = ap.parse_args()

    schema_text = DEFAULT_SCHEMA_TEXT
    if args.schema_file:
        with open(args.schema_file, "r", encoding="utf-8") as f:
            schema_text = f.read()

    intent = generate_intent_json(
        question=args.q,
        schema_text=schema_text,
        model=args.model,
    )

    print(json.dumps(intent, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
