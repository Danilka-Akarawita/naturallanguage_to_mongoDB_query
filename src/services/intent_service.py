import json
from typing import Dict, Any, Optional

from openai import OpenAI
from pydantic import ValidationError

from src.models.intent import Intent
from src.config import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


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

SYSTEM_PROMPT = """You convert user requests into an Intent JSON object.

Rules:
- When user asks for human-readable names of roles, map foreign key fields to their aliases in the schema.
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

Aggregation Rules:
- When user asks "how many", "count", "total number of", "number of" → use aggregation with type="count"
- For count queries, select should be empty [] since we only return the count.
- When user asks "total/sum of X", "average X", "group by" → use aggregation with type="group"
- For group queries, specify 'by' field and 'operations' with named results.
- If no aggregation is needed (user wants actual records), leave aggregation as null.
"""


def build_response_format_json_schema() -> Dict[str, Any]:
    """
    Build OpenAI Structured Outputs schema wrapper.
    """
    schema = Intent.model_json_schema()
    props = schema.get("properties", {})
    schema["required"] = list(props.keys())

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
    # Use config settings for API key
    client = OpenAI(api_key=settings.OPENAI_API_KEY)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"SCHEMA CONTEXT:\n{schema_text}\n\nUSER QUESTION:\n{question}",
        },
    ]

    logger.info(f"Generating intent for question: {question}")
    
    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            response_format=build_response_format_json_schema(),
            temperature=0.0,
        )

        raw = response.choices[0].message.content
        if not raw:
            raise ValueError("Empty model response")

        data = json.loads(raw)
        intent = Intent.model_validate(data)
        logger.info(f"Intent generated successfully: {intent.root}")
        return intent.model_dump()

    except json.JSONDecodeError as e:
        logger.error(f"Model returned invalid JSON: {e}")
        raise ValueError(f"Model returned invalid JSON") from e
    except ValidationError as e:
        logger.error(f"Intent validation failed: {e}")
        raise ValueError(f"Intent validation failed") from e
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        raise

if __name__ == "__main__":
    # Simple CLI test
    import sys
    if len(sys.argv) > 1:
        q = sys.argv[1]
        print(json.dumps(generate_intent_json(q), indent=2))
    else:
        print("Usage: python -m src.services.intent_service 'Your question here'")
