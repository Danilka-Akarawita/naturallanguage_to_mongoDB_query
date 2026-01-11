
"""
mongo_schemas_seed.py

A single-file prototype MongoDB schema + seeder for a "complex" dataset
to test your NL -> Intent JSON -> (graph metadata join recipes) -> Mongo $lookup pipeline.

What you get:
- Pydantic models for 8 collections (schemas)
- Example seed documents
- A seed_to_mongo() function that inserts everything into MongoDB
- Unique indexes for common lookup keys (optional)

Requirements:
    pip install pymongo pydantic

Run:
    python mongo_schemas_seed.py --uri "mongodb://localhost:27017" --db "cakeflow_proto" --drop

Notes:
- Uses string IDs for simplicity (e.g., "O1001") so you can prototype quickly.
- In production, Mongo uses ObjectId; you can migrate later.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Literal, Optional, List, Dict, Any

from pydantic import BaseModel, Field, ConfigDict
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError


# ----------------------------
# Helpers
# ----------------------------

def utc_dt(s: str) -> datetime:
    """Parse ISO-like 'YYYY-MM-DDTHH:MM:SSZ' into a timezone-aware datetime."""
    # Accept both "...Z" and "...+00:00"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


# ----------------------------
# 1) users
# ----------------------------

UserRole = Literal["ADMIN", "OUTLET_STAFF", "DELIVERY_STAFF", "FACTORY_STAFF"]

class User(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    name: str
    role: UserRole
    outletId: Optional[str] = None  # ref -> outlets._id
    orgId: str


# ----------------------------
# 2) customers
# ----------------------------

class Customer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    name: str
    phone: str
    tags: List[str] = Field(default_factory=list)
    orgId: str


# ----------------------------
# 3) outlets
# ----------------------------

class Outlet(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    name: str
    city: str
    orgId: str


# ----------------------------
# 4) products
# ----------------------------

ProductCategory = Literal["CAKE", "TOPPING", "PACKAGING"]

class Product(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    sku: str
    name: str
    category: ProductCategory
    price: float
    active: bool
    orgId: str


# ----------------------------
# 5) orders (root)
# ----------------------------

OrderStatus = Literal["PENDING", "IN_PRODUCTION", "READY", "DELIVERED", "CANCELLED"]

class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    productId: str  # ref -> products._id
    qty: int
    unitPrice: float

class OrderCustomization(BaseModel):
    model_config = ConfigDict(extra="forbid")
    messageOnCake: Optional[str] = None
    theme: Optional[str] = None
    notes: Optional[str] = None

class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    orderNo: str
    customerId: str        # ref -> customers._id
    outletId: str          # ref -> outlets._id
    createdByUserId: str   # ref -> users._id
    status: OrderStatus
    createdAt: datetime
    needDelivery: bool
    items: List[OrderItem]
    customization: Optional[OrderCustomization] = None
    orgId: str


# ----------------------------
# 6) payments
# ----------------------------

PaymentMethod = Literal["CASH", "CARD", "BANK"]
PaymentStatus = Literal["SUCCESS", "FAILED", "REFUNDED"]

class Payment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    orderId: str              # ref -> orders._id
    paidByCustomerId: str     # ref -> customers._id
    method: PaymentMethod
    amount: float
    paidAt: datetime
    status: PaymentStatus
    orgId: str


# ----------------------------
# 7) deliveries
# ----------------------------

DeliveryStatus = Literal["PENDING", "OUT_FOR_DELIVERY", "DELIVERED", "FAILED"]

class Delivery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    orderId: str            # ref -> orders._id
    assignedToUserId: str   # ref -> users._id (delivery staff)
    deliveryStatus: DeliveryStatus
    address: str
    pinCode: Optional[str] = None
    deliveredAt: Optional[datetime] = None
    orgId: str


# ----------------------------
# 8) inventory_moves
# ----------------------------

InventoryMoveType = Literal["IN", "OUT", "WASTAGE", "TRANSFER"]

class InventoryRef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    orderId: Optional[str] = None     # ref -> orders._id
    deliveryId: Optional[str] = None  # ref -> deliveries._id

class InventoryMove(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    productId: str          # ref -> products._id
    outletId: str           # ref -> outlets._id
    type: InventoryMoveType
    qty: int
    ref: InventoryRef = Field(default_factory=InventoryRef)
    createdAt: datetime
    orgId: str


# ----------------------------
# Seed data
# ----------------------------

ORG = "ORG-01"

SEED_USERS: List[User] = [
    User(_id="U1", name="Kasun", role="OUTLET_STAFF", outletId="OT7", orgId=ORG),
    User(_id="U2", name="Amali", role="ADMIN", outletId=None, orgId=ORG),
    User(_id="U9", name="Ruwan", role="DELIVERY_STAFF", outletId=None, orgId=ORG),
]

SEED_OUTLETS: List[Outlet] = [
    Outlet(_id="OT7", name="Colombo 07 Outlet", city="Colombo", orgId=ORG),
    Outlet(_id="OT2", name="Kandy City Outlet", city="Kandy", orgId=ORG),
]

SEED_CUSTOMERS: List[Customer] = [
    Customer(_id="C10", name="Nimal", phone="+94XXXXXXXXX", tags=["VIP"], orgId=ORG),
    Customer(_id="C11", name="Sahan", phone="+94YYYYYYYYY", tags=[], orgId=ORG),
]

SEED_PRODUCTS: List[Product] = [
    Product(_id="P1", sku="CK-CHO-01", name="Chocolate Gateau", category="CAKE", price=4500, active=True, orgId=ORG),
    Product(_id="P2", sku="PK-RIB-01", name="Ribbon Pack", category="PACKAGING", price=300, active=True, orgId=ORG),
    Product(_id="P3", sku="TP-CHO-01", name="Choco Shavings", category="TOPPING", price=250, active=True, orgId=ORG),
]

SEED_ORDERS: List[Order] = [
    Order(
        _id="O1001",
        orderNo="ORD-1001",
        customerId="C10",
        outletId="OT7",
        createdByUserId="U1",
        status="PENDING",
        createdAt=utc_dt("2025-12-10T09:30:00Z"),
        needDelivery=True,
        items=[
            OrderItem(productId="P1", qty=1, unitPrice=4500),
            OrderItem(productId="P2", qty=2, unitPrice=300),
        ],
        customization=OrderCustomization(messageOnCake="Happy Birthday!", theme="Spiderman"),
        orgId=ORG,
    ),
    Order(
        _id="O1002",
        orderNo="ORD-1002",
        customerId="C11",
        outletId="OT7",
        createdByUserId="U1",
        status="READY",
        createdAt=utc_dt("2025-12-15T14:10:00Z"),
        needDelivery=False,
        items=[
            OrderItem(productId="P1", qty=1, unitPrice=4500),
            OrderItem(productId="P3", qty=1, unitPrice=250),
        ],
        customization=None,
        orgId=ORG,
    ),
    Order(
        _id="O2001",
        orderNo="ORD-2001",
        customerId="C10",
        outletId="OT2",
        createdByUserId="U2",
        status="DELIVERED",
        createdAt=utc_dt("2025-11-20T11:05:00Z"),
        needDelivery=True,
        items=[
            OrderItem(productId="P1", qty=2, unitPrice=4400),
        ],
        customization=OrderCustomization(messageOnCake="Congrats!", theme="Gold"),
        orgId=ORG,
    ),
]

SEED_PAYMENTS: List[Payment] = [
    Payment(
        _id="PAY1",
        orderId="O1001",
        paidByCustomerId="C10",
        method="CARD",
        amount=5100,
        paidAt=utc_dt("2025-12-10T10:00:00Z"),
        status="SUCCESS",
        orgId=ORG,
    ),
    Payment(
        _id="PAY2",
        orderId="O2001",
        paidByCustomerId="C10",
        method="CASH",
        amount=8800,
        paidAt=utc_dt("2025-11-20T12:00:00Z"),
        status="SUCCESS",
        orgId=ORG,
    ),
]

SEED_DELIVERIES: List[Delivery] = [
    Delivery(
        _id="D1",
        orderId="O1001",
        assignedToUserId="U9",
        deliveryStatus="OUT_FOR_DELIVERY",
        address="Colombo 07, ...",
        pinCode="4832",
        deliveredAt=None,
        orgId=ORG,
    ),
    Delivery(
        _id="D2",
        orderId="O2001",
        assignedToUserId="U9",
        deliveryStatus="DELIVERED",
        address="Kandy City, ...",
        pinCode="2190",
        deliveredAt=utc_dt("2025-11-20T15:20:00Z"),
        orgId=ORG,
    ),
]

SEED_INVENTORY_MOVES: List[InventoryMove] = [
    InventoryMove(
        _id="IM1",
        productId="P1",
        outletId="OT7",
        type="OUT",
        qty=1,
        ref=InventoryRef(orderId="O1001"),
        createdAt=utc_dt("2025-12-10T09:40:00Z"),
        orgId=ORG,
    ),
    InventoryMove(
        _id="IM2",
        productId="P2",
        outletId="OT7",
        type="OUT",
        qty=2,
        ref=InventoryRef(orderId="O1001"),
        createdAt=utc_dt("2025-12-10T09:41:00Z"),
        orgId=ORG,
    ),
    InventoryMove(
        _id="IM3",
        productId="P1",
        outletId="OT2",
        type="OUT",
        qty=2,
        ref=InventoryRef(orderId="O2001", deliveryId="D2"),
        createdAt=utc_dt("2025-11-20T11:20:00Z"),
        orgId=ORG,
    ),
    InventoryMove(
        _id="IM4",
        productId="P3",
        outletId="OT7",
        type="WASTAGE",
        qty=1,
        ref=InventoryRef(),
        createdAt=utc_dt("2025-12-16T08:10:00Z"),
        orgId=ORG,
    ),
]


# ----------------------------
# Seeding functions
# ----------------------------

def _insert_many_safe(coll, docs: List[Dict[str, Any]]) -> None:
    """Insert docs; ignore duplicates for repeated runs."""
    if not docs:
        return
    try:
        coll.insert_many(docs, ordered=False)
    except DuplicateKeyError:
        pass


def ensure_indexes(db):
    # Common patterns: org scoping + time filters + joins
    db.users.create_index([("orgId", ASCENDING), ("_id", ASCENDING)], unique=True)
    db.customers.create_index([("orgId", ASCENDING), ("_id", ASCENDING)], unique=True)
    db.outlets.create_index([("orgId", ASCENDING), ("_id", ASCENDING)], unique=True)
    db.products.create_index([("orgId", ASCENDING), ("_id", ASCENDING)], unique=True)

    db.orders.create_index([("orgId", ASCENDING), ("status", ASCENDING), ("createdAt", ASCENDING)])
    db.orders.create_index([("orgId", ASCENDING), ("customerId", ASCENDING)])
    db.orders.create_index([("orgId", ASCENDING), ("outletId", ASCENDING)])
    db.orders.create_index([("orgId", ASCENDING), ("createdByUserId", ASCENDING)])
    db.orders.create_index([("orgId", ASCENDING), ("items.productId", ASCENDING)])

    db.payments.create_index([("orgId", ASCENDING), ("orderId", ASCENDING)])
    db.deliveries.create_index([("orgId", ASCENDING), ("orderId", ASCENDING)])
    db.inventory_moves.create_index([("orgId", ASCENDING), ("productId", ASCENDING), ("createdAt", ASCENDING)])
    db.inventory_moves.create_index([("orgId", ASCENDING), ("outletId", ASCENDING), ("createdAt", ASCENDING)])


def seed_to_mongo(uri: str, db_name: str, drop: bool = False) -> None:
    client = MongoClient(uri)
    db = client[db_name]

    if drop:
        client.drop_database(db_name)

    # Convert Pydantic models to plain dicts
    users = [u.model_dump(by_alias=True) for u in SEED_USERS]
    customers = [c.model_dump(by_alias=True) for c in SEED_CUSTOMERS]
    outlets = [o.model_dump(by_alias=True) for o in SEED_OUTLETS]
    products = [p.model_dump(by_alias=True) for p in SEED_PRODUCTS]
    orders = [o.model_dump(by_alias=True) for o in SEED_ORDERS]
    payments = [p.model_dump(by_alias=True) for p in SEED_PAYMENTS]
    deliveries = [d.model_dump(by_alias=True) for d in SEED_DELIVERIES]
    inventory_moves = [m.model_dump(by_alias=True) for m in SEED_INVENTORY_MOVES]

    # Insert
    _insert_many_safe(db.users, users)
    _insert_many_safe(db.customers, customers)
    _insert_many_safe(db.outlets, outlets)
    _insert_many_safe(db.products, products)
    _insert_many_safe(db.orders, orders)
    _insert_many_safe(db.payments, payments)
    _insert_many_safe(db.deliveries, deliveries)
    _insert_many_safe(db.inventory_moves, inventory_moves)

    ensure_indexes(db)

    print(f"✅ Seeded database '{db_name}' at {uri}")
    print("Collections:", db.list_collection_names())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", type=str, default="mongodb+srv://danilka:4uPwMFlKFheJws5Q@cluster0.nq9yl1i.mongodb.net/?appName=Cluster0")
    ap.add_argument("--db", type=str, default="cakeflow_proto")
    ap.add_argument("--drop", action="store_true", help="Drop DB before seeding")
    args = ap.parse_args()

    seed_to_mongo(args.uri, args.db, drop=args.drop)


if __name__ == "__main__":
    main()



