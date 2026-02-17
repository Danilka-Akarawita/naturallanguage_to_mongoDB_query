from datetime import datetime
from typing import Literal, Optional, List
from pydantic import BaseModel, Field, ConfigDict

# ----------------------------
# Enums
# ----------------------------
UserRole = Literal["ADMIN", "OUTLET_STAFF", "DELIVERY_STAFF", "FACTORY_STAFF"]
ProductCategory = Literal["CAKE", "TOPPING", "PACKAGING"]
OrderStatus = Literal["PENDING", "IN_PRODUCTION", "READY", "DELIVERED", "CANCELLED"]
PaymentMethod = Literal["CASH", "CARD", "BANK"]
PaymentStatus = Literal["SUCCESS", "FAILED", "REFUNDED"]
DeliveryStatus = Literal["PENDING", "OUT_FOR_DELIVERY", "DELIVERED", "FAILED"]
InventoryMoveType = Literal["IN", "OUT", "WASTAGE", "TRANSFER"]

# ----------------------------
# 1) users
# ----------------------------
class User(BaseModel):
    """User model representing application users."""
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
    """Customer model."""
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
    """Outlet model."""
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    name: str
    city: str
    orgId: str

# ----------------------------
# 4) products
# ----------------------------
class Product(BaseModel):
    """Product model."""
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
class OrderItem(BaseModel):
    """Item within an order."""
    model_config = ConfigDict(extra="forbid")
    productId: str  # ref -> products._id
    qty: int
    unitPrice: float

class OrderCustomization(BaseModel):
    """Customization options for an order."""
    model_config = ConfigDict(extra="forbid")
    messageOnCake: Optional[str] = None
    theme: Optional[str] = None
    notes: Optional[str] = None

class Order(BaseModel):
    """Order model."""
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
class Payment(BaseModel):
    """Payment model."""
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
class Delivery(BaseModel):
    """Delivery model."""
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
class InventoryRef(BaseModel):
    """Reference for inventory movement."""
    model_config = ConfigDict(extra="forbid")
    orderId: Optional[str] = None     # ref -> orders._id
    deliveryId: Optional[str] = None  # ref -> deliveries._id

class InventoryMove(BaseModel):
    """Inventory movement model."""
    model_config = ConfigDict(extra="forbid")
    id: str = Field(alias="_id")
    productId: str          # ref -> products._id
    outletId: str           # ref -> outlets._id
    type: InventoryMoveType
    qty: int
    ref: InventoryRef = Field(default_factory=InventoryRef)
    createdAt: datetime
    orgId: str
