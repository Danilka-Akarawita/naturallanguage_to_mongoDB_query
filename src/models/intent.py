from typing import Literal, List, Union, Optional
from pydantic import BaseModel, Field, ConfigDict
Op = Literal[
    "eq", "ne", "gt", "gte", "lt", "lte",
    "between", "in",
    "contains", "starts_with", "ends_with"
]

SortDir = Literal["asc", "desc"]

AggregationType = Literal["count", "group"]



class Filter(BaseModel):
    """Filter condition for the query."""
    model_config = ConfigDict(extra="forbid")

    pathHint: str = Field(
        ...,
        description="Field path requested by user, e.g. 'status', 'outlet.name', 'items.product.name'"
    )
    op: Op
    value: Union[str, int, float, bool, List[Union[str, int, float, bool]]]


class Sort(BaseModel):
    """Sort instruction."""
    model_config = ConfigDict(extra="forbid")

    pathHint: str
    dir: SortDir


class GroupOperation(BaseModel):
    """A single group operation like sum, avg, count."""
    model_config = ConfigDict(extra="forbid")

    op: Literal["count", "sum", "avg", "min", "max"] = Field(..., description="Aggregation operation")
    field: Optional[str] = Field(None, description="Field to aggregate (not needed for count)")


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
    aggregation: Optional[Literal["count"]] = Field(
        None,
        description="Use 'count' when user asks how many / total number"
    )
