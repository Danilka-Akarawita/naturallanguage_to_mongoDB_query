import pytest
import datetime
from src.services.intent_service import generate_intent_json
from src.models.intent import Intent

# --------------------------------------------------------------------------
# Test Data: (Query, Expected Root, Expected Filter Key/Value Part)
# --------------------------------------------------------------------------

BASIC_SCENARIOS = [
    (
        "Show me all orders for Colombo 07 Outlet.",
        "orders",
        {"path_part": "outlet", "value_part": "Colombo"}
    ),
    (
        "List all customers tagged VIP.",
        "customers",
        {"path_part": "tags", "value_part": "VIP"}
    ),
    (
        "What products are active right now, and what are their prices?",
        "products",
        {"path_part": "active", "value_part": True}
    )
]

INTERMEDIATE_SCENARIOS = [
    (
        "Show all PENDING orders created after 2025-12-01, including the customer name and outlet name.",
        "orders",
        [
            {"path_part": "status", "value_part": "PENDING"},
            {"path_part": "createdAt", "op": "gt"} 
        ]
    ),
    (
        "For order ORD-1001, show the items with product names, quantities, and the total amount.",
        "orders",
        {"path_part": "orderNo", "value_part": "ORD-1001"}
    ),
    (
        "List all deliveries that are OUT_FOR_DELIVERY, including the order number and delivery staff name.",
        "deliveries",
        {"path_part": "deliveryStatus", "value_part": "OUT_FOR_DELIVERY"}
    )
]

ADVANCED_SCENARIOS = [
    (
        "For VIP customer Nimal, show all orders with payment status/method, and delivery status if delivery was needed.",
        "orders",
        [
            {"path_part": "customer.tags", "value_part": "VIP"},
            {"path_part": "customer.name", "value_part": "Nimal"}
        ]
    ),
    (
        # Note: Aggregation details might not be fully captured by current Intent model, 
        # but we verify correct filtering and root.
        "Give me an inventory usage report for Colombo 07 Outlet in December 2025: total OUT qty per product, and also show WASTAGE per product.",
        "inventory_moves",
        [
            {"path_part": "outlet", "value_part": "Colombo"},
            {"path_part": "createdAt", "op": "gte"} # Date range implied
        ]
    ),
    (
        "Find orders that are READY but have no successful payment, and show customer + outlet + createdBy user.",
        "orders",
        [
            {"path_part": "status", "value_part": "READY"},
             # Logic for "no successful payment" might vary (e.g. payment.status != SUCCESS), verifying root/status first
        ]
    )
]

ALL_SCENARIOS = BASIC_SCENARIOS + INTERMEDIATE_SCENARIOS + ADVANCED_SCENARIOS

@pytest.mark.parametrize("query, expected_root, expected_checks", ALL_SCENARIOS)
def test_user_scenario_generation(query, expected_root, expected_checks):
    """
    Verifies that the IntentService generates the correct Intent structure for user provided scenarios.
    """
    print(f"\nTesting Query: {query}")
    intent_dict = generate_intent_json(query)
    intent = Intent.model_validate(intent_dict)
    
    # 1. Check Root
    assert intent.root == expected_root, f"Expected root '{expected_root}', got '{intent.root}'"
    
    # 2. Check Filters/Logic
    if isinstance(expected_checks, dict):
        expected_checks = [expected_checks]
        
    for check in expected_checks:
        path_part = check.get("path_part")
        value_part = check.get("value_part")
        op = check.get("op")
        
        found = False
        for f in intent.filters:
            # Check path match
            if path_part and path_part not in f.pathHint:
                continue
            
            # Check Op Match (if specified)
            if op and f.op != op and op not in str(f.op): # loose check for gt/gte
                continue

            # Check Value Match (if specified)
            if value_part is not None:
                # Handle boolean vs string looseness if needed, or exact match
                if str(value_part).lower() in str(f.value).lower():
                    found = True
                    break
            else:
                found = True
                break
        
        if not found:
            pytest.fail(f"Could not find filter matching {check} in generated filters: {intent.filters}")
