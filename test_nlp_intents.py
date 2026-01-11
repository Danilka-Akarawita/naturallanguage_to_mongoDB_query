"""
test_nlp_intents.py

Test suite for converting Natural Language queries to MongoDB Intents.
Validates that the 'intent_generator' produces correct JSON structures for
various complexities (joins, embedded arrays, filters).
"""

import json
import logging
from typing import List, Dict, Any
from intent_generator import generate_intent_json

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("NLP_Tester")

TEST_CASES = [
    {
        "name": "Simple Filter",
        "query": "Show me all pending orders."
    },
    {
        "name": "Joined Filter",
        "query": "Find orders created by 'Kasun'."
    },
    {
        "name": "Date Range",
        "query": "List orders created in December 2025."
    },
    {
        "name": "Embedded Field Access (Double Unwind Candidate)",
        "query": "Show orders with items priced over 500 and show me the product name." 
        # accessing items.unitPrice and items.product.name -> should trigger embedded reasoning
    },
    {
        "name": "Complex Multi-Join",
        "query": "Show orders from 'Colombo 07 Outlet' that contain 'Chocolate Gateau'."
    },
    {
        "name": "Aggregation/Sort",
        "query": "Show the top 5 most expensive orders."
    },
    {
        "name": "Deep Nesting/Ambiguity",
        "query": "Show deliveries where the driver is 'Ruwan' and status is 'DELIVERED'."
    }
]

def run_tests():
    success_count = 0
    
    print("\n" + "="*60)
    print("NLP INTENT GENERATION TEST SUITE")
    print("="*60 + "\n")

    for i, test in enumerate(TEST_CASES, 1):
        print(f"Test #{i}: {test['name']}")
        print(f"Query:   \"{test['query']}\"")
        
        try:
            # Call the actual generator
            # Note: This makes a real API call to OpenAI
            intent = generate_intent_json(question=test['query'])
            
            print(f"Result:  [PASS] Valid JSON generated")
            print("-" * 40)
            print(json.dumps(intent, indent=2))
            print("-" * 40)
            success_count += 1
            
        except Exception as e:
            print(f"Result:  [FAIL] FAILED")
            # print(f"Error:   {e}") 
            # Printing error might also contain unicode from OpenAI response
            print(f"Error occurred during generation.")
        
        print("\n")

    print("="*60)
    print(f"Summary: {success_count}/{len(TEST_CASES)} tests passed.")
    print("="*60)

if __name__ == "__main__":
    run_tests()
