"""
test_mongo_compiler.py

Unit tests for mongo_query_compiler.py using mocked Neo4j responses.
"""
import unittest
import json
from mongo_query_compiler import compile_pipeline, JoinRecipe

class TestMongoCompiler(unittest.TestCase):
    
    def test_double_unwind_prevention(self):
        """
        Scenario: Intent requests two fields from the SAME embedded array.
        e.g. deliveries.driver.name AND deliveries.vehicle.type
        Goal: Verify '$unwind': '$deliveries' appears ONLY ONCE.
        """
        intent = {
            "root": "Order",
            "filters": [],
            "select": ["deliveries.driver.name", "deliveries.vehicle.type"]
        }
        
        # Simulating what Neo4j would return
        recipes = [
            JoinRecipe(kind="embedded", src_collection="Order", alias="driver", 
                       dst_collection="users", local_field="driverId", foreign_field="_id",
                       array_path="deliveries"),
            JoinRecipe(kind="embedded", src_collection="Order", alias="vehicle", 
                       dst_collection="vehicles", local_field="vehicleId", foreign_field="_id",
                       array_path="deliveries"),
        ]
        
        pipeline = compile_pipeline(intent, recipes)
        
        # Count unwinds of "deliveries"
        unwind_counts = 0
        for stage in pipeline:
            if "$unwind" in stage:
                path = stage["$unwind"].get("path")
                if path == "$deliveries":
                    unwind_counts += 1
        
        print(f"\n[Test Double Unwind] Pipeline stages: {len(pipeline)}")
        print(json.dumps(pipeline, indent=2))
        
        self.assertEqual(unwind_counts, 1, f"Expected 1 unwind for 'deliveries', found {unwind_counts}")

    def test_filter_placement(self):
        """
        Scenario: Filter on embedded field (no join) and joined field.
        Goal: Verify embedded field filter is 'Pre-Lookup' (early in pipeline).
        """
        intent = {
            "root": "Order",
            "filters": [
                {"pathHint": "deliveries.status", "value": "pending"}, # Embedded field, no join needed
                {"pathHint": "driver.name", "value": "John"}           # Joined field
            ],
            "select": []
        }
        
        # The recipes only know about 'driver' alias
        recipes = [
            JoinRecipe(kind="embedded", src_collection="Order", alias="driver", 
                       dst_collection="users", local_field="driverId", foreign_field="_id",
                       array_path="deliveries"),
        ]
        
        pipeline = compile_pipeline(intent, recipes)
        
        print(f"\n[Test Filter Placement] Pipeline stages: {len(pipeline)}")
        print(json.dumps(pipeline, indent=2))
        
        # Check order
        # We expect:
        # 1. $match (deliveries.status)
        # 2. $unwind deliveries
        # 3. $lookup driver
        # 4. $match (driver.name)
        
        self.assertEqual(pipeline[0].get("$match", {}).get("deliveries.status"), "pending", "First stage should be local match")
        self.assertEqual(pipeline[-1].get("$match", {}).get("driver.name"), "John", "Last stage should be post-lookup match")

if __name__ == '__main__':
    unittest.main()
