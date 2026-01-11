"""
neo4j_metadata_loader.py

Loads a metadata-only schema graph into Neo4j for the $lookup approach.

Creates:
- (:Collection {name})
- (:Embedded {owner, path})
- (Collection)-[:REFERS_TO {alias, localField, foreignField}]->(Collection)
- (Collection)-[:EMBEDS]->(Embedded)
- (Embedded)-[:REFERS_TO {alias, localField, foreignField}]->(Collection)

Run:
  python neo4j_metadata_loader.py --uri bolt://localhost:7687 --user neo4j --password password --reset
"""

from pytz.tzinfo import DstTzInfo
import argparse
from neo4j import GraphDatabase

# ----------------------------
# Schema Metadata
# ----------------------------

COLLECTIONS = [
    "users",
    "customers",
    "outlets",
    "products",
    "orders",
    "payments",
    "deliveries",
    "inventory_moves",
]

EMBEDDED = [
    
    ("orders", "items"),
    ("inventory_moves", "ref"),
]

REFERS_TO_COLLECTION = [
    #src,dst,alias, localField, foreignField
    # src is the source collection
    # dst is the destination collection
    # alias is the alias of the destination collection
    # localField is the local field of the source collection
    # foreignField is the foreign field of the destination collection
    ("orders", "customers", "customer", "customerId", "_id"),
    ("orders", "outlets", "outlet", "outletId", "_id"),
    ("orders", "users", "createdBy", "createdByUserId", "_id"),
    ("payments", "orders", "order", "orderId", "_id"),
    ("payments", "customers", "payer", "paidByCustomerId", "_id"),
    ("deliveries", "orders", "order", "orderId", "_id"),
    ("deliveries", "users", "assignedToUserId", "assignedToUserId", "_id"),
    ("inventory_moves", "products", "product", "productId", "_id"),
    ("inventory_moves", "outlets", "outlet", "outletId", "_id"),
]

REFERS_TO_EMBEDDED = [
    
    ("orders", "items", "products", "product", "productId", "_id"),
    ("inventory_moves", "ref", "orders", "order", "orderId", "_id"),
    ("inventory_moves", "ref", "deliveries", "delivery", "deliveryId", "_id"),
]

# ----------------------------
# Cypher helpers
# ----------------------------

RESET_CYPHER = "MATCH (n) DETACH DELETE n;"

CONSTRAINTS_CYPHER = [
    "CREATE CONSTRAINT collection_name IF NOT EXISTS FOR (c:Collection) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT embedded_key IF NOT EXISTS FOR (e:Embedded) REQUIRE (e.owner, e.path) IS UNIQUE",
]

# ----------------------------
# Metadata loader
# ----------------------------

def load_metadata(tx):
    print(">>  Creating Collection nodes...")
    for name in COLLECTIONS:
        tx.run("MERGE (c:Collection {name:$name})", name=name)
        print(f"    Created/merged Collection: {name}")

    print(">>  Creating Embedded nodes and EMNEDS + REFERS_TO relationships...")
    for owner, path, dst, alias, localField, foreignField in REFERS_TO_EMBEDDED:
        tx.run(
            """
            MERGE (c:Collection {name:$owner})
            MERGE (e:Embedded {owner:$owner, path:$path})
            MERGE (c)-[:EMBEDS]->(e)
            MERGE (dst:Collection {name:$dst})
            MERGE (e)-[r:REFERS_TO {alias:$alias}]->(dst)
            SET r.localField = $localField,
                r.foreignField = $foreignField
            """,
            owner=owner,
            path=path,
            dst=dst,
            alias=alias,
            localField=localField,
            foreignField=foreignField,
        )
        print(f"    Created Embedded {owner}.{path} with REFERS_TO -> {dst} as {alias}")

    # Also create any remaining Embedded nodes without REFERS_TO
    for owner, path in EMBEDDED:
        if not any(owner == e[0] and path == e[1] for e in REFERS_TO_EMBEDDED):
            tx.run(
                """
                MERGE (c:Collection {name:$owner})
                MERGE (e:Embedded {owner:$owner, path:$path})
                MERGE (c)-[:EMBEDS]->(e)
                """,
                owner=owner,
                path=path,
            )
            print(f"    Created Embedded {owner}.{path} without REFERS_TO")

    print(">>  Creating REFERS_TO (Collection -> Collection)...")
    for src, dst, alias, localField, foreignField in REFERS_TO_COLLECTION:
        tx.run(
            """
            MATCH (src:Collection {name:$src})
            MATCH (dst:Collection {name:$dst})
            MERGE (src)-[r:REFERS_TO {alias:$alias}]->(dst)
            SET r.localField = $localField,
                r.foreignField = $foreignField
            """,
            src=src,
            dst=dst,
            alias=alias,
            localField=localField,
            foreignField=foreignField,
        )
        print(f"    Created REFERS_TO: {src} -> {dst} as {alias}")

# ----------------------------
# Main entry
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default="neo4j+s://d7382db4.databases.neo4j.io")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", required=True)
    ap.add_argument("--reset", action="store_true", help="Delete all nodes first (USE ONLY on empty dev DB)")
    args = ap.parse_args()

    try:
        driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
        print("[SUCCESS] Connected to Neo4j successfully.")

        # 1. Reset (Optional)
        if args.reset:
            with driver.session() as session:
                session.run(RESET_CYPHER)
                print(">> Database reset complete.")

        # 2. Apply Constraints
        with driver.session() as session:
            for c in CONSTRAINTS_CYPHER:
                session.run(c)
            print(">> Constraints applied.")

        # 3. Load Metadata
        with driver.session() as session:
            session.execute_write(load_metadata)
            print("[SUCCESS] Neo4j metadata graph loaded successfully.")

        # 4. Debug: show what was created
        with driver.session() as session:
            print("\n Collections:")
            for record in session.run("MATCH (c:Collection) RETURN c.name AS name"):
                print("   -", record["name"])

            print("\n Embedded nodes and EMNEDS relationships:")
            for record in session.run("""
                MATCH (c:Collection)-[r:EMBEDS]->(e:Embedded)
                RETURN c.name AS collection, e.path AS embedded
            """):
                print(f"   - {record['collection']} -> {record['embedded']}")

            print("\n REFERS_TO relationships (Collection -> Collection):")
            for record in session.run("""
                MATCH (src:Collection)-[r:REFERS_TO]->(dst:Collection)
                RETURN src.name AS src, r.alias AS alias, dst.name AS dst, r.localField AS localField, r.foreignField AS foreignField
            """):
                print(f"   - {record['src']}.{record['localField']} -> {record['dst']}.{record['foreignField']} as {record['alias']}")

            print("\n REFERS_TO relationships (Embedded -> Collection):")
            for record in session.run("""
                MATCH (e:Embedded)-[r:REFERS_TO]->(c:Collection)
                RETURN e.owner AS owner, e.path AS embedded, r.alias AS alias, c.name AS dst, r.localField AS localField, r.foreignField AS foreignField
            """):
                print(f"   - Embedded {record['owner']}.{record['embedded']}.{record['localField']} -> {record['dst']}.{record['foreignField']} as {record['alias']}")

    except Exception as e:
        print(f"[ERROR] Failed to load metadata: {e}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
