import argparse
from neo4j import GraphDatabase
from src.config import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

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
    logger.info(">>  Creating Collection nodes...")
    for name in COLLECTIONS:
        tx.run("MERGE (c:Collection {name:$name})", name=name)
        logger.debug(f"    Created/merged Collection: {name}")

    logger.info(">>  Creating Embedded nodes and EMBEDS + REFERS_TO relationships...")
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
        logger.debug(f"    Created Embedded {owner}.{path} with REFERS_TO -> {dst} as {alias}")

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
            logger.debug(f"    Created Embedded {owner}.{path} without REFERS_TO")

    logger.info(">>  Creating REFERS_TO (Collection -> Collection)...")
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
        logger.debug(f"    Created REFERS_TO: {src} -> {dst} as {alias}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=settings.NEO4J_URI)
    ap.add_argument("--user", default=settings.NEO4J_USER)
    ap.add_argument("--password", default=settings.NEO4J_PASSWORD)
    ap.add_argument("--reset", action="store_true", help="Delete all nodes first")
    args = ap.parse_args()

    try:
        driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
        logger.info("[SUCCESS] Connected to Neo4j successfully.")

        # 1. Reset (Optional)
        if args.reset:
            with driver.session() as session:
                session.run(RESET_CYPHER)
                logger.info(">> Database reset complete.")

        # 2. Apply Constraints
        with driver.session() as session:
            for c in CONSTRAINTS_CYPHER:
                session.run(c)
            logger.info(">> Constraints applied.")

        # 3. Load Metadata
        with driver.session() as session:
            session.execute_write(load_metadata)
            logger.info("[SUCCESS] Neo4j metadata graph loaded successfully.")

    except Exception as e:
        logger.critical(f"[ERROR] Failed to load metadata: {e}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
