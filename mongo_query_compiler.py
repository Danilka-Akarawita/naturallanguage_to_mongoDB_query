"""
mongo_query_compiler.py

Production-ready MongoDB aggregation compiler driven by Neo4j schema metadata.
Handles collection joins, embedded joins, deduplication, and proper unwind stages.
"""

from __future__ import annotations
import argparse
import json
import logging
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from neo4j import GraphDatabase, Driver
from pymongo import MongoClient

# ------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("neo4j").setLevel(logging.WARNING)
logger = logging.getLogger("mongo_query_compiler")

# ------------------------------------------------------------------
# Data Models
# ------------------------------------------------------------------

@dataclass
class JoinRecipe:
    kind: str                    # "collection" | "embedded"
    src_collection: str
    alias: str
    dst_collection: Optional[str]
    local_field: Optional[str]
    foreign_field: Optional[str]
    target_path: Optional[str] = None
    lookup_local_field: Optional[str] = None
    array_path: Optional[str] = None


class QueryCompilationError(Exception):
    """Raised when query compilation fails."""

# ------------------------------------------------------------------
# Intent Loading
# ------------------------------------------------------------------

def load_intent(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        raise QueryCompilationError(f"Failed to load intent file: {exc}") from exc


def extract_potential_paths(intent: Dict[str, Any]) -> Set[str]:
    used: Set[str] = set()
    for section in ("select", "filters", "sort"):
        for item in intent.get(section, []):
            path = item if isinstance(item, str) else item.get("pathHint") or item.get("field")
            if not path:
                continue
            parts = path.split(".")
            for i in range(1, len(parts) + 1):
                used.add(".".join(parts[:i]))
    logger.info("Potential paths extracted: %s", used)
    return used

# ------------------------------------------------------------------
# Fetch Join Recipes from Neo4j
# ------------------------------------------------------------------

def fetch_join_recipes(driver: Driver, root_collection: str) -> List[JoinRecipe]:
    recipes: List[JoinRecipe] = []

    with driver.session() as session:
        # 1️⃣ Collection joins
        collection_records = session.run("""
            MATCH p=(root:Collection {name:$root})-[:REFERS_TO*0..]->(c:Collection)
            UNWIND relationships(p) AS r
            RETURN startNode(r).name AS src, endNode(r).name AS dst,
                   r.alias AS alias, r.localField AS localField, r.foreignField AS foreignField
        """, root=root_collection)

        for rec in collection_records:
            jr = JoinRecipe(
                kind="collection",
                src_collection=rec["src"],
                dst_collection=rec["dst"],
                alias=rec["alias"],
                local_field=rec["localField"],
                foreign_field=rec["foreignField"]
            )
            recipes.append(jr)

        # Compute fully qualified target paths
        def resolve_paths(jr: JoinRecipe):
            if jr.target_path:
                return  # already set
            if jr.src_collection == root_collection:
                jr.target_path = jr.alias
                jr.lookup_local_field = jr.local_field
            else:
                parent = next((r for r in recipes if r.dst_collection == jr.src_collection), None)
                if parent:
                    resolve_paths(parent)
                    jr.target_path = f"{parent.target_path}.{jr.alias}"
                    jr.lookup_local_field = f"{parent.target_path}.{jr.local_field}"
                else:
                    jr.target_path = jr.alias
                    jr.lookup_local_field = jr.local_field

        for jr in recipes:
            resolve_paths(jr)

        # 2️⃣ Embedded joins
        collections_to_check = [root_collection] + [r.dst_collection for r in recipes if r.kind == "collection"]
        for col in collections_to_check:
            embedded_records = session.run("""
                MATCH (c:Collection {name:$col})-[:EMBEDS]->(e:Embedded)-[r:REFERS_TO]->(dst:Collection)
                RETURN e.path AS array_path, r.alias AS alias,
                       dst.name AS dst_collection,
                       r.localField AS local_field, r.foreignField AS foreign_field
            """, col=col)

            for rec in embedded_records:
                parent = next((r for r in recipes if r.dst_collection == col and r.kind == "collection"), None)
                full_array_path = f"{parent.target_path}.{rec['array_path']}" if parent else rec['array_path']

                jr = JoinRecipe(
                    kind="embedded",
                    src_collection=col,
                    alias=rec["alias"],
                    dst_collection=rec["dst_collection"],
                    local_field=rec["local_field"],
                    foreign_field=rec["foreign_field"],
                    array_path=full_array_path
                )
                recipes.append(jr)

        # Root-level embedded arrays without REFERS_TO
        embedded_root = session.run("""
            MATCH (c:Collection {name:$root})-[:EMBEDS]->(e:Embedded)
            WHERE NOT (e)-[:REFERS_TO]->(:Collection)
            RETURN e.path AS array_path
        """, root=root_collection)

        for rec in embedded_root:
            jr = JoinRecipe(
                kind="embedded",
                src_collection=root_collection,
                alias=rec["array_path"],
                dst_collection=None,
                local_field=None,
                foreign_field=None,
                array_path=rec["array_path"]
            )
            recipes.append(jr)

    # Deduplicate joins
    seen = set()
    final_recipes = []
    for r in recipes:
        key = (r.target_path or r.alias, r.array_path)
        if key not in seen:
            seen.add(key)
            final_recipes.append(r)

    logger.info("Total join recipes fetched: %d", len(final_recipes))
    return final_recipes

# ------------------------------------------------------------------
# Compile MongoDB Aggregation Pipeline
# ------------------------------------------------------------------

def compile_match(filters: List[Dict[str, Any]], join_recipes: List[JoinRecipe]) -> Dict[str, Any]:
    match: Dict[str, Any] = {}
    rewrite_map = {f"{r.array_path}.{r.alias}": r.alias for r in join_recipes if r.kind == "embedded" and r.array_path}

    for f in filters:
        path = f["pathHint"]
        for logical, alias in rewrite_map.items():
            if path.startswith(logical):
                logger.debug("Rewriting path '%s' -> '%s'", path, alias)
                path = path.replace(logical, alias, 1)
                break
        op = f.get("op", "eq")
        val = f["value"]
        match[path] = (
            val if op == "eq" else
            {"$ne": val} if op == "neq" else
            {"$gt": val} if op == "gt" else
            {"$gte": val} if op == "gte" else
            {"$lt": val} if op == "lt" else
            {"$lte": val} if op == "lte" else
            {"$in": val} if op == "in" else val
        )
    logger.debug("Compiled $match stage: %s", match)
    return match


def compile_pipeline(intent: Dict[str, Any], join_recipes: List[JoinRecipe]) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    unwound: Set[str] = set()

    # Split filters into pre/post
    lookup_paths = {r.target_path for r in join_recipes if r.kind == "collection"}
    pre_filters, post_filters = [], []
    for f in intent.get("filters", []):
        path = f["pathHint"]
        if any(path.startswith(lp + ".") or path == lp for lp in lookup_paths):
            post_filters.append(f)
        else:
            pre_filters.append(f)

    if pre_filters:
        pipeline.append({"$match": compile_match(pre_filters, join_recipes)})

    # Sort joins: collection first
    join_recipes_sorted = sorted(join_recipes, key=lambda r: r.kind != "collection")

    for r in join_recipes_sorted:
        if r.kind == "collection":
            if r.target_path not in unwound:
                pipeline.append({
                    "$lookup": {
                        "from": r.dst_collection,
                        "localField": r.lookup_local_field,
                        "foreignField": r.foreign_field,
                        "as": r.target_path
                    }
                })
                pipeline.append({
                    "$unwind": {"path": f"${r.target_path}", "preserveNullAndEmptyArrays": True}
                })
                unwound.add(r.target_path)
        else:
            # embedded
            if r.array_path and r.array_path not in unwound:
                pipeline.append({
                    "$unwind": {"path": f"${r.array_path}", "preserveNullAndEmptyArrays": True}
                })
                unwound.add(r.array_path)
            if r.dst_collection:
                pipeline.append({
                    "$lookup": {
                        "from": r.dst_collection,
                        "localField": f"{r.array_path}.{r.local_field}" if r.array_path else r.local_field,
                        "foreignField": r.foreign_field,
                        "as": r.alias
                    }
                })
                pipeline.append({
                    "$unwind": {"path": f"${r.alias}", "preserveNullAndEmptyArrays": True}
                })

    if post_filters:
        pipeline.append({"$match": compile_match(post_filters, join_recipes)})

    if intent.get("aggregation") == "count":
        pipeline.append({"$count": "total"})

    logger.info("Final pipeline compiled:\n%s", json.dumps(pipeline, indent=2))
    return pipeline

# ------------------------------------------------------------------
# Execute Mongo Pipeline
# ------------------------------------------------------------------

def run_pipeline(uri: str, db: str, collection: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        with MongoClient(uri) as client:
            return list(client[db][collection].aggregate(pipeline))
    except Exception as exc:
        raise QueryCompilationError(f"MongoDB execution failed: {exc}") from exc

# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mongo_uri", required=True)
    ap.add_argument("--mongo_db", required=True)
    ap.add_argument("--neo4j_uri", required=True)
    ap.add_argument("--neo4j_user", default="neo4j")
    ap.add_argument("--neo4j_password", required=True)
    ap.add_argument("--intent_file", required=True)
    ap.add_argument("--print_pipeline", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    try:
        intent = load_intent(args.intent_file)
        paths = extract_potential_paths(intent)

        driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
        with driver:
            joins = fetch_join_recipes(driver, intent["root"])
            pipeline = compile_pipeline(intent, joins)

        if args.print_pipeline:
            print(json.dumps(pipeline, indent=2))

        if args.execute:
            results = run_pipeline(args.mongo_uri, args.mongo_db, intent["root"], pipeline)
            print(json.dumps(results, indent=2, default=str))

    except QueryCompilationError as exc:
        logger.error(exc)
        sys.exit(1)

if __name__ == "__main__":
    main()
