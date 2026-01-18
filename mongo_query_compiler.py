"""
mongo_query_compiler.py

Production-ready MongoDB aggregation compiler with full support for nested embedded paths
driven by Neo4j schema metadata.
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
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("neo4j").setLevel(logging.WARNING)
logger = logging.getLogger("mongo_query_compiler")

# ------------------------------------------------------------------
# Data Models
# ------------------------------------------------------------------

@dataclass(frozen=True)
class JoinRecipe:
    kind: str                    # "collection" | "embedded"
    src_collection: str
    alias: str
    dst_collection: str
    local_field: str
    foreign_field: str
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
    """
    Extract all dot-notation prefixes from select, filters, and sort.
    Handles arbitrarily deep paths.
    """
    used: Set[str] = set()
    for section in ("select", "filters", "sort"):
        for item in intent.get(section, []):
            path = item if isinstance(item, str) else item.get("pathHint") or item.get("field")
            if not path:
                continue
            parts = path.split(".")
            for i in range(1, len(parts)+1):
                used.add(".".join(parts[:i]))
    logger.info("Potential paths extracted: %s", used)
    return used

# ------------------------------------------------------------------
# Neo4j Metadata Queries
# ------------------------------------------------------------------

CY_VERIFY_REFERS_CHAIN = """
MATCH (start:Collection {name:$root})
MATCH p = (start)-[:REFERS_TO*]->(end)
WHERE [r IN relationships(p) | r.alias] = $segments
RETURN
  [n IN nodes(p) | n.name] AS collections,
  [r IN relationships(p) | {
     alias: r.alias,
     localField: r.localField,
     foreignField: r.foreignField
  }] AS rels
"""

CY_DISCOVER_EMBEDDED = """
MATCH (src:Collection {name:$root})-[:EMBEDS*1..]->(e:Embedded)
MATCH (e)-[r:REFERS_TO]->(dst:Collection)
WHERE e.path + '.' + r.alias IN $candidates
RETURN
  e.path AS array_path,
  r.alias AS alias,
  dst.name AS dst_collection,
  r.localField AS local_field,
  r.foreignField AS foreign_field
"""

def fetch_join_recipes(
    driver: Driver,
    root_collection: str,
    potential_paths: Set[str],
) -> List[JoinRecipe]:

    recipes: Dict[str, JoinRecipe] = {}

    with driver.session() as session:
        # Collection-to-collection joins
        for path in sorted(potential_paths, key=lambda p: p.count(".")):
            segments = path.split(".")
            record = session.run(
                CY_VERIFY_REFERS_CHAIN,
                root=root_collection,
                segments=segments,
            ).single()
            logger.debug("Record for path %s: %s", path, record)
            if not record:
                continue

            collections = record["collections"]
            rels = record["rels"]
            current_prefix = ""
            for idx, rel in enumerate(rels):
                alias = rel["alias"]
                target_path = alias if idx == 0 else f"{current_prefix}.{alias}"
                lookup_local = rel["localField"] if idx == 0 else f"{current_prefix}.{rel['localField']}"
                if target_path not in recipes:
                    recipes[target_path] = JoinRecipe(
                        kind="collection",
                        src_collection=collections[idx],
                        dst_collection=collections[idx+1],
                        alias=alias,
                        local_field=rel["localField"],
                        foreign_field=rel["foreignField"],
                        target_path=target_path,
                        lookup_local_field=lookup_local
                    )
                current_prefix = target_path

        # Embedded joins (multi-level)
        embedded = session.run(
            CY_DISCOVER_EMBEDDED,
            root=root_collection,
            candidates=list(potential_paths),
        )

        for rec in embedded:
            key = f"embedded:{rec['array_path']}.{rec['alias']}"
            if key not in recipes:
                recipes[key] = JoinRecipe(
                    kind="embedded",
                    src_collection=root_collection,
                    alias=rec["alias"],
                    dst_collection=rec["dst_collection"],
                    local_field=rec["local_field"],
                    foreign_field=rec["foreign_field"],
                    array_path=rec["array_path"]
                )

    return list(recipes.values())

# ------------------------------------------------------------------
# MongoDB Pipeline Compilation
# ------------------------------------------------------------------

def compile_match(filters: List[Dict[str, Any]], join_recipes: List[JoinRecipe]) -> Dict[str, Any]:
    match: Dict[str, Any] = {}
    rewrite_map = {f"{r.array_path}.{r.alias}": r.alias for r in join_recipes if r.kind=="embedded" and r.array_path}
    for f in filters:
        path = f["pathHint"]
        for logical, alias in rewrite_map.items():
            if path.startswith(logical):
                path = path.replace(logical, alias, 1)
                break
        op = f.get("op", "eq")
        val = f["value"]
        match[path] = (
            val if op=="eq" else
            {"$ne": val} if op=="neq" else
            {"$gt": val} if op=="gt" else
            {"$gte": val} if op=="gte" else
            {"$lt": val} if op=="lt" else
            {"$lte": val} if op=="lte" else
            {"$in": val} if op=="in" else
            {"$regex": val, "$options": "i"} if op=="contains" else
            val
        )
    return match


def compile_pipeline(intent: Dict[str, Any], join_recipes: List[JoinRecipe]) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    lookup_aliases = {r.alias for r in join_recipes}
    pre_filters, post_filters = [], []
    for f in intent.get("filters", []):
        parts = f["pathHint"].split(".")
        if any(p in lookup_aliases for p in parts):
            post_filters.append(f)
        else:
            pre_filters.append(f)
    if pre_filters:
        pipeline.append({"$match": compile_match(pre_filters, join_recipes)})

    # Sort joins: collections first, then embedded by array path depth
    join_recipes = sorted(
        join_recipes,
        key=lambda r: (r.kind!="collection", r.target_path.count(".") if r.target_path else 0)
    )

    unwound: Set[str] = set()
    for r in join_recipes:
        if r.kind == "collection":
            pipeline += [
                {
                    "$lookup": {
                        "from": r.dst_collection,
                        "localField": r.lookup_local_field,
                        "foreignField": r.foreign_field,
                        "as": r.target_path
                    }
                },
                {"$unwind": {"path": f"${r.target_path}", "preserveNullAndEmptyArrays": True}}
            ]
        else:
            # multi-level unwind
            if r.array_path not in unwound:
                levels = r.array_path.split(".")
                path_acc = ""
                for lvl in levels:
                    path_acc = f"{path_acc}.{lvl}" if path_acc else lvl
                    if path_acc not in unwound:
                        pipeline.append({"$unwind": {"path": f"${path_acc}", "preserveNullAndEmptyArrays": True}})
                        unwound.add(path_acc)
            pipeline += [
                {
                    "$lookup": {
                        "from": r.dst_collection,
                        "localField": f"{r.array_path}.{r.local_field}",
                        "foreignField": r.foreign_field,
                        "as": r.alias
                    }
                },
                {"$unwind": {"path": f"${r.alias}", "preserveNullAndEmptyArrays": True}}
            ]

    if post_filters:
        pipeline.append({"$match": compile_match(post_filters, join_recipes)})

    if intent.get("aggregation") == "count":
        pipeline.append({"$count": "total"})

    return pipeline

# ------------------------------------------------------------------
# Execution
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
            joins = fetch_join_recipes(driver, intent["root"], paths)
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
