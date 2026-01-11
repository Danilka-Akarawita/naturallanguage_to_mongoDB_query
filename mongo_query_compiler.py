"""
mongo_query_compiler.py

Debug-friendly MongoDB aggregation compiler driven by Neo4j schema metadata.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set

from neo4j import GraphDatabase, Driver
from pymongo import MongoClient

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("neo4j").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Data Structures
# ------------------------------------------------------------------

@dataclass(frozen=True)
class JoinRecipe:
    kind: str                   # "collection" | "embedded"
    src_collection: str
    alias: str
    dst_collection: str
    local_field: str
    foreign_field: str
    array_path: Optional[str] = None
    target_path: Optional[str] = None # Full dot-path e.g. "order.customer"
    lookup_local_field: Optional[str] = None # Full path to local field e.g. "order.customerId"

class QueryCompilationError(Exception):
    """Custom exception for query compilation failures."""
    pass

# ------------------------------------------------------------------
# 1) Intent Handling
# ------------------------------------------------------------------

def load_intent(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            intent = json.load(f)
        logger.info("Intent loaded successfully")
        return intent
    except Exception as exc:
        raise QueryCompilationError(f"Failed to load intent file: {exc}") from exc


def extract_potential_paths(intent: Dict[str, Any]) -> Set[str]:
    """
    Extracts all dot-notation paths referenced in the intent (select, filters, sort).
    Returns a set of unique potential paths (e.g., 'deliveries.driver', 'createdBy').
    """
    root = intent["root"]
    used_paths: Set[str] = set()

    logger.debug("--- extract_potential_paths ---")
    logger.debug(f"Root: {root}")

    for section in ("select", "filters", "sort"):
        items = intent.get(section, [])
        logger.debug(f"Scanning section '{section}': {items}")
        for item in items:
            if isinstance(item, str):
                path = item
            else:
                 # filters use 'pathHint', sort uses 'field'
                path = item.get("pathHint") or item.get("field")
            parts = path.split(".")
            if len(parts) >= 1:
                used_paths.add(parts[0])
            if len(parts) >= 2:
                used_paths.add(f"{parts[0]}.{parts[1]}")

    logger.debug("Extracted potential paths: %s", used_paths)
    return used_paths

# ------------------------------------------------------------------
# 2) Neo4j Metadata Queries (Dynamic Discovery)
# ------------------------------------------------------------------

# Recursive path check
CY_VERIFY_CHAIN = """
MATCH (start:Collection {name: $root})
// We want to find a path of relationships that matches the alias sequence
// This is a bit complex in pure Cypher 3.5/4.x without APOC, so we iterate in Python 
// or use a path matching pattern.
// Simplified assumption for Prototype: All relationships are direct REFERS_TO for chaining.
// Mixed EMBEDS/REFERS_TO chains are harder. 
// For this request, we strictly look for REFERS_TO chains.

MATCH p = (start)-[:REFERS_TO*]->(end)
WHERE [r IN relationships(p) | r.alias] = $segments
RETURN length(p) as depth,
       [N IN nodes(p) | N.name] as collections,
       [R IN relationships(p) | {
            alias: R.alias,
            localField: R.localField,
            foreignField: R.foreignField
       }] as rels
"""

def fetch_join_recipes(
    driver: Driver,
    root_collection: str,
    potential_paths: Set[str]
) -> List[JoinRecipe]:
    """
    Discover joins including deep/transitive ones (e.g. order.customer).
    """
    recipes_map: Dict[str, JoinRecipe] = {} # Keyed by target_path to deduplicate

    with driver.session() as session:
        # Check every potential path to see if it's a valid chain
        # e.g. "order", "order.customer", "order.customer.name"
        # We only care about paths that *might* be joins.
        
        # Sort paths by length so we process 'order' before 'order.customer'
        sorted_paths = sorted(list(potential_paths), key=lambda s: s.count('.'))
        
        for path in sorted_paths:
            segments = path.split(".")
            # We try to validate if this path (or a prefix of it) represents a chain of joins
            # Optimization: only check if it looks like a chain (not ending in scalar field).
            # But we don't know which is scalar. So we check all. 
            
            # Neo4j check
            try:
                result = session.run(CY_VERIFY_CHAIN, root=root_collection, segments=segments)
                record = result.single()
                
                if record:
                    # It's a valid chain!
                    # Construct recipes for each step in the chain
                    colls = record["collections"] # [Root, Step1, Step2...]
                    rels = record["rels"]         # [Rel1, Rel2...]
                    
                    current_path_prefix = ""
                    
                    for i, rel in enumerate(rels):
                        # src is colls[i], dst is colls[i+1]
                        # alias is rel['alias']
                        alias = rel['alias']
                        
                        # Target path accumulates: "order", then "order.customer"
                        if i == 0:
                            target_path = alias
                            lookup_local = rel['localField'] # e.g. orderId
                        else:
                            target_path = f"{current_path_prefix}.{alias}"
                            lookup_local = f"{current_path_prefix}.{rel['localField']}"
                        
                        if target_path not in recipes_map:
                            # Create recipe
                            recipe = JoinRecipe(
                                kind="collection", # Simplified: assuming REFERS_TO is collection join
                                src_collection=colls[i],
                                alias=alias,
                                dst_collection=colls[i+1],
                                local_field=rel['localField'],
                                foreign_field=rel['foreignField'],
                                target_path=target_path,
                                lookup_local_field=lookup_local
                            )
                            recipes_map[target_path] = recipe
                            logger.debug("Found transitive recipe: %s", recipe)
                        
                        current_path_prefix = target_path
                        
            except Exception as e:
                logger.warning("Error checking path %s: %s", path, e)
                
    # Also include the old logic for EMBEDS if needed, or assume this replaces it?
    # For constraints of this task, we stick to the new chain logic for REFERS_TO.
    # But we MUST preserve the embedded logic (Items -> Products).
    # The pure path query above misses EMBEDS. 
    # Let's run the old query strictly for embedded/1-hop to be safe? 
    # Or rely on the user intent being clear. 
    # For safety, let's keep the old single-hop discovery for Embedded specifically.
    
    # ... (Re-run legacy discovery for 'embedded' specifically if needed, 
    # but the prompt asked for Transitive, mostly affecting REFERS_TO chains).
    # To save time/code, I'll merge the existing 1-hop embedded logic here.

    CY_DISCOVER_EMBEDDED = """
    MATCH (src:Collection {name:$root})-[:EMBEDS]->(e:Embedded)
    MATCH (e)-[r:REFERS_TO]->(dst:Collection)
    WHERE e.path + '.' + r.alias IN $candidates
    RETURN 'embedded' AS kind, r.alias AS alias, dst.name AS dst_collection,
           r.localField AS local_field, r.foreignField AS foreign_field, e.path AS array_path
    """
    
    try:
        with driver.session() as session:
            res = session.run(CY_DISCOVER_EMBEDDED, root=root_collection, candidates=list(potential_paths))
            for record in res:
                # Embedded joins (items.product)
                # target_path is alias (product), but inside array (items). 
                # effectively target is "product" but we handle it via unwind.
                recipe = JoinRecipe(
                    kind="embedded",
                    src_collection=root_collection,
                    alias=record["alias"],
                    dst_collection=record["dst_collection"],
                    local_field=record["local_field"],
                    foreign_field=record["foreign_field"],
                    array_path=record["array_path"],
                    target_path=record["alias"], # kept simple for embedded
                    lookup_local_field=None # handled by special logic
                )
                if recipe.alias not in [r.alias for r in recipes_map.values()]: # simple dedupe
                     recipes_map[f"embedded_{recipe.alias}"] = recipe
                     logger.debug("Found embedded recipe: %s", recipe)

    except Exception as e:
        logger.warning(f"Embedded discovery failed: {e}")

    return list(recipes_map.values())

# ------------------------------------------------------------------
# 3) Mongo Pipeline Compilation
# ------------------------------------------------------------------

def compile_match(filters: List[Dict[str, Any]], join_recipes: List[JoinRecipe] = []) -> Dict[str, Any]:
    match: Dict[str, Any] = {}
    
    # Create a mapping of {full_intent_path_prefix -> alias} for rewriting
    # e.g. "items.product" -> "product"
    path_rewrites = {}
    for jr in join_recipes:
        if jr.kind == "embedded" and jr.array_path:
            # construct the logical path "items.product"
            logical_path = f"{jr.array_path}.{jr.alias}"
            path_rewrites[logical_path] = jr.alias

    for f in filters:
        original_path = f["pathHint"]
        final_path = original_path
        
        # Check if we need to rewrite
        for logical, alias in path_rewrites.items():
            if original_path.startswith(logical):
                # replace "items.product.category" -> "product.category"
                final_path = original_path.replace(logical, alias, 1)
                break
        
        op = f.get("op", "eq")
        val = f["value"]
        
        if op == "eq":
            match[final_path] = val
        elif op == "neq":
            match[final_path] = {"$ne": val}
        elif op == "gt":
            match[final_path] = {"$gt": val}
        elif op == "gte":
            match[final_path] = {"$gte": val}
        elif op == "lt":
            match[final_path] = {"$lt": val}
        elif op == "lte":
            match[final_path] = {"$lte": val}
        elif op == "in":
            match[final_path] = {"$in": val}
        else:
            # Fallback to equality
            match[final_path] = val
    return match

def compile_pipeline(intent: Dict[str, Any], join_recipes: List[JoinRecipe]) -> List[Dict[str, Any]]:
    logger.debug("--- compile_pipeline ---")
    pipeline: List[Dict[str, Any]] = []
    
    # helper sets for quick lookups
    # which aliases are created by lookups?
    lookup_aliases = {r.alias for r in join_recipes}
    
    # Distinct set of logical joins needed
    # Sort them to ensure deterministic order (collections first, then embedded)
    sorted_recipes = sorted(join_recipes, key=lambda x: (x.kind != 'collection', x.array_path or "", x.alias))
    logger.debug("Sorted Join Recipes:\n%s", "\n".join(str(r) for r in sorted_recipes))

    # 1. Pre-Lookup Filters
    # Filters that apply to fields NOT produced by a lookup.
    # This includes root fields and embedded array fields (before lookup).
    raw_filters = intent.get("filters", [])
    pre_lookup_filters = []
    post_lookup_filters = []

    for f in raw_filters:
        path = f["pathHint"]
        # Improved heuristic: check if ANY segment matches a lookup alias
        # OR if it matches a known "array_path.alias" pattern
        
        is_post = False
        
        # Check direct aliases
        parts = path.split(".")
        if any(p in lookup_aliases for p in parts):
            is_post = True
            
        # Check embedded logical paths (e.g. items.product)
        if not is_post:
            for jr in join_recipes:
                if jr.kind == "embedded" and jr.array_path:
                    logical_prefix = f"{jr.array_path}.{jr.alias}"
                    if path.startswith(logical_prefix):
                        is_post = True
                        break

        if is_post:
            post_lookup_filters.append(f)
        else:
            pre_lookup_filters.append(f)

    if pre_lookup_filters:
        logger.debug("Pre-lookup filters identified: %s", pre_lookup_filters)
    if pre_lookup_filters:
        logger.debug("Pre-lookup filters identified: %s", pre_lookup_filters)
        pipeline.append({"$match": compile_match(pre_lookup_filters, join_recipes)}) # No rewrites needed usually for pre
        logger.debug("Added pre-lookup filters")

    # 2. Apply Joins
    # We must track which arrays have already been unwound to prevent "Double Unwind"
    unwound_arrays: Set[str] = set()

    # Sort logic: Collections by path depth (length of target_path), then Embedded
    # e.g. "order" (depth 1) before "order.customer" (depth 2)
    def recipe_sort_key(r: JoinRecipe):
        if r.kind == "collection":
            return (0, r.target_path.count(".") if r.target_path else 0)
        return (1, 0)
        
    sorted_recipes = sorted(join_recipes, key=recipe_sort_key)

    for jr in sorted_recipes:
        if jr.kind == "collection":
            # Standard Lookup (Transitive aware)
            # Use target_path as 'as', and lookup_local_field as 'localField'
            
            # If target_path is missing (legacy/fallback), use alias
            as_field = jr.target_path if jr.target_path else jr.alias
            local_field = jr.lookup_local_field if jr.lookup_local_field else jr.local_field
            
            pipeline.append({
                "$lookup": {
                    "from": jr.dst_collection,
                    "localField": local_field,
                    "foreignField": jr.foreign_field,
                    "as": as_field,
                }
            })
            pipeline.append({
                "$unwind": {
                    "path": f"${as_field}",
                    "preserveNullAndEmptyArrays": True,
                }
            })
        
        elif jr.kind == "embedded":
            # Embedded Lookup
            # Check if we already unwound this array
            if jr.array_path not in unwound_arrays:
                pipeline.append({
                    "$unwind": {
                        "path": f"${jr.array_path}",
                        "preserveNullAndEmptyArrays": True,
                    }
                })
                unwound_arrays.add(jr.array_path)
                logger.debug("Added embedded unwind for alias '%s' via path '%s'", jr.alias, jr.array_path)
            
            # Now perform the lookup on the unwound document
            pipeline.append({
                "$lookup": {
                    "from": jr.dst_collection,
                    "localField": f"{jr.array_path}.{jr.local_field}",
                    "foreignField": jr.foreign_field,
                    "as": jr.alias,
                }
            })
            pipeline.append({
                "$unwind": {
                    "path": f"${jr.alias}",
                    "preserveNullAndEmptyArrays": True,
                }
            })

    # 3. Post-Lookup Filters
    if post_lookup_filters:
        logger.debug("Post-lookup filters identified: %s", post_lookup_filters)
        # Pass recipes so we can rewrite items.product -> product
        pipeline.append({"$match": compile_match(post_lookup_filters, join_recipes)})
        logger.debug("Added post-lookup filters")

    # 4. Final Projection (Optional but good practice, implicit in how aggregation works)
    # We won't add an explicit $project unless 'select' requires complex reshaping,
    # but for now we leave it as is to return full documents + joins.

    logger.info("MongoDB pipeline compiled successfully with %d stages", len(pipeline))
    logger.debug("Full Pipeline:\n%s", json.dumps(pipeline, indent=2))
    return pipeline

# ------------------------------------------------------------------
# 4) Execution
# ------------------------------------------------------------------

def run_pipeline(
    mongo_uri: str,
    mongo_db: str,
    collection: str,
    pipeline: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    logger.info("Executing pipeline on MongoDB: %s", collection)
    try:
        with MongoClient(mongo_uri) as client:
            docs = list(client[mongo_db][collection].aggregate(pipeline))
            logger.debug("Pipeline execution returned %d documents", len(docs))
            return docs
    except Exception as e:
        raise QueryCompilationError(f"MongoDB execution failed: {e}") from e

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
        potential_paths = extract_potential_paths(intent)

        driver = GraphDatabase.driver(
            args.neo4j_uri,
            auth=(args.neo4j_user, args.neo4j_password),
        )

        try:
            join_recipes = fetch_join_recipes(driver, intent["root"], potential_paths)
            pipeline = compile_pipeline(intent, join_recipes)

            if args.print_pipeline:
                print(json.dumps(pipeline, indent=2))

            if args.execute:
                results = run_pipeline(
                    args.mongo_uri,
                    args.mongo_db,
                    intent["root"],
                    pipeline,
                )
                print(json.dumps(results, indent=2, default=str))

        finally:
            driver.close()

    except QueryCompilationError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error occurred")
        sys.exit(1)

if __name__ == "__main__":
    main()
