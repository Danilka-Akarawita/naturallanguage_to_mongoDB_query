from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from neo4j import GraphDatabase, Driver
from pymongo import MongoClient

from src.config import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

@dataclass
class JoinRecipe:
    """Represents a join operation derived from Neo4j metadata."""
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


class QueryCompiler:
    """
    Compiles an Intent (dict) into a MongoDB Aggregation Pipeline 
    using schema relationships from Neo4j.
    """

    def __init__(self, neo4j_uri: str = settings.NEO4J_URI, neo4j_auth: tuple = (settings.NEO4J_USER, settings.NEO4J_PASSWORD)):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)

    def close(self):
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def extract_potential_paths(self, intent: Dict[str, Any]) -> Set[str]:
        """Extracts all field paths referenced in the intent."""
        used: Set[str] = set()
        for section in ("select", "filters", "sort"):
            for item in intent.get(section, []):
                # Handle both object (dict) and string representations if checking legacy or dict inputs
                if isinstance(item, dict):
                     path = item.get("pathHint") or item.get("field")
                else:
                     path = item # Assuming string if not dict
                
                if not path:
                    continue
                parts = path.split(".")
                for i in range(1, len(parts) + 1):
                    used.add(".".join(parts[:i]))
        logger.info(f"Potential paths extracted: {used}")
        return used

    def fetch_join_recipes(self, root_collection: str, required_paths: Set[str]) -> List[JoinRecipe]:
        """Queries Neo4j to find necessary joins for the required paths."""
        recipes: List[JoinRecipe] = []

        with self.driver.session() as session:
            # 1. Collection joins
            collection_result = session.run("""
                MATCH p=(root:Collection {name:$root})-[:REFERS_TO*0..]->(c:Collection)
                UNWIND relationships(p) AS r
                RETURN startNode(r).name AS src, endNode(r).name AS dst,
                       r.alias AS alias, r.localField AS localField, r.foreignField AS foreignField
            """, root=root_collection)
            collection_records = collection_result.data()
            logger.debug(f"Collection join recipes fetched: {collection_records}")

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

            # 2. Embedded joins
            collections_to_check = [root_collection] + [r.dst_collection for r in recipes if r.kind == "collection"]
            
            for col in collections_to_check:
                embedded_result = session.run("""
                    MATCH (c:Collection {name:$col})-[:EMBEDS]->(e:Embedded)-[r:REFERS_TO]->(dst:Collection)
                    RETURN e.path AS array_path, r.alias AS alias,
                           dst.name AS dst_collection,
                           r.localField AS local_field, r.foreignField AS foreign_field
                """, col=col)
                embedded_records = embedded_result.data()

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

            # 3. Root-level embedded arrays without REFERS_TO
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
            path_to_check = r.target_path if r.kind == "collection" else (r.array_path or r.alias)
            
            if path_to_check and path_to_check not in required_paths:
                 continue

            key = (r.target_path or r.alias, r.array_path)
            if key not in seen:
                seen.add(key)
                final_recipes.append(r)

        logger.info(f"Total join recipes fetched (after filtering): {len(final_recipes)}")
        return final_recipes

    def compile_match(self, filters: List[Dict[str, Any]], join_recipes: List[JoinRecipe]) -> Dict[str, Any]:
        """Compiles the $match stage of the pipeline."""
        match: Dict[str, Any] = {}
        rewrite_map = {f"{r.array_path}.{r.alias}": r.alias for r in join_recipes if r.kind == "embedded" and r.array_path}

        for f in filters:
            path = f["pathHint"]
            for logical, alias in rewrite_map.items():
                if path.startswith(logical):
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
        return match

    def compile_pipeline(self, intent: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Main method to compile the pipeline."""
        required_paths = self.extract_potential_paths(intent)
        join_recipes = self.fetch_join_recipes(intent["root"], required_paths)
        
        pipeline: List[Dict[str, Any]] = []
        unwound: Set[str] = set()

        lookup_paths = {r.target_path for r in join_recipes if r.kind == "collection"}
        for r in join_recipes:
            if r.kind == "embedded" and r.dst_collection:
                if r.array_path:
                    lookup_paths.add(f"{r.array_path}.{r.alias}")
                lookup_paths.add(r.alias)

        pre_filters, post_filters = [], []

        for f in intent.get("filters", []):
            path = f["pathHint"]
            if any(path == lp or path.startswith(lp + ".") for lp in lookup_paths):
                post_filters.append(f)
            else:
                pre_filters.append(f)

        # 1. Pre-lookup filters
        if pre_filters:
            pipeline.append({"$match": self.compile_match(pre_filters, join_recipes)})

        # 2. Joins
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
                # embedded joins
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

        # 3. Post-lookup filters
        if post_filters:
            pipeline.append({"$match": self.compile_match(post_filters, join_recipes)})

        # 4. Aggregation
        if intent.get("aggregation") == "count":
            pipeline.append({"$count": "total"})

        logger.info("Pipeline compiled successfully.")
        return pipeline


# ------------------------------------------------------------------
# Execution Helper
# ------------------------------------------------------------------

def execute_pipeline(pipeline: List[Dict[str, Any]], collection: str, db_name: str = settings.MONGO_DB, uri: str = settings.MONGO_URI) -> List[Dict[str, Any]]:
    """Executes a pipeline against MongoDB."""
    try:
        with MongoClient(uri, tlsAllowInvalidCertificates=True) as client:
            return list(client[db_name][collection].aggregate(pipeline))
    except Exception as exc:
        raise QueryCompilationError(f"MongoDB execution failed: {exc}") from exc


if __name__ == "__main__":
    import argparse
    import sys
    
    # Simple CLI
    ap = argparse.ArgumentParser()
    ap.add_argument("--intent", required=True, help="Path to intent JSON file")
    ap.add_argument("--execute", action="store_true", help="Execute against MongoDB")
    args = ap.parse_args()

    try:
        with open(args.intent, "r") as f:
            intent_data = json.load(f)

        with QueryCompiler() as compiler:
            pipeline = compiler.compile_pipeline(intent_data)
            print(json.dumps(pipeline, indent=2))

            if args.execute:
                results = execute_pipeline(pipeline, intent_data["root"])
                print(json.dumps(results, indent=2, default=str))

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
