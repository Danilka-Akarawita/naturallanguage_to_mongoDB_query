# Mongo Query Compiler Architecture

This document details the internal architecture and flow of the **Mongo Query Compiler**. It explains how a high-level user intent is transformed into an optimized executable MongoDB aggregation pipeline.

**Example Context**: We will use the provided `intent.json` scenario to illustrate each step.

## 1. System Overview

The compiler operates in four main stages:
1.  **Intent Parsing**: Loading the user's request and extracting relevant data paths.
2.  **Schema Resolution**: querying a Neo4j graph database to discover relationships between collections.
3.  **Optimization**: Filtering potential joins to include *only* what is necessary for the current query.
4.  **Pipeline Compilation**: Constructing the MongoDB aggregation pipeline (match, lookup, unwind, project).

---

## 2. Step-by-Step Execution Flow

### Step 1: Intent Loading & Path Extraction

**Input (`intent.json`):**
```json
{
  "root": "deliveries",
  "select": ["orderNo"],
  "filters": [
    { "pathHint": "driver.name", "op": "eq", "value": "Ruwan" },
    { "pathHint": "order.items.product.name", "op": "contains", "value": "Chocolate Gateau" }
  ]
}
```

**Process:**
The `extract_potential_paths` function scans `select`, `filters`, and `sort` fields to identify every field the user cares about. It generates a set of all required path prefixes.

**Extracted Paths (Example):**
For the input above, the system validates that we need:
-   `driver`, `driver.name`
-   `order`, `order.items`, `order.items.product`, `order.items.product.name`
-   `orderNo`

### Step 2: Fetching Join Recipes (Neo4j)

The system needs to know *how* `deliveries` connects to `driver` or `order`. It queries Neo4j for:
1.  **Collection Joins**: Relationships defined by `[:REFERS_TO]` edges between collections.
2.  **Embedded Joins**: Relationships involving `[:EMBEDS]` and `[:REFERS_TO]` for nested structures.

**Neo4j Query Logic:**
-   It searches for paths starting from the root (`deliveries`).
-   It identifies aliases, local fields, and foreign fields.
-   It recursively resolves target paths (e.g., `order.items.product`).

**Optimization (New Feature):**
Previously, the system returned *everything*. Now, it uses the **Extracted Paths** from Step 1.
         -   If Neo4j returns a recipe for `vehicle` but `vehicle` is not in our extracted paths, **it is skipped**.
         -   This ensures we don't perform expensive `$lookup` operations for data the user didn't ask for.

#### Deep Dive: The `resolve_paths` Function

The raw output from Neo4j gives us isolated relationships, e.g., "Order refers to OrderItem" or "OrderItem refers to Product". It does not inherently know that "Order" is already joined as a field of "Delivery".

**The Problem:**
MongoDB lookups depend on the *current* state of the document. If we have:
1.  Joined `drivers` as `driver`.
2.  Joined `orders` as `order`.
3.  Now want to join `order.items`...

We cannot just say "join items". We must know that "items" lives inside the "order" object we just created.

**The Solution (`resolve_paths`):**
This function recursively calculates the **fully qualified dot-notation path** for every join.
-   It connects the graph-style output (Node A -> Node B) to the document-style structure (Root.A.B).
-   **Example**:
    -   Neo4j says: `Order` -> `alias: items` -> `OrderItem`
    -   Parent (`Order`) path is: `order`
    -   `resolve_paths` calculates: `target_path = order.items`
-   **Why?** This "dot path" is required for the `localField` of subsequent lookups (e.g., `localField: "order.items.productId"`).

#### Deep Dive: The `$unwind` Strategy

You will notice every `$lookup` in the compiler is immediately followed by an `$unwind`.

```json
{ "$lookup": { ... "as": "driver" } },
{ "$unwind": { "path": "$driver", "preserveNullAndEmptyArrays": true } }
```

**Why is this necessary?**

1.  **Lookup returns an Array**:
    By default, `$lookup` returns an array of matching documents, even if a one-to-one relationship exists (e.g. `driver: [ { name: "Ruwan" } ]`).
    -   Accessing this requires array syntax: `driver.0.name`.
    -   We want objects: `driver.name`.
    -   **Result**: `$unwind` flattens the array into a single object, simplifying the document structure for filters and the final projection.

2.  **Enabling Chained Joins**:
    MongoDB cannot easily join based on a field *inside* an array.
    -   **Scenario**: We have `order` -> `items` (array) -> `productId`. We want to join `products`.
    -   If we don't unwind `items`, `items` is a list of objects. We cannot say `localField: "items.productId"` to lookup `products` for *each* item efficiently in standard lookups.
    -   **Solution**: We `$unwind` `items`. Now the document is duplicated for every item. Each document has a **single** `item` object.
    -   **Then**: We can perform a standard lookup using `localField: "item.productId"`.

This "Lookup-Unwind" pattern is critical for strictly replicating the relational "JOIN" behavior where we want to work with the joined data immediately as first-class fields, not lists.

### Step 3: Pipeline Compilation

The `compile_pipeline` function constructs the MongoDB aggregation stages.

#### A. Pre-Filtering (Root Level)
First, it checks for filters that apply directly to the root collection (`deliveries`).
-   *Example*: If we had `{"pathHint": "status", "value": "pending"}`, it would be added here as a `$match`.
-   In our example, `driver.name` and `order...` are joined fields, so they are deferred.

#### B. Join Execution ($lookup & $unwind)
The compiler iterates through the filtered Join Recipes.

1.  **Driver Join**:
    -   There is a recipe mapping `deliveries.driverId` → `drivers._id`.
    -   Stage:
        ```json
        { "$lookup": { "from": "drivers", "localField": "driverId", "foreignField": "_id", "as": "driver" } },
        { "$unwind": { "path": "$driver", "preserveNullAndEmptyArrays": true } }
        ```

2.  **Order Join**:
    -   Recipe mapping `deliveries.orderId` → `orders._id`.
    -   Stage:
        ```json
        { "$lookup": { "from": "orders", "localField": "orderId", "foreignField": "_id", "as": "order" } },
        { "$unwind": { "path": "$order", "preserveNullAndEmptyArrays": true } }
        ```

3.  **Product Join (Deep/Embedded)**:
    -   The system identifies that `order.items` is an array of references.
    -   Stage (Unwind Array):
        ```json
        { "$unwind": { "path": "$order.items", "preserveNullAndEmptyArrays": true } }
        ```
    -   Stage (Lookup Product):
        ```json
        { "$lookup": { "from": "products", "localField": "order.items.productId", "foreignField": "_id", "as": "product" } },
        { "$unwind": { "path": "$product", "preserveNullAndEmptyArrays": true } }
        ```

#### C. Post-Filtering (Joined Fields)
Now that the data is joined, the compiler applies the remaining filters.

-   **Filter 1**: `driver.name` == "Ruwan"
-   **Filter 2**: `product.name` contains "Chocolate Gateau" (remapped from `order.items.product.name`)

**Generated Stage:**
```json
{
  "$match": {
    "driver.name": "Ruwan",
    "product.name": { "$regex": "Chocolate Gateau" }
  }
}
```

### Step 4: Execution & Result

Finally, `pymongo` runs this pipeline against the `cakeflow_proto` database. The result is a list of documents where:
-   The delivery has a driver named "Ruwan".
-   The order contains a "Chocolate Gateau".

## Summary

| Component | Responsibility |
| :--- | :--- |
| **`load_intent`** | Parses JSON input. |
| **`extract_potential_paths`** | Determines *what* data is needed (optimization key). |
| **`fetch_join_recipes`** | Asks Neo4j *how* to get that data, filtering out unused paths. |
| **`compile_pipeline`** | Builds the standard `$match`, `$lookup`, `$unwind` stages. |
| **`run_pipeline`** | Executes the query on MongoDB. |
