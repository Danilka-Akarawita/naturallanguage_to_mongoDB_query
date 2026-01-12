# 🏗️ Architecture Documentation: Natural Language to MongoDB Query System

> **Purpose**: This document provides deep function-level architecture diagrams to help understand the system's design and data flow for future reference.

---

## 📊 System Overview

```mermaid
flowchart TB
    subgraph Input["📥 Input Layer"]
        NL["Natural Language Query"]
        Schema["schema.txt"]
    end
    
    subgraph Processing["⚙️ Processing Pipeline"]
        IG["intent_generator.py"]
        NM["neo4j_metadata_loader.py"]
        MQC["mongo_query_compiler.py"]
    end
    
    subgraph Storage["🗄️ Data Stores"]
        Neo4j["Neo4j Graph DB"]
        MongoDB["MongoDB"]
    end
    
    subgraph Output["📤 Output"]
        Intent["Intent JSON"]
        Pipeline["Aggregation Pipeline"]
        Results["Query Results"]
    end
    
    NL --> IG
    Schema --> IG
    IG --> Intent
    Intent --> MQC
    NM --> Neo4j
    Neo4j --> MQC
    MQC --> Pipeline
    Pipeline --> MongoDB
    MongoDB --> Results
```

---

## 1️⃣ Intent Generator Module (`intent_generator.py`)

This module converts natural language questions into structured Intent JSON using OpenAI's API.

### High-Level Flow

```mermaid
flowchart LR
    subgraph Input
        Q["User Question"]
        S["Schema Context"]
    end
    
    subgraph Functions
        GI["generate_intent_json()"]
        BRF["build_response_format_json_schema()"]
    end
    
    subgraph External
        OpenAI["OpenAI API"]
    end
    
    subgraph Validation
        PM["Pydantic Models"]
    end
    
    subgraph Output
        JSON["Intent JSON"]
    end
    
    Q --> GI
    S --> GI
    GI --> BRF
    BRF --> OpenAI
    OpenAI --> PM
    PM --> JSON
```

### Function Details

```mermaid
classDiagram
    class Intent {
        +str root
        +List~str~ select
        +List~Filter~ filters
        +List~Sort~ sort
        +int limit
    }
    
    class Filter {
        +str pathHint
        +Op op
        +value: str|int|float|bool|List
    }
    
    class Sort {
        +str pathHint
        +SortDir dir
    }
    
    Intent --> Filter : contains
    Intent --> Sort : contains
```

### `generate_intent_json()` Function Flow

```mermaid
flowchart TD
    Start["generate_intent_json(question, schema_text, model)"]
    
    subgraph Step1["Step 1: Initialize"]
        Init["Create OpenAI client"]
    end
    
    subgraph Step2["Step 2: Build Messages"]
        Sys["System Prompt with rules"]
        User["Schema Context + User Question"]
        Msgs["Construct messages array"]
    end
    
    subgraph Step3["Step 3: API Call"]
        Schema["build_response_format_json_schema()"]
        Call["client.chat.completions.create()"]
    end
    
    subgraph Step4["Step 4: Validation"]
        Parse["json.loads(raw)"]
        Validate["Intent.model_validate(data)"]
    end
    
    subgraph Step5["Step 5: Return"]
        Dump["intent.model_dump()"]
    end
    
    Start --> Step1
    Step1 --> Step2
    Sys --> Msgs
    User --> Msgs
    Step2 --> Step3
    Schema --> Call
    Step3 --> Step4
    Parse --> Validate
    Step4 --> Step5
    Step5 --> End["Return Dict"]
```

---

## 2️⃣ Neo4j Metadata Loader Module (`neo4j_metadata_loader.py`)

This module loads schema metadata into Neo4j to enable dynamic join discovery.

### Graph Schema

```mermaid
erDiagram
    Collection ||--o{ Embedded : EMBEDS
    Collection ||--o{ Collection : REFERS_TO
    Embedded ||--o{ Collection : REFERS_TO
    
    Collection {
        string name
    }
    
    Embedded {
        string owner
        string path
    }
```

### Data Structures

```mermaid
flowchart TB
    subgraph Collections["Collections (8 total)"]
        U["users"]
        C["customers"]
        O["outlets"]
        P["products"]
        OR["orders"]
        PA["payments"]
        D["deliveries"]
        IM["inventory_moves"]
    end
    
    subgraph Embedded["Embedded Objects"]
        OI["orders.items"]
        IMR["inventory_moves.ref"]
    end
    
    subgraph Relationships["Key Relationships"]
        R1["orders → customers (customer)"]
        R2["orders → outlets (outlet)"]
        R3["orders → users (createdBy)"]
        R4["orders.items → products (product)"]
        R5["deliveries → orders (order)"]
        R6["deliveries → users (driver)"]
    end
```

### `load_metadata()` Function Flow

```mermaid
flowchart TD
    Start["load_metadata(tx)"]
    
    subgraph Step1["Step 1: Create Collection Nodes"]
        Loop1["For each collection name"]
        Merge1["MERGE (:Collection {name})"]
    end
    
    subgraph Step2["Step 2: Create Embedded + REFERS_TO"]
        Loop2["For each embedded reference"]
        Merge2["MERGE Collection → Embedded → Collection"]
        Set2["SET localField, foreignField"]
    end
    
    subgraph Step3["Step 3: Create Collection REFERS_TO"]
        Loop3["For each collection reference"]
        Merge3["MERGE (src)-[:REFERS_TO]->(dst)"]
        Set3["SET alias, localField, foreignField"]
    end
    
    Start --> Step1
    Loop1 --> Merge1
    Step1 --> Step2
    Loop2 --> Merge2 --> Set2
    Step2 --> Step3
    Loop3 --> Merge3 --> Set3
    Step3 --> End["Complete"]
```

### Neo4j Graph Visualization

```mermaid
graph LR
    subgraph Collections
        orders((orders))
        customers((customers))
        outlets((outlets))
        users((users))
        products((products))
        payments((payments))
        deliveries((deliveries))
        inventory_moves((inventory_moves))
    end
    
    subgraph Embedded
        items[["orders.items"]]
        ref[["inventory_moves.ref"]]
    end
    
    orders -->|customer| customers
    orders -->|outlet| outlets
    orders -->|createdBy| users
    orders -.->|EMBEDS| items
    items -->|product| products
    
    payments -->|order| orders
    payments -->|payer| customers
    
    deliveries -->|order| orders
    deliveries -->|driver| users
    
    inventory_moves -->|product| products
    inventory_moves -->|outlet| outlets
    inventory_moves -.->|EMBEDS| ref
    ref -->|order| orders
    ref -->|delivery| deliveries
```

---

## 3️⃣ MongoDB Query Compiler Module (`mongo_query_compiler.py`)

This is the core module that compiles Intent JSON into MongoDB aggregation pipelines.

### High-Level Architecture

```mermaid
flowchart TB
    subgraph Input["📥 Inputs"]
        Intent["Intent JSON File"]
        Neo4j["Neo4j Schema Graph"]
    end
    
    subgraph Core["⚙️ Core Functions"]
        LI["load_intent()"]
        EPP["extract_potential_paths()"]
        FJR["fetch_join_recipes()"]
        CP["compile_pipeline()"]
        CM["compile_match()"]
        RP["run_pipeline()"]
    end
    
    subgraph Data["📦 Data Structures"]
        JR["JoinRecipe"]
    end
    
    subgraph Output["📤 Outputs"]
        Pipeline["Aggregation Pipeline"]
        Results["Query Results"]
    end
    
    Intent --> LI
    LI --> EPP
    EPP --> FJR
    Neo4j --> FJR
    FJR --> JR
    JR --> CP
    CM --> CP
    CP --> Pipeline
    Pipeline --> RP
    RP --> Results
```

### Data Structure: `JoinRecipe`

```mermaid
classDiagram
    class JoinRecipe {
        +str kind
        +str src_collection
        +str alias
        +str dst_collection
        +str local_field
        +str foreign_field
        +Optional~str~ array_path
        +Optional~str~ target_path
        +Optional~str~ lookup_local_field
    }
    
    note for JoinRecipe "kind: 'collection' | 'embedded'\narray_path: Used for embedded joins\ntarget_path: Full dot-path for transitive joins"
```

### Function: `load_intent()`

```mermaid
flowchart LR
    Input["path: str"] --> Open["Open JSON file"]
    Open --> Parse["json.load()"]
    Parse --> Log["Log success"]
    Log --> Return["Return intent dict"]
    
    Open -.-> Error["QueryCompilationError"]
    Parse -.-> Error
```

### Function: `extract_potential_paths()`

Extracts all dot-notation paths referenced in the intent to discover required joins.

```mermaid
flowchart TD
    Start["extract_potential_paths(intent)"]
    
    GetRoot["Get root collection"]
    InitSet["Initialize used_paths = set()"]
    
    subgraph ScanSections["Scan Each Section"]
        Select["Scan 'select'"]
        Filters["Scan 'filters' (pathHint)"]
        Sort["Scan 'sort' (field)"]
    end
    
    subgraph ExtractLogic["Extraction Logic"]
        Split["Split path by '.'"]
        Add1["Add parts[0] to used_paths"]
        Add2["Add parts[0].parts[1] if len >= 2"]
    end
    
    Return["Return used_paths"]
    
    Start --> GetRoot --> InitSet
    InitSet --> ScanSections
    Select --> ExtractLogic
    Filters --> ExtractLogic
    Sort --> ExtractLogic
    ExtractLogic --> Return
```

**Example:**
```
Input: select = ["orderNo", "customer.name", "items.product.name"]
Output: {"orderNo", "customer", "customer.name", "items", "items.product"}
```

### Function: `fetch_join_recipes()`

Discovers join recipes from Neo4j based on potential paths.

```mermaid
flowchart TD
    Start["fetch_join_recipes(driver, root, paths)"]
    
    InitMap["Initialize recipes_map = {}"]
    SortPaths["Sort paths by '.' count"]
    
    subgraph ChainLoop["For Each Path"]
        Split["segments = path.split('.')"]
        Query["Run CY_VERIFY_CHAIN query"]
        Check{"Record found?"}
        
        subgraph BuildRecipes["Build Recipes"]
            Loop["For each relationship in chain"]
            Create["Create JoinRecipe"]
            Store["Store in recipes_map"]
        end
    end
    
    subgraph EmbeddedDiscovery["Embedded Discovery"]
        EmbedQuery["Run CY_DISCOVER_EMBEDDED"]
        EmbedLoop["For each embedded result"]
        EmbedRecipe["Create embedded JoinRecipe"]
    end
    
    Return["Return list(recipes_map.values())"]
    
    Start --> InitMap --> SortPaths
    SortPaths --> ChainLoop
    Split --> Query --> Check
    Check -->|Yes| BuildRecipes
    Check -->|No| Continue["Continue to next path"]
    Loop --> Create --> Store
    ChainLoop --> EmbeddedDiscovery
    EmbedQuery --> EmbedLoop --> EmbedRecipe
    EmbeddedDiscovery --> Return
```

### Neo4j Cypher Queries

```mermaid
flowchart LR
    subgraph CY_VERIFY_CHAIN["CY_VERIFY_CHAIN"]
        VC1["MATCH (start:Collection {name: $root})"]
        VC2["MATCH p = (start)-[:REFERS_TO*]->(end)"]
        VC3["WHERE [r IN rels | r.alias] = $segments"]
        VC4["RETURN depth, collections, rels"]
    end
    
    subgraph CY_DISCOVER_EMBEDDED["CY_DISCOVER_EMBEDDED"]
        DE1["MATCH (src:Collection)-[:EMBEDS]->(e:Embedded)"]
        DE2["MATCH (e)-[r:REFERS_TO]->(dst:Collection)"]
        DE3["WHERE e.path + '.' + r.alias IN $candidates"]
        DE4["RETURN kind, alias, dst_collection, fields"]
    end
```

### Function: `compile_match()`

Converts filters to MongoDB `$match` stage with path rewriting.

```mermaid
flowchart TD
    Start["compile_match(filters, join_recipes)"]
    
    InitMatch["match = {}"]
    
    subgraph BuildRewrites["Build Path Rewrites"]
        Loop1["For each embedded join recipe"]
        Create["logical_path = array_path.alias"]
        Store["path_rewrites[logical_path] = alias"]
    end
    
    subgraph ProcessFilters["Process Each Filter"]
        GetPath["original_path = f['pathHint']"]
        CheckRewrite{"Needs rewrite?"}
        Rewrite["Replace logical path with alias"]
        GetOp["Get operator and value"]
        
        subgraph OpMapping["Operator Mapping"]
            EQ["eq → direct value"]
            NEQ["neq → $ne"]
            GT["gt → $gt"]
            GTE["gte → $gte"]
            LT["lt → $lt"]
            LTE["lte → $lte"]
            IN["in → $in"]
        end
        
        AddMatch["Add to match dict"]
    end
    
    Return["Return match"]
    
    Start --> InitMatch --> BuildRewrites
    Loop1 --> Create --> Store
    BuildRewrites --> ProcessFilters
    GetPath --> CheckRewrite
    CheckRewrite -->|Yes| Rewrite
    CheckRewrite -->|No| GetOp
    Rewrite --> GetOp
    GetOp --> OpMapping --> AddMatch
    ProcessFilters --> Return
```

**Path Rewriting Example:**
```
Input: "items.product.category"
Rewrite Map: {"items.product": "product"}
Output: "product.category"
```

### Function: `compile_pipeline()` - Main Compilation

```mermaid
flowchart TD
    Start["compile_pipeline(intent, join_recipes)"]
    
    subgraph Init["Initialization"]
        InitPipeline["pipeline = []"]
        GetAliases["lookup_aliases = {r.alias for r in recipes}"]
        SortRecipes["Sort recipes by kind & depth"]
    end
    
    subgraph FilterSeparation["1. Separate Filters"]
        GetFilters["raw_filters = intent['filters']"]
        
        subgraph FilterLoop["For Each Filter"]
            CheckPath["Check path segments"]
            IsPost{"Contains lookup alias?"}
            PreList["Add to pre_lookup_filters"]
            PostList["Add to post_lookup_filters"]
        end
    end
    
    subgraph PreMatch["2. Pre-Lookup Match"]
        AddPreMatch["pipeline.append({$match: ...})"]
    end
    
    subgraph Joins["3. Apply Joins"]
        TrackUnwind["unwound_arrays = set()"]
        
        subgraph JoinLoop["For Each Recipe"]
            CheckKind{"kind?"}
            
            subgraph CollectionJoin["Collection Join"]
                CLookup["$lookup from dst_collection"]
                CUnwind["$unwind $alias"]
            end
            
            subgraph EmbeddedJoin["Embedded Join"]
                CheckUnwound{"Already unwound?"}
                ArrayUnwind["$unwind $array_path"]
                MarkUnwound["unwound_arrays.add(array_path)"]
                ELookup["$lookup from dst_collection"]
                EUnwind["$unwind $alias"]
            end
        end
    end
    
    subgraph PostMatch["4. Post-Lookup Match"]
        AddPostMatch["pipeline.append({$match: ...})"]
    end
    
    Return["Return pipeline"]
    
    Start --> Init
    Init --> FilterSeparation
    GetFilters --> FilterLoop
    CheckPath --> IsPost
    IsPost -->|No| PreList
    IsPost -->|Yes| PostList
    FilterSeparation --> PreMatch
    PreMatch --> Joins
    TrackUnwind --> JoinLoop
    CheckKind -->|collection| CollectionJoin
    CheckKind -->|embedded| EmbeddedJoin
    CheckUnwound -->|No| ArrayUnwind --> MarkUnwound --> ELookup
    CheckUnwound -->|Yes| ELookup
    CLookup --> CUnwind
    ELookup --> EUnwind
    Joins --> PostMatch
    PostMatch --> Return
```

### Pipeline Stage Generation Detail

```mermaid
sequenceDiagram
    participant Intent
    participant Compiler
    participant Pipeline
    
    Note over Intent,Pipeline: Example: Order with Customer and Product
    
    Intent->>Compiler: filters: [{pathHint: "status", op: "eq", value: "PENDING"}]
    Compiler->>Pipeline: $match: {status: "PENDING"} (pre-lookup)
    
    Intent->>Compiler: join_recipes: [customer, product]
    
    Compiler->>Pipeline: $lookup: {from: customers, localField: customerId, as: customer}
    Compiler->>Pipeline: $unwind: {path: $customer}
    
    Compiler->>Pipeline: $unwind: {path: $items} (array)
    Compiler->>Pipeline: $lookup: {from: products, localField: items.productId, as: product}
    Compiler->>Pipeline: $unwind: {path: $product}
    
    Intent->>Compiler: filters: [{pathHint: "items.product.category", op: "eq", value: "Cake"}]
    Compiler->>Pipeline: $match: {"product.category": "Cake"} (post-lookup, rewritten)
```

### Function: `run_pipeline()`

```mermaid
flowchart LR
    Input["mongo_uri, mongo_db, collection, pipeline"]
    
    subgraph Execution
        Connect["MongoClient(mongo_uri)"]
        DB["client[mongo_db]"]
        Coll["db[collection]"]
        Agg["collection.aggregate(pipeline)"]
        List["list(results)"]
    end
    
    subgraph Output
        Docs["List[Dict] documents"]
    end
    
    subgraph Error
        QCE["QueryCompilationError"]
    end
    
    Input --> Connect --> DB --> Coll --> Agg --> List --> Docs
    Agg -.-> QCE
```

### Function: `main()` - CLI Entry Point

```mermaid
flowchart TD
    Start["main()"]
    
    subgraph Args["Parse Arguments"]
        MongoURI["--mongo_uri"]
        MongoDB["--mongo_db"]
        Neo4jURI["--neo4j_uri"]
        Neo4jUser["--neo4j_user"]
        Neo4jPass["--neo4j_password"]
        IntentFile["--intent_file"]
        PrintPipe["--print_pipeline"]
        Execute["--execute"]
    end
    
    subgraph Pipeline["Build Pipeline"]
        LoadIntent["intent = load_intent(path)"]
        ExtractPaths["paths = extract_potential_paths(intent)"]
        ConnectNeo4j["driver = GraphDatabase.driver(...)"]
        FetchRecipes["recipes = fetch_join_recipes(...)"]
        CompilePipe["pipeline = compile_pipeline(...)"]
    end
    
    subgraph Output["Output"]
        CheckPrint{"print_pipeline?"}
        PrintJSON["print(json.dumps(pipeline))"]
        CheckExec{"execute?"}
        RunPipe["results = run_pipeline(...)"]
        PrintResults["print(json.dumps(results))"]
    end
    
    subgraph Cleanup["Cleanup"]
        CloseNeo4j["driver.close()"]
    end
    
    subgraph Errors["Error Handling"]
        QCE["QueryCompilationError → exit(1)"]
        Other["Exception → exit(1)"]
    end
    
    Start --> Args --> Pipeline
    LoadIntent --> ExtractPaths --> ConnectNeo4j --> FetchRecipes --> CompilePipe
    Pipeline --> Output
    CheckPrint -->|Yes| PrintJSON
    CheckPrint -->|No| CheckExec
    PrintJSON --> CheckExec
    CheckExec -->|Yes| RunPipe --> PrintResults
    CheckExec -->|No| Cleanup
    PrintResults --> Cleanup
    Cleanup --> End["Complete"]
    
    Pipeline -.-> Errors
    Output -.-> Errors
```

---

## 4️⃣ Complete End-to-End Flow

```mermaid
sequenceDiagram
    actor User
    participant CLI as CLI (main.py)
    participant IG as intent_generator.py
    participant OpenAI as OpenAI API
    participant NL as neo4j_metadata_loader.py
    participant Neo4j as Neo4j DB
    participant MQC as mongo_query_compiler.py
    participant MongoDB as MongoDB
    
    Note over User,MongoDB: One-time Setup
    User->>NL: Load schema metadata
    NL->>Neo4j: Create Collection/Embedded nodes
    NL->>Neo4j: Create REFERS_TO/EMBEDS relationships
    Neo4j-->>NL: Success
    
    Note over User,MongoDB: Query Execution
    User->>IG: "Show pending orders from Colombo outlet"
    IG->>OpenAI: Generate structured intent
    OpenAI-->>IG: Intent JSON
    IG-->>User: intent.json saved
    
    User->>MQC: Run compiler with intent.json
    MQC->>MQC: load_intent()
    MQC->>MQC: extract_potential_paths()
    MQC->>Neo4j: fetch_join_recipes()
    Neo4j-->>MQC: JoinRecipe[]
    MQC->>MQC: compile_pipeline()
    MQC->>MQC: compile_match() for filters
    MQC-->>User: Aggregation Pipeline
    
    opt --execute
        MQC->>MongoDB: run_pipeline()
        MongoDB-->>MQC: Query Results
        MQC-->>User: JSON Results
    end
```

---

## 5️⃣ Error Handling Architecture

```mermaid
flowchart TD
    subgraph Errors["Exception Types"]
        QCE["QueryCompilationError"]
        VE["ValueError"]
        JDE["JSONDecodeError"]
        PVE["Pydantic ValidationError"]
    end
    
    subgraph Handlers["Error Handlers"]
        LoadIntent["load_intent() → QCE"]
        GenIntent["generate_intent_json() → ValueError"]
        RunPipe["run_pipeline() → QCE"]
        Main["main() → logs & exit(1)"]
    end
    
    subgraph Sources["Error Sources"]
        FileIO["File I/O"]
        OpenAI["OpenAI API"]
        Neo4j["Neo4j Connection"]
        MongoDB["MongoDB Execution"]
        Validation["Schema Validation"]
    end
    
    FileIO --> LoadIntent --> QCE
    OpenAI --> GenIntent --> VE
    Validation --> GenIntent
    Neo4j --> QCE
    MongoDB --> RunPipe --> QCE
    
    QCE --> Main
    VE --> Main
```

---

## 6️⃣ Data Flow Summary Table

| Stage | Input | Function | Output |
|-------|-------|----------|--------|
| 1. NL → Intent | Natural language + Schema | `generate_intent_json()` | Intent JSON |
| 2. Schema Load | Metadata constants | `load_metadata()` | Neo4j Graph |
| 3. Intent Parse | intent.json path | `load_intent()` | Dict |
| 4. Path Extract | Intent dict | `extract_potential_paths()` | Set[str] |
| 5. Join Discovery | Neo4j driver + paths | `fetch_join_recipes()` | List[JoinRecipe] |
| 6. Filter Compile | filters + recipes | `compile_match()` | $match stage |
| 7. Pipeline Build | intent + recipes | `compile_pipeline()` | Pipeline stages |
| 8. Execute | MongoDB + pipeline | `run_pipeline()` | List[Dict] |

---

## 📁 File Structure

```
MongoQGeneartion/
├── intent_generator.py      # NL → Intent JSON (OpenAI)
├── neo4j_metadata_loader.py # Schema → Neo4j Graph
├── mongo_query_compiler.py  # Intent → MongoDB Pipeline
├── schema.txt               # Human-readable schema
├── intent.json              # Generated intent (output)
├── mongo_schemas_seed.py    # MongoDB test data seeder
├── test_mongo_compiler.py   # Compiler unit tests
├── test_nlp_intents.py      # Intent generation tests
├── tested_intents.json      # Test case intents
├── requirements.txt         # Python dependencies
└── .env                     # Environment variables
```

---

> **Last Updated**: January 2026  
> **Author**: Architecture Documentation System
