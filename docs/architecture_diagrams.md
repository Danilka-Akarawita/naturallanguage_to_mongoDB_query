# 🏗️ Architecture Documentation: Natural Language to MongoDB Query System

> **Purpose**: This document provides deep function-level architecture diagrams to help understand the system's design and data flow for future reference.

---

## 📊 System Overview

```mermaid
flowchart TB
    subgraph Input["📥 Input Layer"]
        NL["Natural Language Query"]
        Schema["src/models/schemas.py"]
    end
    
    subgraph Services["⚙️ Services Layer (src/services)"]
        IS["intent_service.py"]
        MQC["query_compiler.py"]
    end

    subgraph Scripts["🛠️ Scripts"]
        NM["scripts/load_neo4j.py"]
        SM["scripts/seed_mongo.py"]
    end
    
    subgraph Storage["🗄️ Data Stores"]
        Neo4j["Neo4j Graph DB"]
        MongoDB["MongoDB"]
    end
    
    subgraph Output["📤 Output"]
        Intent["Intent Object"]
        Pipeline["Aggregation Pipeline"]
        Results["Query Results"]
    end
    
    NL --> IS
    IS --> Intent
    Intent --> MQC
    NM --> Neo4j
    Neo4j --> MQC
    MQC --> Pipeline
    Pipeline --> MongoDB
    MongoDB --> Results
    SM --> MongoDB
```

---

## 1️⃣ Intent Service (`src/services/intent_service.py`)

This service converts natural language questions into structured `Intent` objects using OpenAI's API.

### High-Level Flow

```mermaid
flowchart LR
    subgraph Input
        Q["User Question"]
        S["Schema Context"]
    end
    
    subgraph Service["Intent Service"]
        GI["generate_intent_json()"]
        BRF["build_response_format_json_schema()"]
    end
    
    subgraph External
        OpenAI["OpenAI API"]
    end
    
    subgraph DomainModels["src/models"]
        IntentModel["Intent"]
    end
    
    subgraph Output
        Dict["Intent Dictionary"]
    end
    
    Q --> GI
    S --> GI
    GI --> BRF
    BRF --> OpenAI
    OpenAI --> IntentModel
    IntentModel --> Dict
```

### Domain Models (`src/models/intent.py`)

```mermaid
classDiagram
    class Intent {
        +str root
        +List~str~ select
        +List~Filter~ filters
        +List~Sort~ sort
        +int limit
        +Optional~AggregationType~ aggregation
    }
    
    class Filter {
        +str pathHint
        +Op op
        +value: Union[str, int, float, bool, List]
    }
    
    class Sort {
        +str pathHint
        +SortDir dir
    }
    
    Intent --> Filter : contains
    Intent --> Sort : contains
```

---

## 2️⃣ Neo4j Metadata Loader (`scripts/load_neo4j.py`)

This script loads schema metadata into Neo4j to enable dynamic join discovery for the Query Compiler.

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

### Data Flow

```mermaid
flowchart TD
    Start["load_metadata(tx)"]
    
    subgraph Step1["Step 1: Create Collections"]
        Loop1["Iterate defined collections"]
        Merge1["MERGE (:Collection {name})"]
    end
    
    subgraph Step2["Step 2: Embedded Relations"]
        Loop2["Iterate embedded refs"]
        Merge2["MERGE Collection → Embedded → Collection"]
    end
    
    subgraph Step3["Step 3: Direct References"]
        Loop3["Iterate direct refs"]
        Merge3["MERGE (src)-[:REFERS_TO]->(dst)"]
    end
    
    Start --> Step1
    Step1 --> Step2
    Step2 --> Step3
    Step3 --> End["Graph Updated"]
```

---

## 3️⃣ Query Compiler Service (`src/services/query_compiler.py`)

This service compiles `Intent` objects into MongoDB aggregation pipelines by resolving relationships via Neo4j.

### Architecture

```mermaid
flowchart TB
    subgraph Input["📥 Inputs"]
        IntentObj["Intent Object"]
        Neo4j["Neo4j Knowledge Graph"]
    end
    
    subgraph Compiler["⚙️ QueryCompiler Class"]
        CP["compile_pipeline()"]
        CM["compile_match()"]
        FJR["fetch_join_recipes()"]
        EPP["extract_potential_paths()"]
    end
    
    subgraph Output["📤 Outputs"]
        Pipeline["MongoDB Aggregation Pipeline"]
    end
    
    IntentObj --> EPP
    EPP --> FJR
    Neo4j --> FJR
    FJR --> CP
    CM --> CP
    CP --> Pipeline
```

### Compilation Logic

1.  **Path Extraction**: `extract_potential_paths()` identifies all fields referenced in the intent (select, filters, sort).
2.  **Join Discovery**: `fetch_join_recipes()` queries Neo4j to find the shortest path of relationships to reach those fields from the root collection.
3.  **Pipeline Construction**: `compile_pipeline()`:
    *   Separates filters into "Pre-Lookup" (local fields) and "Post-Lookup" (joined fields).
    *   Adds initial `$match` stage.
    *   Iterates through `JoinRecipes` to append `$lookup` and `$unwind` stages.
    *   Adds final `$match` stage for joined fields.
    *   Appends `$sort`, `$project`, and `$limit`.

### Join Recipe Structure

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
    }
    
    note for JoinRecipe "kind: 'collection' | 'embedded'\nused to dynamically build $lookup stages"
```
