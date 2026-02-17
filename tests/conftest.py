import json
import pytest
from pathlib import Path
from src.services.query_compiler import QueryCompiler
from src.config import settings

@pytest.fixture(scope="session")
def query_compiler():
    """Fixture to provide a QueryCompiler instance."""
    compiler = QueryCompiler()
    yield compiler
    compiler.close()

@pytest.fixture(scope="session")
def tested_intents():
    """Fixture to load tested_intents.json."""
    intent_path = Path("tested_intents.json")
    if not intent_path.exists():
        pytest.fail("tested_intents.json not found in root directory.")
    
    with open(intent_path, "r") as f:
        data = json.load(f)
    return data.get("scenarios", {})
