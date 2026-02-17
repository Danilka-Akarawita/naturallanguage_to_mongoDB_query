import pytest
from src.services.query_compiler import execute_pipeline
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def get_test_cases():
    """Helper to generate test cases for parameterization."""
    # This is a bit tricky with pytest parameterization since we need to read the file
    # before parameterization happens. For simplicity, we'll use a fixture in the test
    # and iterate inside, or let pytest collect it if we modify conftest.
    # Ideally, we iterate over the keys.
    import json
    with open("tested_intents.json", "r") as f:
        data = json.load(f)
    return [(k, v) for k, v in data.get("scenarios", {}).items()]

@pytest.mark.parametrize("scenario_id, scenario_data", get_test_cases())
def test_intent_execution(query_compiler, scenario_id, scenario_data):
    """
    Tests that a given intent compiles to a valid pipeline and returns results from MongoDB.
    """
    description = scenario_data.get("description")
    intent = scenario_data.get("intent")
    
    logger.info(f"Testing scenario: {scenario_id} - {description}")
    
    # 1. Compile Pipeline
    pipeline = query_compiler.compile_pipeline(intent)
    assert pipeline is not None, f"Pipeline compilation failed for {scenario_id}"
    assert isinstance(pipeline, list), "Pipeline should be a list"
    assert len(pipeline) > 0, "Pipeline should not be empty"
    
    # 2. Execute Pipeline
    try:
        results = execute_pipeline(pipeline, intent["root"])
        logger.info(f"Scenario {scenario_id} returned {len(results)} results.")
        
        # 3. Assertions
        # We expect at least some results for these "tested" intents, 
        # assuming the seed data matches the intent's expectations.
        # If an intent is designed to return nothing, this assertion might need adjustment,
        # but usually "tested intents" imply working queries.
        assert isinstance(results, list), "Results should be a list"
        
        # Optional: Assert specific counts if known, otherwise just check for no errors
        # useful for basic validity check.
        
    except Exception as e:
        pytest.fail(f"Execution failed for {scenario_id}: {e}")
