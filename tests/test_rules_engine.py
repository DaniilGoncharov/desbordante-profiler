import pytest
from src.rules_engine import PythonRulesEngine
from unittest.mock import MagicMock

@pytest.fixture
def mock_history_storage():
    return MagicMock()

@pytest.fixture
def engine(mock_history_storage):
    return PythonRulesEngine(history_storage=mock_history_storage)

def test_handle_failure_memory_retry(engine, mock_history_storage):
    mock_history_storage.get_recent_errors.return_value = []
    decision = engine.handle_failure({
        "algorithm": "test_algo",
        "error_type": "memory"
    })
    assert decision["action"] == "retry"

def test_handle_failure_memory_skip(engine, mock_history_storage):
    mock_history_storage.get_recent_errors.return_value = [{"error_type":"memory"}, {"error_type":"memory"}]
    decision = engine.handle_failure({
        "algorithm": "test_algo",
        "error_type": "memory"
    })
    assert decision["action"] == "skip"

def test_handle_failure_timeout(engine):
    decision = engine.handle_failure({
        "algorithm": "test_algo",
        "error_type": "timeout"
    })
    assert decision["action"] == "cut_df"
    assert decision["params"]["divisor"] == 2

def test_handle_failure_unknown(engine):
    decision = engine.handle_failure({
        "algorithm": "test_algo",
        "error_type": "some_weird_error"
    })
    assert decision["action"] == "skip"
