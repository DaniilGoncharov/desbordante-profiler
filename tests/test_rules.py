import pytest
import pandas as pd
from unittest.mock import patch

from desbordante_profiler_package.core.rules import handle_failure
from desbordante_profiler_package.core.scheduler import TaskToRun
from desbordante_profiler_package.core.enums import Strategy, TaskStatus, RulesAction, RulesRetryParameter, AlgorithmFamily, Algorithm

TIMEOUT_STEP = 300
TIMEOUT_MAX = 1800
PRUNE_FACTOR = 0.7
MIN_ROWS = 2

@pytest.fixture
def base_task(sample_dataframe: pd.DataFrame) -> TaskToRun:
    return TaskToRun(
        task_id="rule-task",
        algorithm_family=str(AlgorithmFamily.fd),
        algorithm_name=str(Algorithm.hyfd),
        params={},
        data=sample_dataframe,
        rows=len(sample_dataframe),
        cols=len(sample_dataframe.columns),
        data_hash="hash",
        timeout=600,
        strategy=str(Strategy.auto_decision),
        stage=1
    )

def test_handle_failure_single_run(base_task: TaskToRun):
    base_task.strategy = str(Strategy.single_run)
    run_info = {"task": base_task, "error_type": TaskStatus.Timeout}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.skip

def test_handle_failure_timeout_grow_retry(base_task: TaskToRun):
    base_task.strategy = str(Strategy.timeout_grow)
    base_task.timeout = TIMEOUT_STEP
    run_info = {"task": base_task, "error_type": TaskStatus.Timeout}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.retry
    assert decision["retry_params"][RulesRetryParameter.new_timeout] == TIMEOUT_STEP * 2

def test_handle_failure_timeout_grow_skip_max_reached(base_task: TaskToRun):
    base_task.strategy = str(Strategy.timeout_grow)
    base_task.timeout = TIMEOUT_MAX
    run_info = {"task": base_task, "error_type": TaskStatus.Timeout}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.skip

def test_handle_failure_shrink_search_retry(base_task: TaskToRun, sample_dataframe: pd.DataFrame):
    base_task.strategy = str(Strategy.shrink_search)
    initial_rows = len(sample_dataframe)
    run_info = {"task": base_task, "error_type": TaskStatus.MemoryError}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.retry
    new_df = decision["retry_params"][RulesRetryParameter.new_dataframe]
    assert isinstance(new_df, pd.DataFrame)
    assert len(new_df) < initial_rows
    assert len(new_df) == round(initial_rows * PRUNE_FACTOR)

def test_handle_failure_shrink_search_skip_min_rows(base_task: TaskToRun):
    base_task.strategy = str(Strategy.shrink_search)
    small_df = pd.DataFrame({'A': range(MIN_ROWS // 2)})
    base_task.data = small_df
    base_task.rows = len(small_df)

    run_info = {"task": base_task, "error_type": TaskStatus.MemoryError}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.skip

def test_handle_failure_auto_decision_memory_error_stage_1(base_task: TaskToRun):
    base_task.strategy = str(Strategy.auto_decision)
    base_task.stage = 1
    run_info = {"task": base_task, "error_type": TaskStatus.MemoryError}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.retry
    assert RulesRetryParameter.new_dataframe in decision["retry_params"]

def test_handle_failure_auto_decision_timeout_max_stages(base_task: TaskToRun):
    from desbordante_profiler_package.core.rules import MAX_STAGES
    base_task.strategy = str(Strategy.auto_decision)
    base_task.stage = MAX_STAGES
    run_info = {"task": base_task, "error_type": TaskStatus.Timeout}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.skip

@patch('click.prompt')
def test_handle_failure_ask_strategy_skip(mock_prompt, base_task: TaskToRun):
    mock_prompt.return_value = RulesAction.skip
    base_task.strategy = str(Strategy.ask)
    run_info = {"task": base_task, "error_type": TaskStatus.Timeout}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.skip
    mock_prompt.assert_called_once()

@patch('click.prompt')
def test_handle_failure_ask_strategy_retry(mock_prompt, base_task: TaskToRun):
    mock_prompt.side_effect = [RulesAction.retry]
    base_task.strategy = str(Strategy.ask)
    run_info = {"task": base_task, "error_type": TaskStatus.MemoryError}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.retry
    assert RulesRetryParameter.new_dataframe not in decision.get("retry_params", {})
    assert RulesRetryParameter.new_timeout not in decision.get("retry_params", {})
    assert mock_prompt.call_count == 1

@patch('click.prompt')
def test_handle_failure_ask_strategy_prune(mock_prompt, base_task: TaskToRun):
    user_prune_factor = 0.5
    mock_prompt.side_effect = [RulesAction.prune, user_prune_factor]
    base_task.strategy = str(Strategy.ask)
    run_info = {"task": base_task, "error_type": TaskStatus.Timeout}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)

    assert decision["action"] == RulesAction.retry
    assert RulesRetryParameter.new_dataframe in decision["retry_params"]
    new_df = decision["retry_params"][RulesRetryParameter.new_dataframe]
    assert len(new_df) == round(len(base_task.data) * user_prune_factor)
    assert mock_prompt.call_count == 2

def test_handle_failure_unknown_error_type_skips(base_task: TaskToRun):
    base_task.strategy = str(Strategy.auto_decision)
    run_info = {"task": base_task, "error_type": "SomeOtherError"}
    decision = handle_failure(run_info, TIMEOUT_STEP, TIMEOUT_MAX, PRUNE_FACTOR, MIN_ROWS)
    assert decision["action"] == RulesAction.skip
