import pytest
from unittest.mock import patch, MagicMock
from src.core_manager import CoreManager
from src.scheduler_module import TaskScheduler, Task
from src.rules_engine import BaseRulesEngine
from src.history_storage import HistoryStorage


@pytest.fixture
def mock_scheduler():
    scheduler = MagicMock(spec=TaskScheduler)
    scheduler.run_tasks.return_value = ([], [])
    return scheduler


@pytest.fixture
def mock_rules_engine():
    class MockRules(BaseRulesEngine):
        def handle_failure(self, run_info):
            return {"action": "skip"}

        def handle_success(self, run_info):
            return {}

    return MockRules()


@pytest.fixture
def mock_history():
    history = MagicMock(spec=HistoryStorage)
    history.get_last_run_for_algo_and_data.side_effect = ["/path/to/old_result.pkl", None]
    return history


def test_run_profile_no_tasks(mock_scheduler, mock_rules_engine, mock_history, tmp_path):
    cm = CoreManager(scheduler=mock_scheduler, rules_engine=mock_rules_engine, history_storage=mock_history)
    cm.run_profile([], tmp_path, run_id="test_run")
    mock_scheduler.run_tasks.assert_not_called()


# TODO: Add mock for run_profile
# def test_run_profile_with_tasks(mock_scheduler, mock_rules_engine, mock_history, tmp_path):
#     tasks = [
#         Task(task_id="123", algorithm_family="fd", algorithm_name="hyfd", params={}, data=None, data_name="test"),
#         Task(task_id="456", algorithm_family="ucc", algorithm_name="hpivalid", params={}, data=None,
#              data_name="test")
#     ]
#     cm = CoreManager(scheduler=mock_scheduler, rules_engine=mock_rules_engine, history_storage=mock_history)
#     cm.run_profile(tasks, tmp_path, run_id="test_run")
#     mock_scheduler.run_tasks.assert_called_once_with(tasks)
#     assert mock_history.mark_success.call_count == 2


def test_run_profile_existing_results(mock_scheduler, mock_rules_engine, mock_history, tmp_path):

    tasks = [
        Task(task_id="t1", algorithm_family="fd", algorithm_name="hyfd", params={}, data=None, data_name="test"),
        Task(task_id="t2", algorithm_family="ucc", algorithm_name="hpivalid", params={}, data=None,
             data_name="test2")
    ]
    cm = CoreManager(scheduler=mock_scheduler, rules_engine=mock_rules_engine, history_storage=mock_history)

    fake_result_type = "fd"
    fake_result_data = ["some_fd_result"]

    with patch("builtins.open", create=True) as mock_open, patch("pickle.load") as mock_pickle_load:
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        mock_pickle_load.return_value = (fake_result_type, fake_result_data)

        cm.run_profile(tasks, tmp_path, run_id="r1", check_results=True)

    mock_scheduler.run_tasks.assert_called_once()
    remaining_tasks = mock_scheduler.run_tasks.call_args[0][0]
    assert len(remaining_tasks) == 1
    assert remaining_tasks[0].task_id == "t2"
