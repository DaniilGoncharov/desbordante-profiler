import pytest
import time
from unittest.mock import patch
from src.scheduler_module import TaskScheduler, Task

def dummy_algorithm(data):
    time.sleep(0.1)
    return ["some_result"]

@patch("src.scheduler_module.create_algorithm")
def test_run_tasks_success(mock_create_algo):
    class FakeAlgo:
        def run(self, data):
            time.sleep(0.05)
            return ["OK"]
    mock_create_algo.return_value = FakeAlgo()

    scheduler = TaskScheduler(max_workers=2)
    tasks = [
        Task(task_id="t1", algorithm_family="fd", algorithm_name="hyfd", params={}, data=None, data_name="test", timeout=None),
        Task(task_id="t2", algorithm_family="ucc", algorithm_name="hpivalid", params={}, data=None, data_name="test2", timeout=None)
    ]
    results, exec_times = scheduler.run_tasks(tasks)

    assert len(results) == 2
    assert results[0][0] == "fd"
    assert results[0][1] == ["OK"]
    assert results[1][0] == "ucc"
    assert results[1][1] == ["OK"]

    assert isinstance(exec_times[0], float)
    assert exec_times[0] > 0.0

@patch("src.scheduler_module.create_algorithm")
def test_run_tasks_timeout(mock_create_algo):
    class SlowAlgo:
        def run(self, data):
            time.sleep(2)
            return ["slow_result"]
    mock_create_algo.return_value = SlowAlgo()

    scheduler = TaskScheduler(max_workers=1)
    tasks = [
        Task(task_id="t1", algorithm_family="fd", algorithm_name="hyfd",
             params={}, data=None, data_name="test", timeout=1)
    ]
    results, exec_times = scheduler.run_tasks(tasks)

    assert len(results) == 1
    assert results[0][0] == 'timeout'
    assert results[0][1] is None
    assert exec_times[0] == "N/A"
