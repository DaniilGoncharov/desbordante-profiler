import json
from pathlib import Path

from desbordante_profiler_package.core.history import HistoryStorage
from desbordante_profiler_package.core.enums import DictionaryField, TaskStatus

def test_history_storage_initialization_creates_file(temp_dir: Path):
    hs_file = temp_dir / "new_history.json"
    assert not hs_file.exists()
    HistoryStorage(filename=str(hs_file))
    assert hs_file.exists()
    with open(hs_file, 'r') as f:
        data = json.load(f)
        assert data == {DictionaryField.runs: []}

def test_add_run(empty_history_storage: HistoryStorage, successful_run_info: dict):
    empty_history_storage.add_run(successful_run_info)
    db = empty_history_storage._load_db()
    assert len(db[DictionaryField.runs]) == 1
    assert db[DictionaryField.runs][0][DictionaryField.task_id] == successful_run_info[DictionaryField.task_id]

def test_update_run(empty_history_storage: HistoryStorage, successful_run_info: dict):
    empty_history_storage.add_run(successful_run_info)
    task_id_to_update = successful_run_info[DictionaryField.task_id]
    updates = {DictionaryField.result: TaskStatus.Failure, "new_field": "test_value"}
    empty_history_storage.update_run(task_id_to_update, updates)

    db = empty_history_storage._load_db()
    updated_run = next(run for run in db[DictionaryField.runs] if run[DictionaryField.task_id] == task_id_to_update)
    assert updated_run[DictionaryField.result] == TaskStatus.Failure
    assert updated_run["new_field"] == "test_value"

def test_mark_success(empty_history_storage: HistoryStorage, sample_task_to_run):
    start_time = 1000.0
    initial_run_info = {
        DictionaryField.run_id: "run1",
        DictionaryField.task_id: sample_task_to_run.task_id,
        DictionaryField.algorithm: sample_task_to_run.algorithm_name,
        DictionaryField.params: sample_task_to_run.params,
        DictionaryField.timestamp_start: start_time,
        DictionaryField.result: TaskStatus.NotStarted,
    }
    empty_history_storage.add_run(initial_run_info)

    exec_time = 10.5
    success_info = {
        DictionaryField.task_id: sample_task_to_run.task_id,
        DictionaryField.timestamp_start: start_time,
        DictionaryField.execution_time: exec_time,
        DictionaryField.result: TaskStatus.Success,
        DictionaryField.result_path: "path/to/success.pkl",
        DictionaryField.instances: 10
    }
    empty_history_storage.mark_success(success_info)

    db = empty_history_storage._load_db()
    marked_run = db[DictionaryField.runs][0]
    assert marked_run[DictionaryField.result] == TaskStatus.Success
    assert marked_run[DictionaryField.timestamp_end] == start_time + exec_time
    assert marked_run[DictionaryField.execution_time] == exec_time
    assert marked_run[DictionaryField.result_path] == "path/to/success.pkl"
    assert marked_run[DictionaryField.instances] == 10

def test_mark_failure(empty_history_storage: HistoryStorage, sample_task_to_run):
    initial_run_info = {
        DictionaryField.task_id: sample_task_to_run.task_id
    }
    empty_history_storage.add_run(initial_run_info)

    failure_info = {
        DictionaryField.task_id: sample_task_to_run.task_id,
        DictionaryField.error_type: TaskStatus.MemoryError,
        DictionaryField.rules_decision: "skip"
    }
    empty_history_storage.mark_failure(failure_info)
    db = empty_history_storage._load_db()
    marked_run = db[DictionaryField.runs][0]
    assert marked_run[DictionaryField.result] == TaskStatus.Failure
    assert marked_run[DictionaryField.error_type] == TaskStatus.MemoryError
    assert marked_run[DictionaryField.rules_decision] == "skip"


def test_get_tasks_by_run_id(empty_history_storage: HistoryStorage, successful_run_info: dict, failed_run_info: dict):
    run_id1 = "test_run_1"
    run_id2 = "test_run_2"

    task1_run1 = {**successful_run_info, DictionaryField.run_id: run_id1, DictionaryField.task_id: "task1"}
    task2_run1 = {**failed_run_info, DictionaryField.run_id: run_id1, DictionaryField.task_id: "task2"}
    task1_run2 = {**successful_run_info, DictionaryField.run_id: run_id2, DictionaryField.task_id: "task3"}

    empty_history_storage.add_run(task1_run1)
    empty_history_storage.add_run(task1_run2)
    empty_history_storage.add_run(task2_run1)


    tasks_for_run1 = empty_history_storage.get_tasks_by_run_id(run_id1)
    assert len(tasks_for_run1) == 2
    assert tasks_for_run1[0][DictionaryField.task_id] == "task1"
    assert tasks_for_run1[1][DictionaryField.task_id] == "task2"

    tasks_for_run2 = empty_history_storage.get_tasks_by_run_id(run_id2)
    assert len(tasks_for_run2) == 1
    assert tasks_for_run2[0][DictionaryField.task_id] == "task3"

    tasks_for_unknown_run = empty_history_storage.get_tasks_by_run_id("unknown_run")
    assert len(tasks_for_unknown_run) == 0


def test_get_last_run_for_algo_and_data(empty_history_storage: HistoryStorage, successful_run_info: dict):
    base_info = {
        DictionaryField.algorithm: "hyfd",
        DictionaryField.params: {"p1": 1},
        DictionaryField.data_hash: "hash123",
        DictionaryField.rows: 100,
        DictionaryField.cols: 5,
        DictionaryField.result: TaskStatus.Success,
        DictionaryField.result_path: "path1.pkl",
        DictionaryField.task_id: "task_A"
    }
    empty_history_storage.add_run(base_info)
    empty_history_storage.add_run({**base_info, DictionaryField.algorithm: "tane", DictionaryField.task_id: "task_B"})
    empty_history_storage.add_run({**base_info, DictionaryField.params: {"p1": 2}, DictionaryField.task_id: "task_C"})
    empty_history_storage.add_run({**base_info, DictionaryField.result: TaskStatus.Failure, DictionaryField.task_id: "task_D"})
    newer_success_info = {**base_info, DictionaryField.result_path: "path2.pkl", DictionaryField.task_id: "task_E"}
    empty_history_storage.add_run(newer_success_info)


    found_run = empty_history_storage.get_last_run_for_algo_and_data(
        algo_name="hyfd", params={"p1": 1}, data_hash="hash123", rows=100, cols=5
    )
    assert found_run is not None
    assert found_run[DictionaryField.result_path] == "path2.pkl"
    assert found_run[DictionaryField.task_id] == "task_E"

    not_found_run = empty_history_storage.get_last_run_for_algo_and_data(
        algo_name="hyfd", params={"p1": 99}, data_hash="hash123", rows=100, cols=5
    )
    assert not_found_run is None
    assert empty_history_storage.get_last_run_for_algo_and_data("hyfd", {}, None, 10, 10) is None
