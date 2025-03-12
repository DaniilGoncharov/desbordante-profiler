import pytest
import json
from src.history_storage import HistoryStorage

@pytest.fixture
def empty_history_file(tmp_path):
    path = tmp_path / "history.json"
    path.write_text(json.dumps({"runs": []}), encoding="utf-8")
    return path

def test_add_run(empty_history_file):
    hs = HistoryStorage(filename=str(empty_history_file))
    hs.add_run({"run_id":"r1", "task_id":"t1", "algorithm":"test_algo", "result":"not finished"})
    with open(empty_history_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert len(data["runs"]) == 1
    assert data["runs"][0]["task_id"] == "t1"

def test_mark_success(empty_history_file):
    hs = HistoryStorage(filename=str(empty_history_file))
    hs.add_run({"task_id":"t1","run_id":"r1","result":"not finished","algorithm":"test_algo"})
    hs.mark_success({
        "task_id":"t1",
        "data":"test.csv",
        "timestamp_start":100,
        "execution_time":10,
        "result":"/path/to/result.pkl"
    })
    with open(empty_history_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    run = data["runs"][0]
    assert run["result"] == "/path/to/result.pkl"
    assert run["data"] == "test.csv"
    assert run["timestamp_end"] == 110
    assert run["execution_time"] == 10

def test_get_last_run_for_algo_and_data(empty_history_file):
    hs = HistoryStorage(filename=str(empty_history_file))
    hs.add_run({"task_id":"t1","run_id":"r1","algorithm":"fd","params":{"p":1},
                "data":"data.csv","result":"error"})
    hs.add_run({"task_id":"t2","run_id":"r1","algorithm":"fd","params":{"p":1},
                "data":"data.csv","result":"not finished"})
    hs.add_run({"task_id":"t3","run_id":"r2","algorithm":"fd","params":{"p":1},
                "data":"data.csv","result":"/path/to/success.pkl","execution_time":5})
    res = hs.get_last_run_for_algo_and_data("fd", {"p":1}, "data.csv")
    assert res == "/path/to/success.pkl"

def test_get_recent_errors(empty_history_file):
    hs = HistoryStorage(filename=str(empty_history_file))
    hs.add_run({"task_id":"t1","run_id":"r1","algorithm":"fd","params":{},
                "error_type":"memory","result":"error"})
    hs.add_run({"task_id":"t2","run_id":"r1","algorithm":"fd","params":{},
                "error_type":"timeout","result":"error"})
    hs.add_run({"task_id":"t3","run_id":"r1","algorithm":"fd","params":{},
                "error_type":"memory","result":"error"})
    errors = hs.get_recent_errors("fd","memory",limit=3)
    assert len(errors) == 2
    assert errors[0]["task_id"] == "t3"
    assert errors[1]["task_id"] == "t1"
