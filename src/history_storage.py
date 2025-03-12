import json
import os
from typing import Dict, Any, List, Optional
from src.logging_conf import get_logger

logger = get_logger(__name__)

class HistoryStorage:
    """Handles storage and retrieval of profiling task history."""

    def __init__(self, filename: str = "history.json"):
        self.filename = filename
        if not os.path.exists(self.filename):
            self._initialize_file()

    def _initialize_file(self):
        """Creates an empty history file if it does not exist."""
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump({"runs": []}, f)

    def _load_db(self) -> dict:
        """Loads the history database from a JSON file."""
        with open(self.filename, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _save_db(self, db: dict) -> None:
        """Saves the history database to a JSON file."""
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2)

    def add_run(self, run_info: Dict[str, Any]) -> None:
        """Adds a new run entry to the history."""
        db = self._load_db()
        db["runs"].append(run_info)
        self._save_db(db)

    def update_run(self, task_id: str, updates: Dict[str, Any]) -> None:
        """Updates an existing run entry with new information."""
        db = self._load_db()
        for run in db["runs"]:
            if run.get("task_id") == task_id:
                run.update(updates)
                break
        self._save_db(db)

    def mark_success(self, run_info: Dict[str, Any]) -> None:
        """Marks a run as successful and records execution time."""
        task_id = run_info["task_id"]
        timestamp_end = run_info["timestamp_start"] + run_info["execution_time"]

        logger.info(f"Mark success for task_id={task_id}")
        self.update_run(task_id, {
            "result": run_info["result"],
            "data": run_info["data"],
            "timestamp_end": timestamp_end,
            "execution_time": run_info["execution_time"]
        })

    def mark_failure(self, run_info: Dict[str, Any]) -> None:
        """Marks a run as failed with an error type."""
        task_id, error_type = run_info.get("task_id"), run_info.get("error_type", "unknown")

        logger.info(f"Mark failure for task_id={task_id}, error_type={error_type}")
        self.update_run(task_id, {
            "result": "error",
            "error_type": error_type,
            "rules_decision": run_info.get("rules_decision")
        })

    def get_tasks_by_run_id(self, run_id: str) -> Optional[Any]:
        """Retrieves all tasks for a given run_id."""
        db = self._load_db()
        runs = []
        for run in reversed(db["runs"]):
            if run.get("run_id") == run_id:
                runs.append(run)
        return runs

    def get_last_run_for_algo_and_data(self, algo_name: str, params: dict, data_name: str) -> Optional[Any]:
        """Retrieves the last successful run result for a given algorithm and dataset."""
        db = self._load_db()
        for run in reversed(db["runs"]):
            if ((run.get("data") == data_name and run.get("algorithm") == algo_name) and run.get("params") == params and
                    run.get("execution_time", None) is not None):
                return run.get("result")
        return None

    def get_recent_errors(self, algo_name: str, error_type: str, limit: int = 3) -> List[dict]:
        """Retrieves recent errors for a given algorithm and error type."""
        db = self._load_db()
        errors = []
        for run in reversed(db["runs"]):
            if run.get("algorithm") == algo_name and run.get("error_type") == error_type:
                errors.append(run)
                if len(errors) >= limit:
                    break
        return errors
