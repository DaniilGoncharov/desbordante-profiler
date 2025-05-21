import json
import logging
import click
from pathlib import Path
from typing import Dict, Any, List, Optional

from desbordante_profiler_package.core.enums import DictionaryField, TaskStatus

logger = logging.getLogger(__name__)

DEFAULT_APP_NAME = "desbordante_profiler"

class HistoryStorage:
    def __init__(self, filename: Optional[str] = None) -> None:
        if filename is None:
            app_dir = Path(click.get_app_dir(DEFAULT_APP_NAME))
            self.filename = app_dir / "history.json"
        else:
            self.filename = Path(filename)

        self.filename.parent.mkdir(parents=True, exist_ok=True)

        if not self.filename.exists():
            self._initialize_file()

    def _initialize_file(self) -> None:
        """Creates an empty history file if it does not exist."""
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump({DictionaryField.runs: []}, f)

    def _load_db(self) -> Dict[str, Any]:
        """Loads the history database from a JSON file."""
        with open(self.filename, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _save_db(self, db: Dict[str, Any]) -> None:
        """Saves the history database to a JSON file."""
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(db, f, indent=2)

    def add_run(self, run_info: Dict[str, Any]) -> None:
        """Adds a new run entry to the history."""
        db = self._load_db()
        db[DictionaryField.runs].append(run_info)
        self._save_db(db)

    def update_run(self, task_id: str, updates: Dict[str, Any]) -> None:
        """Updates an existing run entry with new information."""
        db = self._load_db()
        for run in db[DictionaryField.runs]:
            if run.get(DictionaryField.task_id) == task_id:
                run.update(updates)
                break
        self._save_db(db)

    def mark_success(self, run_info: Dict[str, Any]) -> None:
        """Marks a run as successful and records execution time."""
        task_id = run_info[DictionaryField.task_id]
        timestamp_end = run_info[DictionaryField.timestamp_start] + run_info[DictionaryField.execution_time]

        logger.debug(f"Mark success for task_id={task_id}")
        self.update_run(task_id, {
            DictionaryField.timestamp_end: timestamp_end,
            DictionaryField.execution_time: run_info[DictionaryField.execution_time],
            DictionaryField.result: run_info[DictionaryField.result],
            DictionaryField.result_path: run_info[DictionaryField.result_path],
            DictionaryField.instances: run_info[DictionaryField.instances]
        })

    def mark_failure(self, run_info: Dict[str, Any]) -> None:
        """Marks a run as failed with an error type."""
        task_id, error_type = run_info.get("task_id"), run_info.get("error_type", "unknown")

        logger.debug(f"Mark failure for task_id={task_id}, error_type={error_type}")
        self.update_run(task_id, {
            DictionaryField.result: TaskStatus.Failure,
            DictionaryField.error_type: error_type,
            DictionaryField.rules_decision: run_info.get(DictionaryField.rules_decision)
        })

    def get_tasks_by_run_id(self, run_id: str) -> List[Dict[str, Any]]:
        """Retrieves all tasks for a given run_id."""
        db = self._load_db()
        runs = []
        for run in reversed(db[DictionaryField.runs]):
            if run.get(DictionaryField.run_id) == run_id:
                runs.append(run)
        return list(reversed(runs))

    def get_last_run_for_algo_and_data(
            self,
            algo_name: str,
            params: Dict[str, Any],
            data_hash: Optional[str],
            rows: int,
            cols: int
    ) -> Optional[Dict[str, Any]]:
        """Retrieves the last successful run result for a given algorithm and dataset."""
        if data_hash is None:
            return None

        db = self._load_db()
        for run in reversed(db[DictionaryField.runs]):
            if ((run.get(DictionaryField.data_hash) == data_hash and run.get(DictionaryField.algorithm) == algo_name) and
                    run.get(DictionaryField.params) == params and run.get(DictionaryField.result) == TaskStatus.Success and
                    run.get(DictionaryField.rows) == rows and run.get(DictionaryField.cols) == cols):
                return run
        return None
