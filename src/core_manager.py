import time
import uuid
import logging
import pickle
import desbordante
from typing import List, Any, Tuple, Optional
from pathlib import Path

from src.scheduler_module import Task, TaskScheduler
from src.rules_engine import BaseRulesEngine
from src.history_storage import HistoryStorage

logger = logging.getLogger(__name__)

class CoreManager:
    """Manages execution of profiling tasks."""

    def __init__(self,
                 scheduler: TaskScheduler,
                 rules_engine: BaseRulesEngine,
                 history_storage: HistoryStorage):
        self.scheduler = scheduler
        self.rules_engine = rules_engine
        self.history_storage = history_storage

    def run_profile(self, initial_tasks: List[Task], run_dir: Path, run_id: str, check_results: bool = False):
        """Executes tasks with error handling and retry logic based on rules."""
        iteration = 1
        tasks = initial_tasks

        if check_results:
            self._check_existing_results(tasks, run_dir)

        while tasks:
            logger.info(f"=== Iteration {iteration}: Running {len(tasks)} tasks ===")
            self._record_task_start(run_id, tasks)
            results, execution_time = self.scheduler.run_tasks(tasks)
            tasks = self._process_results(tasks, results, execution_time, run_dir)
            iteration += 1

        logger.info("No more tasks to run, finishing.")

    def _check_existing_results(self, tasks: List[Task], run_dir: Path):
        """Checks for existing results and removes tasks that have already been completed."""
        for task in tasks[:]:
            last_result = self.history_storage.get_last_run_for_algo_and_data(task.algorithm_name, task.params,
                                                                              task.data_name)

            if last_result:
                try:
                    with open(last_result, "rb") as f:
                        logger.info(f"Found stored result for {task.algorithm_name} with params: {task.params}.")
                        result_type, result = pickle.load(f)
                        store_result(result_type, result, task, run_dir)
                        tasks.remove(task)
                except Exception as e:
                    logger.warning(f"Failed to load existing result: {e}")

    def _record_task_start(self, run_id, tasks: List[Task]):
        """Records the start of each task for tracking execution history."""
        for task in tasks:
            task.timestamp_start = time.monotonic()
            self.history_storage.add_run({
                "run_id": run_id,
                "task_id": task.task_id,
                "algorithm": task.algorithm_name,
                "algorithm_family": task.algorithm_family,
                "params": task.params,
                "timestamp_start": task.timestamp_start,
                "result": "not finished"
            })

    def _process_results(self, tasks: List[Task], results: List[Tuple[str, Any]], execution_time: List[Any],
                         run_dir: Path) -> List[Task]:
        """Processes task results and applies rule-based recovery when needed."""
        new_tasks = []
        for task, (result_type, result), task_execution_time in zip(tasks, results, execution_time):
            if result_type in ("fd", "afd", "ind", "ucc", "cfd", "od", "ar", "dd"):
                ser_file = store_result(result_type, result, task, run_dir)
                self.history_storage.mark_success({
                    "task_id": task.task_id,
                    "data": task.data_name,
                    "timestamp_start": task.timestamp_start,
                    "execution_time": task_execution_time,
                    "result": str(ser_file)
                })
            else:
                self._handle_task_failure(task, result_type, new_tasks)
        return new_tasks

    def _handle_task_failure(self, task: Task, error_type: str, new_tasks: List[Task]):
        """Handles task failures using rule-based decisions."""
        logger.info(f"Task {task.task_id} failed with error: {error_type}")

        decision = self.rules_engine.handle_failure(
            {"task_id": task.task_id, "algorithm": task.algorithm_name, "error_type": error_type})
        action = decision.get("action", "skip")

        self.history_storage.mark_failure({"task_id": task.task_id, "error_type": error_type, "rules_decision": action})

        if action == "retry":
            new_tasks.append(create_new_task(task))
        elif action == "cut_df":
            divisor = decision["params"].get("divisor", 2)
            new_df = task.data.iloc[:len(task.data) // divisor]
            new_tasks.append(create_new_task(task, new_df))



def create_new_task(old_task: Task, new_df: Optional[Any] = None) -> Task:
    """Creates a new task with modified parameters for retrying failed tasks."""
    new_task_id = str(uuid.uuid4())
    return Task(task_id=new_task_id,
                algorithm_family=old_task.algorithm_family,
                algorithm_name=old_task.algorithm_name,
                params=old_task.params,
                data=old_task.data if new_df is None else new_df,
                data_name=old_task.data_name,
                timeout=old_task.timeout)

def store_result( result_type: str, result: Any, task: Task, run_dir: Path) -> Path:
    """Stores results to disk and logs their completion."""
    result_file = run_dir / "result.txt"
    ser_data_dir = run_dir / "serialized_data"
    ser_data_dir.mkdir(exist_ok=True)
    ser_file = ser_data_dir / f"{task.algorithm_name}_{task.task_id}.pkl"

    try:
        with open(ser_file, "wb") as f:
            pickle.dump((result_type, result), f)
    except Exception as e:
        logger.error(f"Failed to serialize result: {e}")

    with open(result_file, "a", encoding="utf-8") as f:
        f.write(f"{result_type.upper()} by {task.algorithm_name} with params: {task.params}\n")
        for item in result:
            if type(item) is desbordante.od.ListOD:
                f.write(f"{item.lhs} : {item.rhs}\n")
            f.write(f"{item}\n")
        f.write("\n")

    return ser_file