import time
import uuid
import logging
import pickle
from pathlib import Path
import desbordante
from typing import List, Any, Tuple, Optional, Dict
from pandas import DataFrame

from desbordante_profiler_package.core.rules import handle_failure
from desbordante_profiler_package.core.scheduler import run_tasks, TaskToRun
from desbordante_profiler_package.core.enums import DictionaryField, TaskStatus, Strategy, RulesField, RulesAction, RulesRetryParameter
from desbordante_profiler_package.core.mining_algorithms import MINING_FAMILIES
from desbordante_profiler_package.core.history import HistoryStorage

logger = logging.getLogger(__name__)

class CoreManager:
    """Manages execution of profiling tasks."""

    def __init__(self,
                 history_storage: HistoryStorage,
                 run_dir: Path,
                 run_id: str,
                 strategy: Strategy,
                 check_results: bool,
                 try_parallel: bool,
                 mem_limit_bytes: int,
                 workers: int,
                 global_timeout: Optional[int],
                 timeout_step: int = 300,
                 timeout_max: int = 1800,
                 prune_factor: float = 0.7,
                 min_rows: int = 1000,
                 ):
        self.history_storage = history_storage
        self.run_id = run_id
        self.run_dir = run_dir
        self.strategy = strategy
        self.timeout_step = timeout_step
        self.timeout_max = timeout_max
        self.prune_factor = prune_factor
        self.min_rows = min_rows
        self.check_results = check_results
        self.try_parallel = try_parallel
        self.mem_limit_bytes = mem_limit_bytes
        self.workers = workers
        self.global_timeout = global_timeout

    def execute_tasks_to_run(self, initial_tasks: List[TaskToRun]) -> None:
        """Executes tasks with error handling and retry logic based on rules."""
        iteration = 1
        tasks = initial_tasks

        if self.check_results:
            self._check_existing_results(tasks)

        while tasks:
            logger.info(f"=== Iteration {iteration}: Running {len(tasks)} tasks ===")
            self._record_task_start(tasks)
            results, execution_time = run_tasks(tasks, self.try_parallel, self.workers, self.mem_limit_bytes,
                                                self.global_timeout)
            self._update_tasks_params(tasks)  # Add info about threads
            tasks = self._process_results(tasks, results, execution_time)
            iteration += 1
            self.try_parallel = False  # Iterations starting from the second one are always not parallel

        logger.info("No more tasks to run, finishing.")

    @staticmethod
    def _create_new_task(
            old_task: TaskToRun,
            new_df: Optional[DataFrame] = None,
            new_timeout: Optional[int] = None,
            stage_increment: int = 1
    ) -> TaskToRun:
        """Creates a new task with modified parameters for retrying failed tasks."""
        new_task_id = str(uuid.uuid4())
        df = old_task.data if new_df is None else new_df
        return TaskToRun(task_id=new_task_id,
                         algorithm_family=old_task.algorithm_family,
                         algorithm_name=old_task.algorithm_name,
                         params=old_task.params,
                         data=df,
                         data_hash=old_task.data_hash,
                         rows=df.shape[0],
                         cols=df.shape[1],
                         timeout=old_task.timeout if new_timeout is None else new_timeout,
                         strategy=old_task.strategy,
                         stage=old_task.stage + stage_increment)

    def _check_existing_results(self, tasks: List[TaskToRun]) -> None:
        """Checks for existing results and removes tasks that have already been completed."""
        for task in tasks[:]:
            last_succeed_task = self.history_storage.get_last_run_for_algo_and_data(task.algorithm_name, task.params,
                                                                               task.data_hash, task.rows, task.cols)

            if last_succeed_task:
                try:
                    with open(last_succeed_task.get(DictionaryField.result_path), "rb") as f:
                        logger.info(f"Found stored result for {task.algorithm_name} with params: {task.params}.")
                        result_type = task.algorithm_family
                        result_dict = pickle.load(f)
                        self._store_result(result_type, result_dict, task)
                        last_succeed_task[DictionaryField.run_id] = self.run_id
                        self.history_storage.add_run(last_succeed_task)
                        tasks.remove(task)
                except Exception as e:
                    logger.warning(f"Failed to load existing result: {e}")

    def _handle_task_failure(self, task: TaskToRun, error_type: str, new_tasks: List[TaskToRun]) -> None:
        """Handles task failures using rule-based decisions."""
        logger.info(f"Task {task.algorithm_name} failed with error: {error_type}")
        decision = handle_failure({
            RulesField.task: task,
            DictionaryField.error_type: error_type}, self.timeout_step, self.timeout_max, self.prune_factor, self.min_rows)
        action = decision.get(RulesField.action, RulesAction.skip)

        self.history_storage.mark_failure({
            DictionaryField.task_id: task.task_id,
            DictionaryField.error_type: error_type,
            DictionaryField.rules_decision: action})

        if action == RulesAction.retry:
            new_task = self._create_new_task(task,
                                       new_df=decision[RulesField.retry_params].get(
                                           RulesRetryParameter.new_dataframe, task.data),
                                       new_timeout=decision[RulesField.retry_params].get(
                                           RulesRetryParameter.new_timeout, task.timeout),
                                       stage_increment=1)
            new_tasks.append(new_task)

    def _process_results(
            self,
            tasks: List[TaskToRun],
            results: List[Tuple[str, Optional[Dict[str, List[Any]]]]],
            execution_time: List[Any]
    ) -> List[TaskToRun]:
        """Processes task results and applies rule-based recovery when needed."""
        new_tasks = []
        for task, (result_type, result_dict), task_execution_time in zip(tasks, results, execution_time):
            if result_type in MINING_FAMILIES:
                ser_file = self._store_result(result_type, result_dict, task)
                self.history_storage.mark_success({
                    DictionaryField.task_id: task.task_id,
                    DictionaryField.data_hash: task.data_hash,
                    DictionaryField.timestamp_start: task.timestamp_start,
                    DictionaryField.execution_time: task_execution_time,
                    DictionaryField.result: TaskStatus.Success,
                    DictionaryField.result_path: str(ser_file),
                    DictionaryField.instances: sum(len(instances) for instances in result_dict.values())
                })
            else:
                self._handle_task_failure(task, result_type, new_tasks)
        return new_tasks

    def _record_task_start(self, tasks: List[TaskToRun]) -> None:
        """Records the start of each task for tracking execution history."""
        for task in tasks:
            task.timestamp_start = time.monotonic()
            self.history_storage.add_run({
                DictionaryField.run_id: self.run_id,
                DictionaryField.task_id: task.task_id,
                DictionaryField.algorithm: task.algorithm_name,
                DictionaryField.algorithm_family: task.algorithm_family,
                DictionaryField.params: task.params,
                DictionaryField.data_hash: task.data_hash,
                DictionaryField.rows: task.rows,
                DictionaryField.cols: task.cols,
                DictionaryField.timestamp_start: task.timestamp_start,
                DictionaryField.result: TaskStatus.NotStarted
            })

    def _update_tasks_params(self, tasks: List[TaskToRun]) -> None:
        """Updates params field for each task for tracking execution history."""
        for task in tasks:
            self.history_storage.update_run(task.task_id, {
                DictionaryField.params: task.params
            })

    def _store_result(
            self,
            result_type: str,
            result_dict: Dict[str, List[Any]],
            task: TaskToRun
    ) -> Optional[Path]:
        """Stores results to disk and logs their completion."""
        result_file = self.run_dir / "result.txt"
        ser_data_dir = self.run_dir / "serialized_data"
        ser_data_dir.mkdir(exist_ok=True)
        ser_file = ser_data_dir / f"{task.algorithm_name}_{task.task_id}.pkl"

        with open(result_file, "a", encoding="utf-8") as f:
            f.write(f"{result_type.upper()} by {task.algorithm_name} with params: {task.params}\n")

            for instance_type, payload in result_dict.items():
                f.write(f"{instance_type}:\n")
                for instance in payload:
                    if type(instance) is desbordante.od.ListOD:
                        f.write(f"\t{instance.lhs} : {instance.rhs}\n")
                    elif type(instance) is desbordante.ac.ACRanges:
                        f.write(f"\tcolumn indices: {instance.column_indices}; ranges: {instance.ranges}\n")
                    elif type(instance) is desbordante.ac.ACException:
                        f.write(f"\tcolumn pairs: {instance.column_pairs}\n")
                    else:
                        f.write(f"\t{instance}\n")
            f.write("\n")
        try:
            with open(ser_file, "wb") as f:
                pickle.dump(result_dict, f)
        except Exception as e:
            logger.warning(f"Failed to serialize result: {e}")
            return None

        return ser_file





