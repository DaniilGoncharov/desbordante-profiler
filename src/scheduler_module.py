import logging
import psutil
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from typing import List, Tuple, Any, Optional

from src.algos_module import create_algorithm

logger = logging.getLogger(__name__)

MAX_TIMEOUT = 10 ** 9 # High value instead of infinity

class Task:
    """Represents a computational task to be scheduled and executed."""

    def __init__(self, task_id: str, algorithm_family: str, algorithm_name: str,
                 params: dict, data: Any, data_name: str, timeout: Optional[int] = None):
        self.task_id = task_id
        self.algorithm_family = algorithm_family
        self.algorithm_name = algorithm_name
        self.params = params
        self.data = data
        self.data_name = data_name
        self.timeout = timeout

class TaskScheduler:
    """Manages and executes computational tasks in parallel with timeout handling."""

    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or psutil.cpu_count()

    def run_tasks(self, tasks: List["Task"]) -> Tuple[List[Tuple[str, Optional[Any]]], List[Any]]:
        """Executes a list of tasks concurrently and handles timeouts."""
        results = [("", None) for _ in tasks]
        execution_time = [None] * len(tasks)
        start_time = time.monotonic()


        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_idx = {executor.submit(self._run_task_wrapper, task): i for i, task in enumerate(tasks)}
            deadlines = {future: start_time + (tasks[idx].timeout if tasks[idx].timeout is not None
                                               else MAX_TIMEOUT) for future, idx in future_to_idx.items()}
            remaining_futures = set(future_to_idx.keys())

            timeout = False
            while remaining_futures:
                now = time.monotonic()
                next_timeout = min(deadlines[future] - now for future in remaining_futures)

                if next_timeout <= 0:
                    for future in list(remaining_futures):
                        idx = future_to_idx[future]
                        if now >= deadlines[future]:
                            timeout = True
                            logger.warning(f"Task {tasks[idx].task_id} timed out.")
                            results[idx] = ('timeout', None)
                            execution_time[idx] = "N/A"
                            future.cancel() #doesnt really cancel the process
                            remaining_futures.remove(future)

                    continue

                done, _ = wait(remaining_futures, timeout=next_timeout, return_when=FIRST_COMPLETED)

                for future in done:
                    idx = future_to_idx[future]
                    try:
                        res = future.result()
                        results[idx] = (tasks[idx].algorithm_family, res[0])
                        execution_time[idx] = res[1]
                    except MemoryError as mem_e:
                        logger.warning(f"Task {tasks[idx].task_id} memory error: {mem_e}")
                        results[idx] = ('memory', None)
                    except Exception as e:
                        logger.error(f"Task {tasks[idx].task_id} exception: {e}")
                        results[idx] = (type(e).__name__, None)

                    remaining_futures.remove(future)

            if timeout:
                self.terminate_remaining_workers()

        return results, execution_time

    @staticmethod
    def terminate_remaining_workers():
        """Terminates all remaining worker processes to free resources."""
        for child in psutil.Process().children(recursive=True):
            try:
                child.terminate()
                for _ in range(25):
                    if not child.is_running():
                        break
                    time.sleep(0.2)
                if child.is_running():
                    logger.warning(f"Force killing process {child.pid}")
                    child.kill()
            except psutil.NoSuchProcess:
                continue
        logger.info("All remaining worker processes terminated.")

    @staticmethod
    def _run_task_wrapper(task: Task) -> Any:
        start = time.monotonic()
        algo = create_algorithm(task.algorithm_family, task.algorithm_name, task.params)
        end = time.monotonic()
        execution_time = end - start
        return algo.run(task.data), execution_time
