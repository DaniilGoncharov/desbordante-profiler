import logging
import psutil
import time
import sys
import multiprocessing as mp
from queue import Empty as QueueEmpty
from typing import Any, List, Optional, Tuple, Dict
from pandas import DataFrame

from desbordante_profiler_package.core.mining_algorithms import create_mining_algorithm
from desbordante_profiler_package.core.enums import TaskStatus, AlgorithmParameter, Strategy

logger = logging.getLogger(__name__)

INFINITY_TIMEOUT = 10 ** 9 # High value instead of infinity

class TaskToRun:
    """Represents a computational task to be scheduled and executed."""

    def __init__(self,
                 task_id: str,
                 algorithm_family: str,
                 algorithm_name: str,
                 params: Dict[str, Any],
                 data: DataFrame,
                 rows: int,
                 cols: int,
                 data_hash: Optional[str],
                 timeout: Optional[int] = None,
                 strategy: str = Strategy,
                 stage: int = 0) -> None:
        self.task_id = task_id
        self.algorithm_family = algorithm_family
        self.algorithm_name = algorithm_name
        self.params = params
        self.data = data
        self.rows = rows
        self.cols = cols
        self.data_hash = data_hash
        self.timeout = timeout or INFINITY_TIMEOUT
        self.strategy = strategy
        self.stage = stage


def set_resource_limits(memory_limit_per_proc: Optional[int]) -> None:
    """Sets memory resource limits for the current worker process, if supported."""
    if memory_limit_per_proc is None:
        return

    current_pid = mp.current_process().pid
    if sys.platform.startswith("linux") or sys.platform == "darwin":
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_AS)
            new_soft_limit = min(memory_limit_per_proc, hard if hard != resource.RLIM_INFINITY else memory_limit_per_proc)
            resource.setrlimit(resource.RLIMIT_AS, (new_soft_limit, hard))
            logger.debug(f"[worker {current_pid}] RLIMIT_AS soft limit set to {new_soft_limit // (1024 ** 2)} MB "
                         f"(Hard limit: {'Infinity' if hard == resource.RLIM_INFINITY else hard // (1024**2)} MB)")
        except ImportError:
             logger.warning(f"[worker {current_pid}] 'resource' module not available on this platform.")
        except ValueError as ve:
             logger.error(f"[worker {current_pid}] Failed to set memory limit (ValueError): {ve}. "
                          f"Limit requested: {memory_limit_per_proc // (1024 ** 2)} MB")
        except Exception as exc:
            logger.error(f"[worker {current_pid}] Failed to set memory limit: {exc}")
    else:
        logger.warning(f"[worker {current_pid}] Memory limiting via resource module is not supported on {sys.platform}.")


def worker_process_target(
    task: TaskToRun,
    result_queue: mp.Queue,
    memory_limit_per_proc: Optional[int]
) -> None:
    """Target function for worker processes to execute a single TaskToRun."""
    set_resource_limits(memory_limit_per_proc)
    task_id = task.task_id
    logger.debug(f"[worker {mp.current_process().pid}] Starting task {task_id} ({task.algorithm_name}) with params: {task.params}")
    logger.info(f"Starting {task.algorithm_name} with params: {task.params}.")
    try:
        start = time.monotonic()
        algo = create_mining_algorithm(task.algorithm_family, task.algorithm_name, task.params)
        result_data = algo.run(task.data)
        end = time.monotonic()
        execution_time = end - start
        logger.info(f"Algorithm {task.algorithm_name} found {sum(len(instances) for instances in result_data.values())} instances.")
        logger.debug(f"[worker {mp.current_process().pid}] Task {task_id} finished in {execution_time:.2f}s, "
                     f"found {sum(len(instances) for instances in result_data.values())} instances")
        result_queue.put((task_id, TaskStatus.Success, (task.algorithm_family, result_data), execution_time))
    except MemoryError as mem_e:
        logger.warning(f"Worker {mp.current_process().pid}: Task {task_id} ({task.algorithm_name}) memory error: {mem_e}")
        result_queue.put((task_id, TaskStatus.MemoryError, (type(mem_e).__name__, None), "N/A"))
    except Exception as e:
        logger.error(f"Worker {mp.current_process().pid}: Task {task_id} ({task.algorithm_name}) failed with exception: {e}")
        result_queue.put((task_id, TaskStatus.Error, (type(e).__name__, None), "N/A"))


def terminate_process(process: mp.Process, task_id: str) -> None:
    """Terminates a given multiprocessing.Process and its children."""
    pid = process.pid
    if pid is None:
        logger.debug(f"Process for task {task_id} has no PID (likely already terminated or failed to start).")
        return
    if not process.is_alive():
        logger.debug(f"Process for task {task_id} (PID {pid}) already terminated.")
        return

    logger.debug(f"Terminating process for task {task_id} (PID {pid})...")
    try:
        try:
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            for child in children:
                logger.debug(f"Terminating child process {child.pid} of {pid}")
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    continue
            psutil.wait_procs(children, timeout=0.5)
            for child in children:
                 if child.is_running():
                     logger.debug(f"Killing child process {child.pid} of {pid}")
                     try: child.kill()
                     except psutil.NoSuchProcess: pass
        except psutil.NoSuchProcess:
            logger.debug(f"Main process {pid} for task {task_id} not found by psutil (already terminated?).")
            process.join(timeout=0.1)
            return
        except Exception as e:
            logger.error(f"Error terminating child processes of {pid}: {e}")
        process.terminate()
        process.join(timeout=1.0)
        if process.is_alive():
            logger.warning(f"Process {pid} did not terminate gracefully. Killing...")
            process.kill()
            process.join(timeout=0.5)
            if process.is_alive():
                 logger.error(f"Failed to kill process {pid}!")
            else:
                 logger.info(f"Process {pid} killed.")
        else:
             logger.info(f"Process {pid} terminated gracefully.")
    except Exception as e:
        logger.error(f"Error during termination of process {pid}: {e}")
    finally:
        if process.is_alive():
            process.join(timeout=0.1)


def run_tasks(
    tasks: List[TaskToRun],
    try_parallel: bool,
    workers: Optional[int],
    memory_limit: int,
    global_timeout: Optional[float]
    ) -> Tuple[List[Tuple[str, Optional[Any]]], List[Any]]:
    """
    Executes tasks using multiprocessing.Process with enhanced control.
    """
    num_tasks = len(tasks)
    if num_tasks == 0:
        return [], []

    overall_start_time = time.monotonic()

    max_workers = 1 if not try_parallel else workers
    threads_to_set = workers if not try_parallel else 1
    logger.debug(f"Setting 'threads' parameter for algorithms to: {threads_to_set}")

    memory_per_proc = memory_limit // max_workers

    result_queue = mp.Queue()
    active_processes: Dict[str, Tuple[mp.Process, int, float]] = {} # task_id -> (process, task_index, start_time)

    final_results: List[Any] = [(TaskStatus.NotStarted, None)] * num_tasks
    final_execution_time: List[Any] = ["N/A"] * num_tasks
    tasks_to_run = list(enumerate(tasks)) # (index, task)
    tasks_processed_count = 0
    next_task_idx_to_launch = 0
    global_timeout_reached = False

    while tasks_processed_count < num_tasks:
        now = time.monotonic()

        if global_timeout is not None and (now - overall_start_time) > global_timeout:
            logger.warning(f"Global timeout of {global_timeout:.2f}s reached. Stopping task submission and terminating active processes.")
            global_timeout_reached = True
            break

        while len(active_processes) < max_workers and next_task_idx_to_launch < num_tasks:
            task_index, task = tasks_to_run[next_task_idx_to_launch]

            task.params[AlgorithmParameter.threads] = threads_to_set
            logger.debug(f"Preparing task {task.task_id} with params: {task.params}")

            start_time = time.monotonic()
            process = mp.Process(
                target=worker_process_target,
                args=(task, result_queue, memory_per_proc),
                daemon=True
            )
            process.start()

            if process.pid is None:
                 logger.error(f"Failed to start process for task {task.task_id}. It might have terminated immediately.")
                 final_results[task_index] = (TaskStatus.StartingFailure, None)
                 final_execution_time[task_index] = "N/A"
                 tasks_processed_count += 1
                 next_task_idx_to_launch += 1
                 continue

            logger.debug(f"Launched process {process.pid} for task {task.task_id}")
            active_processes[task.task_id] = (process, task_index, start_time)
            final_results[task_index] = (TaskStatus.Running, None)
            next_task_idx_to_launch += 1

        wait_timeout = 0.1
        next_deadline_timeout = INFINITY_TIMEOUT

        if active_processes:
            for task_id, (_, task_index, start_time) in active_processes.items():
                task_timeout_duration = tasks[task_index].timeout
                if task_timeout_duration != INFINITY_TIMEOUT:
                    deadline = start_time + task_timeout_duration
                    time_left = deadline - now
                    next_deadline_timeout = min(next_deadline_timeout, time_left)

            if global_timeout is not None:
                global_time_left = (overall_start_time + global_timeout) - now
                wait_timeout = max(0, min(wait_timeout, next_deadline_timeout, global_time_left))
            else:
                 wait_timeout = max(0, min(wait_timeout, next_deadline_timeout))
        elif next_task_idx_to_launch >= num_tasks:
             break


        try:
            res_task_id, status, result_data, exec_time = result_queue.get(timeout=wait_timeout)

            if res_task_id in active_processes:
                process, task_index, _ = active_processes[res_task_id]
                logger.debug(f"Received result for task {res_task_id} (status: {status}) from process {process.pid}")
                final_results[task_index] = result_data
                final_execution_time[task_index] = exec_time

                if process.is_alive():
                     process.join(timeout=0.5)
                if process.is_alive():
                     logger.warning(f"Process {process.pid} still alive after sending result. Forcing termination.")
                     terminate_process(process, res_task_id)

                del active_processes[res_task_id]
                tasks_processed_count += 1
            else:
                 logger.warning(f"Received result for unknown or already processed task {res_task_id}. Ignoring.")

        except QueueEmpty:
            pass
        except Exception as e:
             logger.error(f"Error while getting result from queue: {e}", exc_info=True)

        now = time.monotonic()
        timed_out_tasks = []
        for task_id, (process, task_index, start_time) in list(active_processes.items()):
            task_timeout_duration = tasks[task_index].timeout
            if now >= start_time + task_timeout_duration:
                 logger.warning(f"Task {task_id} (PID {process.pid}) reached individual timeout of {task_timeout_duration}s.")
                 terminate_process(process, task_id)
                 if final_results[task_index] == (TaskStatus.Running, None) or final_results[task_index] == (TaskStatus.NotStarted, None):
                    final_results[task_index] = (TaskStatus.Timeout, None)
                    final_execution_time[task_index] = "N/A"
                 timed_out_tasks.append(task_id)
                 tasks_processed_count += 1

        for task_id in timed_out_tasks:
            if task_id in active_processes:
                del active_processes[task_id]

    if global_timeout_reached:
        logger.warning("Processing tasks stopped due to global timeout. Terminating remaining active processes.")
        remaining_pids = []
        for task_id, (process, task_index, start_time) in active_processes.items():
            pid = process.pid or "N/A"
            remaining_pids.append(pid)
            logger.warning(f"Terminating process {pid} for task {task_id} due to global timeout.")
            terminate_process(process, task_id)
            if final_results[task_index] == (TaskStatus.Running, None):
                run_duration = time.monotonic() - start_time
                logger.info(f"Task {task_id} marked as 'global_timeout' after running for {run_duration:.2f}s.")
                final_results[task_index] = (TaskStatus.GlobalTimeout, None)
                final_execution_time[task_index] = "N/A"
            elif final_results[task_index] == (TaskStatus.NotStarted, None):
                 logger.info(f"Task {task_id} was not started due to global timeout.")
                 final_results[task_index] = (TaskStatus.GlobalTimeout, None)
                 final_execution_time[task_index] = "N/A"

        active_processes.clear()

    if active_processes:
        logger.warning(f"Performing final cleanup for {len(active_processes)} unexpected remaining processes...")
        remaining_pids = []
        for task_id, (process, task_index, _) in active_processes.items():
            remaining_pids.append(process.pid or "N/A")
            terminate_process(process, task_id)
            if final_results[task_index] == (TaskStatus.Running, None):
                 final_results[task_index] = (TaskStatus.Killed, None)
                 final_execution_time[task_index] = "N/A"

        if remaining_pids:
            logger.warning(f"Force terminated {len(remaining_pids)} processes during final cleanup: {remaining_pids}")

    for i in range(num_tasks):
        if final_results[i] == (TaskStatus.NotStarted, None):
             if not global_timeout_reached:
                final_results[i] = (TaskStatus.Cancelled, None)
                final_execution_time[i] = "N/A"

    logger.info("=== Iteration finished ===")
    return final_results, final_execution_time