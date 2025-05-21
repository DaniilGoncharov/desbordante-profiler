import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple, List, Optional
from pandas import DataFrame


from desbordante_profiler_package.core.enums import Strategy, ProfileParameter
from desbordante_profiler_package.core.profile_loader import load_profile, TaskProfile
from desbordante_profiler_package.core.dataset_loader import get_dataframe_and_hash
from desbordante_profiler_package.core.manager import CoreManager
from desbordante_profiler_package.core.scheduler import TaskToRun
from desbordante_profiler_package.core.comparer import get_runs_comparison_analyze
from desbordante_profiler_package.core.util import generate_markdown_digest_jinja
from desbordante_profiler_package.core.log_config import add_file_handler
from desbordante_profiler_package.core.history import HistoryStorage

logger = logging.getLogger(__name__)

DEFAULT_LOG_FILE = "profiling.log"
PROFILING_DIGEST = "profiling_digest_template.md.j2"
COMPARISON_SUBSET_DIGEST = "subset_comparison_digest_template.md.j2"
COMPARISON_VERSION_DIGEST = "version_comparison_digest_template.md.j2"

def run_profile_on_dataset(
    run_id: str,
    profile_path: Path,
    dataset_path: Path,
    delimiter: str,
    has_header: bool,
    mem_limit_bytes: int,
    workers: int,
    check_results: bool,
    try_parallel: bool,
    strategy: Strategy,
    timeout_step: int,
    timeout_max: int,
    prune_factor: float,
    min_rows: int,
    history_storage: HistoryStorage
) -> None:
    """Runs a full profiling process for a given dataset and profile."""
    profile = load_profile(profile_path)
    run_dir = create_profiling_dir_tree(profile.name, dataset_path)
    add_file_handler(run_dir / DEFAULT_LOG_FILE)
    df, df_hash = get_dataframe_and_hash(dataset_path, delimiter, has_header,
                                                             profile.global_settings.get(ProfileParameter.rows),
                                                             profile.global_settings.get(ProfileParameter.columns))
    tasks_to_run = create_tasks_to_run(df, df_hash, strategy, profile.tasks)
    manager = CoreManager(history_storage=history_storage,
                                       run_dir=run_dir,
                                       run_id=run_id,
                                       strategy=strategy,
                                       timeout_step=timeout_step,
                                       timeout_max=timeout_max,
                                       prune_factor=prune_factor,
                                       min_rows=min_rows,
                                       check_results=check_results,
                                       try_parallel=try_parallel,
                                       mem_limit_bytes=mem_limit_bytes,
                                       workers=workers,
                                       global_timeout=profile.global_settings.get(ProfileParameter.global_timeout, None))

    manager.execute_tasks_to_run(tasks_to_run)
    generate_markdown_digest_jinja(history_storage.get_tasks_by_run_id(run_id), run_dir, dataset_path,
                                   None, PROFILING_DIGEST)

def compare_with_subset(
    profile_path: Path,
    target_path: Path,
    subset_path: Path,
    delimiter: str,
    has_header: bool,
    check_results: bool,
    mem_limit_bytes: int,
    workers: int,
    history_storage: HistoryStorage
) -> None:
    """Compares primitives between a subset and a target dataset."""
    target_run_id = str(uuid.uuid4())
    subset_run_id = str(uuid.uuid4())
    profile = load_profile(profile_path)
    comparison_dir, subset_dir, target_dir = create_comparison_and_profiling_dir_tree(profile.name, subset_path, target_path)
    add_file_handler(comparison_dir / DEFAULT_LOG_FILE)
    subset_df, subset_df_hash = get_dataframe_and_hash(subset_path, delimiter, has_header,
                                                             profile.global_settings.get(ProfileParameter.rows),
                                                             profile.global_settings.get(ProfileParameter.columns))
    target_df, target_df_hash = get_dataframe_and_hash(target_path, delimiter, has_header,
                                                             profile.global_settings.get(ProfileParameter.rows),
                                                             profile.global_settings.get(ProfileParameter.columns))
    subset_tasks_to_run = create_tasks_to_run(subset_df, subset_df_hash, Strategy.single_run, profile.tasks)
    target_tasks_to_run = create_tasks_to_run(target_df, target_df_hash, Strategy.single_run, profile.tasks)

    logger.info("Starting subset profiling.")

    manager = CoreManager(history_storage=history_storage,
                                       run_dir=subset_dir,
                                       run_id=subset_run_id,
                                       strategy=Strategy.single_run,
                                       check_results=check_results,
                                       try_parallel=False,
                                       mem_limit_bytes=mem_limit_bytes,
                                       workers=workers,
                                       global_timeout=profile.global_settings.get(ProfileParameter.global_timeout, None))
    manager.execute_tasks_to_run(subset_tasks_to_run)

    logger.info("Starting target profiling.")

    manager = CoreManager(history_storage=history_storage,
                                       run_dir=target_dir,
                                       run_id=target_run_id,
                                       strategy=Strategy.single_run,
                                       check_results=check_results,
                                       try_parallel=False,
                                       mem_limit_bytes=mem_limit_bytes,
                                       workers=workers,
                                       global_timeout=profile.global_settings.get(ProfileParameter.global_timeout, None))
    manager.execute_tasks_to_run(target_tasks_to_run)

    logger.info("Starting comparison.")

    runs_comparison_dict, runs_comparison_string = get_runs_comparison_analyze(history_storage.get_tasks_by_run_id(subset_run_id),
                                                       history_storage.get_tasks_by_run_id(target_run_id), target_df)

    comparison_path = comparison_dir / "comparison.txt"
    with open(comparison_path, "w", encoding="utf-8") as f:
        f.write(runs_comparison_string)

    logger.info(f"Comparison result saved to {comparison_path}.")

    generate_markdown_digest_jinja(runs_comparison_dict, comparison_dir, subset_path,
                                   target_path, COMPARISON_SUBSET_DIGEST)

def compare_with_new_version(
    profile_path: Path,
    initial_path: Path,
    target_path: Path,
    delimiter: str,
    has_header: bool,
    check_results: bool,
    mem_limit_bytes: int,
    workers: int,
    history_storage: HistoryStorage
) -> None:
    """Compares primitives between an initial and a target version of the same dataset."""
    target_run_id = str(uuid.uuid4())
    initial_run_id = str(uuid.uuid4())
    profile = load_profile(profile_path)
    comparison_dir, initial_dir, target_dir = create_comparison_and_profiling_dir_tree(profile.name, initial_path,
                                                                                      target_path)
    add_file_handler(comparison_dir / DEFAULT_LOG_FILE)
    initial_df, initial_df_hash = get_dataframe_and_hash(initial_path, delimiter, has_header,
                                                             profile.global_settings.get(ProfileParameter.rows),
                                                             profile.global_settings.get(ProfileParameter.columns))
    target_df, target_df_hash = get_dataframe_and_hash(target_path, delimiter, has_header,
                                                             profile.global_settings.get(ProfileParameter.rows),
                                                             profile.global_settings.get(ProfileParameter.columns))
    initial_tasks_to_run = create_tasks_to_run(initial_df, initial_df_hash, Strategy.single_run, profile.tasks)
    target_tasks_to_run = create_tasks_to_run(target_df, target_df_hash, Strategy.single_run, profile.tasks)

    manager = CoreManager(history_storage=history_storage,
                                       run_dir=initial_dir,
                                       run_id=initial_run_id,
                                       strategy=Strategy.single_run,
                                       check_results=check_results,
                                       try_parallel=False,
                                       mem_limit_bytes=mem_limit_bytes,
                                       workers=workers,
                                       global_timeout=profile.global_settings.get(ProfileParameter.global_timeout, None))
    manager.execute_tasks_to_run(initial_tasks_to_run)

    manager = CoreManager(history_storage=history_storage,
                                       run_dir=target_dir,
                                       run_id=target_run_id,
                                       strategy=Strategy.single_run,
                                       check_results=check_results,
                                       try_parallel=False,
                                       mem_limit_bytes=mem_limit_bytes,
                                       workers=workers,
                                       global_timeout=profile.global_settings.get(ProfileParameter.global_timeout, None))
    manager.execute_tasks_to_run(target_tasks_to_run)

    runs_comparison_dict, runs_comparison_string = get_runs_comparison_analyze(history_storage.get_tasks_by_run_id(initial_run_id),
                                                       history_storage.get_tasks_by_run_id(target_run_id), target_df)

    comparison_path = comparison_dir / "comparison.txt"
    with open(comparison_path, "w", encoding="utf-8") as f:
        f.write(runs_comparison_string)

    generate_markdown_digest_jinja(runs_comparison_dict, comparison_dir, initial_path,
                                   target_path, COMPARISON_VERSION_DIGEST)





def create_profiling_dir_tree(profile_name: str, dataset_path: Path, base_output_dir: Optional[Path] = None) -> Path:
    """Creates a directory structure for a profiling run."""
    if base_output_dir is None:
        base_output_dir = Path.cwd()

    results_dir = base_output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    dataset_name = Path(dataset_path).stem
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = results_dir / f"{dataset_name}_{profile_name}_{timestamp}"
    run_dir.mkdir()
    return run_dir

def create_comparison_and_profiling_dir_tree(
    profile_name: str,
    baseline_path: Path,
    target_path: Path,
    base_output_dir: Optional[Path] = None
) -> Tuple[Path, Path, Path]:
    """
    Creates a directory structure for a comparison run, including subdirectories
    for individual profiling runs of baseline and target.
    """
    if base_output_dir is None:
        base_output_dir = Path.cwd()

    results_dir = base_output_dir / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    baseline_name = Path(baseline_path).stem
    target_name = Path(target_path).stem
    if baseline_name == target_name:
        baseline_name = f"{baseline_name}(baseline)"
        target_name = f"{target_name}(target)"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    comparison_dir = results_dir / f"comparison_{baseline_name}_{target_name}_{profile_name}_{timestamp}"
    comparison_dir.mkdir()
    baseline_run_dir = comparison_dir / f"profiling_{baseline_name}_{profile_name}_{timestamp}"
    baseline_run_dir.mkdir()
    target_run_dir = comparison_dir / f"profiling_{target_name}_{profile_name}_{timestamp}"
    target_run_dir.mkdir()
    return comparison_dir, baseline_run_dir, target_run_dir

def create_tasks_to_run(
    df: DataFrame,
    df_hash: Optional[str],
    strategy: Strategy,
    profile_tasks: List[TaskProfile]
) -> List[TaskToRun]:
    """Creates a list of TaskToRun objects from profile tasks."""
    tasks_to_run = [
        TaskToRun(
            task_id=str(uuid.uuid4()),
            algorithm_family=task.family,
            algorithm_name=task.algorithm,
            params=task.parameters,
            data=df,
            rows=df.shape[0],
            cols=df.shape[1],
            data_hash=df_hash,
            timeout=task.timeout,
            strategy=strategy,
            stage=1
        ) for task in profile_tasks
    ]
    return tasks_to_run
