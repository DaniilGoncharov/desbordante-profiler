#!/usr/bin/env python3
import pickle
import sys
import click
import uuid


from datetime import datetime
from pathlib import Path
from typing import List

from src.logging_conf import configure_logging, add_file_handler, get_logger
from src.profiles_module import ProfileLoader
from src.data_module import CSVDataSource
from src.scheduler_module import TaskScheduler, Task
from src.rules_engine import PythonRulesEngine
from src.history_storage import HistoryStorage
from src.core_manager import CoreManager

logger = get_logger(__name__)


def load_data(data_source_type: str, data_path: str, delimiter: str):
    """Handles data loading based on the source type."""
    if data_source_type == "csv":
        csv_source = CSVDataSource(file_path=data_path, delimiter=delimiter, has_header=True)
        return csv_source.load_data()
    else:
        logger.error(f"Unsupported data source type: {data_source_type}")
        sys.exit(1)


def generate_markdown_digest(runs: List[dict], run_dir: Path) -> None:
    """Generates a Markdown digest summarizing the tasks executed in the current run."""
    md_lines = [
        "# Data Profiling Digest",
        "",
        f"**Run Directory:** `{run_dir}`",
        f"**Total Tasks Executed:** `{len(runs)}`",
        "",
        "| Algorithm | Parameters | Execution Time (s) | Result | Dependencies Count | Rule |",
        "|:----------|:----------:|-------------------:|:------:|:-------------------:|----:|"
    ]

    for run in reversed(runs):
        algo = run.get("algorithm", "N/A")
        params = run.get("params", {})
        exec_time = run.get("execution_time", 0)
        error = run.get("error_type", None)
        if error:
            result = error
            rule = run.get("rules_decision")
            dependency_count = "N/A"
        else:
            try:
                with open(run.get("result"), "rb") as f:
                    objects = pickle.load(f)[1]
                    dependency_count = len(objects)
            except Exception as e:
                logger.warning(f"Failed to load serialized data: {e}")
            result = "success"
            rule = "N/A"

        md_lines.append(
            f"| {algo} | `{params}` | {round(exec_time, 8)} | {result} | {dependency_count} | {rule} |"
        )

    digest_file = run_dir / "digest.md"
    with open(digest_file, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))


@click.command()
@click.option("--profile", type=str, required=True, help="Path to YAML profile configuration. If omitted, a minimal profile is used.")
@click.option("--data", type=str, required=True, help="Path/URL to data file (if overriding profile data_source).")
@click.option("--data-source-type", type=click.Choice(["csv", "db", "http"]), default="csv", show_default=True,
              help="Type of data source: 'csv', 'db', or 'http'.")
@click.option("--delimiter", type=str, default=",", show_default=True,
              help="CSV delimiter (if data-source-type=csv).")
@click.option("--check_results", is_flag=True, help="Option to check previous runs for results.")
@click.option("--log-level", type=str, default="INFO", show_default=True,
              help="Logging level (DEBUG, INFO, WARN, etc.)")
def main(profile, data, data_source_type, delimiter, check_results, log_level):
    """Main entry point for the data profiling application."""
    configure_logging(log_level)
    logger.info("Starting Data Profiler...")

    results_dir = Path(__file__).resolve().parent.parent / 'results'
    results_dir.mkdir(exist_ok=True)

    try:
        profile_obj = ProfileLoader.load_profile(profile)
        logger.info(f"Loaded profile '{profile_obj.name}' with {len(profile_obj.tasks)} tasks.")
    except Exception as e:
        logger.error(f"Error loading profile: {e}")
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = results_dir / f"{profile_obj.name} {timestamp}"
    run_dir.mkdir()
    add_file_handler(run_dir / "log.txt")

    df = load_data(data_source_type, data, delimiter)

    tasks = [
        Task(
            task_id=str(uuid.uuid4()),
            algorithm_family=task.family or "unknown",
            algorithm_name=task.algorithm,
            params=task.parameters,
            data=df,
            data_name=data,
            timeout=task.timeout
        ) for task in profile_obj.tasks
    ]
    logger.info(f"Created {len(tasks)} tasks from profile.")

    scheduler = TaskScheduler(max_workers=1)
    history = HistoryStorage("history.json")
    rules = PythonRulesEngine(history)
    core_manager = CoreManager(
        scheduler=scheduler,
        rules_engine=rules,
        history_storage=history
    )

    run_id = str(uuid.uuid4())
    try:
        core_manager.run_profile(tasks, run_dir, run_id, check_results)
        logger.info("Profiling completed.")
    except Exception as e:
        logger.exception(f"Unexpected error during profiling: {e}")
        sys.exit(1)

    generate_markdown_digest(history.get_tasks_by_run_id(run_id), run_dir)

if __name__ == "__main__":
    main()
