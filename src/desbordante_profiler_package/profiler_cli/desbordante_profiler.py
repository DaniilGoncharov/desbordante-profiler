#!/usr/bin/env python3
import click
import uuid

from desbordante_profiler_package.core.log_config import configure_core_logger, add_console_handler
from desbordante_profiler_package.core.history import HistoryStorage
from desbordante_profiler_package.core.runner import run_profile_on_dataset, compare_with_subset, compare_with_new_version
from desbordante_profiler_package.core.util import get_correct_number_of_workers, get_correct_bytes_mem_limit

@click.group(help="Desbordante data‑profiling toolkit")
@click.version_option(package_name="desbordante-profiler")
def cli():
    pass

@cli.command("run", help="Mine primitives on a dataset using a YAML profile")
@click.option("--profile", "profile_path", type=click.Path(exists=True, readable=True), required=True,
              help="Path to YAML profile file")
@click.option("--data", "data_path", type=click.Path(exists=True, readable=True), required=True,
              help="Path to dataset (CSV)")
@click.option("--delimiter", default=",", show_default=True,
              help="CSV delimiter")
@click.option("--has_header", default=True, show_default=True)
@click.option("--strategy",
              type=click.Choice(["auto_decision", "ask", "timeout_grow", "prune_search", "single_run"]),
              default="ask", show_default=True, help="Failure‑handling strategy to apply during mining")
@click.option("--timeout_step", type=click.IntRange(min=1), default=300, show_default=True,
              help="Seconds to add per retry in timeout_grow mode")
@click.option("--timeout_max", type=click.IntRange(min=1), default=1800, show_default=True,
              help="Upper timeout limit in timeout_grow mode")
@click.option("--prune_factor", type=click.FloatRange(0, 1, min_open=True, max_open=True),
              default=0.7, show_default=True, help="Prune factor for dataset size in prune_search mode")
@click.option("--min_rows", type=click.IntRange(min=1), default=1000, show_default=True,
              help="Minimal number of rows to keep when pruning dataset")
@click.option("--skip_results_check", is_flag=True, help="Skip searching for already stored .pkl results")
@click.option("--no_parallel", is_flag=True, help="Don't try to run tasks in parallel")
@click.option("--log_level", default="INFO", show_default=True)
@click.option("--mem_limit", type=click.IntRange(min=1),
              help="Maximum memory (in MB) that is allowed to use.")
@click.option("--workers", type=click.IntRange(min=0), default=0, show_default=True,
              help="Number of CPU cores to use. Use 0 for maximum available.")
def run_profile(profile_path: str, data_path: str, delimiter: str, has_header: bool,
                strategy: str, timeout_step: int, timeout_max: int,
                prune_factor: int, min_rows: int,
                skip_results_check: bool, no_parallel: bool, log_level: str, mem_limit, workers):
    configure_core_logger()
    add_console_handler(log_level)
    run_id = str(uuid.uuid4())
    history_storage = HistoryStorage()
    workers = get_correct_number_of_workers(workers)
    mem_limit_bytes = get_correct_bytes_mem_limit(mem_limit)
    run_profile_on_dataset(run_id=run_id,
                           profile_path=profile_path,
                           dataset_path=data_path,
                           delimiter=delimiter,
                           has_header=has_header,
                           mem_limit_bytes=mem_limit_bytes,
                           workers=workers,
                           check_results=not skip_results_check,
                           try_parallel=not no_parallel,
                           strategy=strategy,
                           timeout_step=timeout_step,
                           timeout_max=timeout_max,
                           prune_factor=prune_factor,
                           min_rows=min_rows,
                           history_storage=history_storage)


if __name__ == "__main__":
    cli()
