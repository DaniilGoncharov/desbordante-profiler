from typing import Dict, Any
import logging
import math
import click
from desbordante_profiler_package.core.enums import DictionaryField, Strategy, TaskStatus, RulesField, RulesAction, RulesRetryParameter

logger = logging.getLogger(__name__)

MAX_STAGES = 3

def handle_failure(run_info: Dict[str, Any], timeout_step, timeout_max, prune_factor, min_rows) -> Dict[str, Any]:
    """Determines the action to take based on failure type."""
    error = run_info[DictionaryField.error_type]
    task = run_info[RulesField.task]

    # single_run
    if task.strategy == Strategy.single_run:
        return {RulesField.action: RulesAction.skip}

    # timeout_grow
    if task.strategy == Strategy.timeout_grow and error == TaskStatus.Timeout:
        new_timeout = (task.timeout or timeout_step) + timeout_step
        if new_timeout <= timeout_max:
            logger.info(f"Retry {task.algorithm_name} with timeout set to {new_timeout}.")
            return {RulesField.action: RulesAction.retry, RulesField.retry_params: {RulesRetryParameter.new_timeout: new_timeout}}
        else:
            logger.info(f"The timeout limit for {task.algorithm_name} has been reached. Skipping.")
            return {RulesField.action: RulesAction.skip}

    # prune_search
    if task.strategy == Strategy.shrink_search and error in (TaskStatus.Timeout, TaskStatus.MemoryError):
        df = task.data
        possible_rows = math.ceil(len(df) * prune_factor)
        if possible_rows >= min_rows:
            new_rows = possible_rows
            new_df = df.iloc[: new_rows]
            logger.info(f"Retry {task.algorithm_name} with rows set to {new_rows}.")
            return {RulesField.action: RulesAction.retry, RulesField.retry_params: {RulesRetryParameter.new_dataframe: new_df}}
        else:
            logger.info(f"The row limit for {task.algorithm_name} has been reached. Skipping.")
            return {RulesField.action: RulesAction.skip}

    # auto
    if task.strategy == Strategy.auto_decision and error in (TaskStatus.MemoryError, TaskStatus.Timeout):
        if task.stage >= MAX_STAGES:
            return {RulesField.action: RulesAction.skip}
        else:
            df = task.data
            new_df = df.iloc[: math.ceil(len(df) * prune_factor)]
            return {RulesField.action: RulesAction.retry,
                    RulesField.retry_params: {RulesRetryParameter.new_dataframe: new_df}}

    # ask
    if task.strategy == Strategy.ask and error in (TaskStatus.MemoryError, TaskStatus.Timeout):
        action = click.prompt(
            f"Algorithm {task.algorithm_name} failed. What would you like to do",
            type=click.Choice([RulesAction.skip, RulesAction.prune, RulesAction.retry], case_sensitive=False),
            show_choices=True,
            default=RulesAction.skip
        )
        match action:
            case RulesAction.skip:
                return {RulesField.action: RulesAction.skip}
            case RulesAction.retry:
                return {RulesField.action: RulesAction.retry,
                        RulesField.retry_params: {}}
            case RulesAction.prune:
                prune_factor = click.prompt(
                    "Enter prune factor from (0,1)",
                    type=click.FloatRange(0, 1, min_open=True, max_open=True),
                    default=0.7,
                    show_default=True
                )
                df = task.data
                new_df = df.iloc[: math.ceil(len(df) * prune_factor)]
                return {RulesField.action: RulesAction.retry,
                        RulesField.retry_params: {RulesRetryParameter.new_dataframe: new_df}}
    else:
        return {RulesField.action: RulesAction.skip}
