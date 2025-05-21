import click
import pickle
import logging
import desbordante
from typing import List, Dict, Any, Tuple
from pandas import DataFrame

from desbordante_profiler_package.core.enums import DictionaryField, TaskStatus
from desbordante_profiler_package.core.verification_algorithms import create_verification_algorithm, VERIFICATION_FAMILIES

logger = logging.getLogger(__name__)

def get_runs_comparison_analyze(
    baseline_tasks: List[Dict[str, Any]],
    target_tasks: List[Dict[str, Any]],
    df: DataFrame,
    auto_validation: bool = False
) -> Tuple[List[Dict[str, Any]], str]:
    """Analyzes and compares results from baseline and target profiling runs."""
    if not auto_validation:
        for task in target_tasks:
            if task.get(DictionaryField.result) != TaskStatus.Success:
                auto_validation = click.confirm(f"Some algorithms failed on target dataset. Do you want to run validation?")
                break

    comparison_result_dist = []
    comparison_result_string = "Comparison result:"

    for baseline_task in baseline_tasks:
        algorithm = baseline_task.get(DictionaryField.algorithm)
        algorithm_family = baseline_task.get(DictionaryField.algorithm_family)
        params = baseline_task.get(DictionaryField.params)
        algo_comparison_result_dict = {
            DictionaryField.algorithm: algorithm,
            DictionaryField.algorithm_family: algorithm_family,
            DictionaryField.params: params,
            DictionaryField.baseline_instances: baseline_task.get(DictionaryField.instances),
            DictionaryField.target_instances: 'N/A',
            DictionaryField.comparison: None
        }
        if baseline_task.get(DictionaryField.result) != TaskStatus.Success:
            algo_comparison_result_dict[DictionaryField.comparison] = "Failed on baseline dataset"
            continue
        try:
            with open(baseline_task.get(DictionaryField.result_path), "rb") as f:
                baseline_result_dict = pickle.load(f)
        except Exception as e:
            logger.warning(f"Error while loading serialized result: {e}. Skipping.")
            continue

        target_task = None
        for assumed_target_task in target_tasks:
            if (assumed_target_task.get(DictionaryField.algorithm) == algorithm and
                    assumed_target_task.get(DictionaryField.params) == params):
                target_task = assumed_target_task
                break

        if target_task.get(DictionaryField.result) != TaskStatus.Success:
            if auto_validation and algorithm_family in VERIFICATION_FAMILIES:
                # validate
                verification_algo = create_verification_algorithm(algorithm_family)
                broken_primitives = verification_algo.run(df, next(iter(baseline_result_dict.values())))
                if len(broken_primitives) == 0:
                    comparison_result_string = (f"{comparison_result_string}\n"
                                                f"All {algorithm_family.upper()}s by {algorithm} are hold")
                    algo_comparison_result_dict[DictionaryField.comparison] = "All instances are hold (validation)"
                else:
                    algo_comparison_result_dict[DictionaryField.comparison] = (f"Broken instances (validation): "
                                                                               f"{len(broken_primitives)}")
                    comparison_result_string = (f"{comparison_result_string}\n"
                                                f"{algorithm_family.upper()}s by {algorithm} validation:")
                    for broken_primitive in broken_primitives:
                        for info, payload in broken_primitive.items():
                            comparison_result_string = f"{comparison_result_string}\n\t{info}: {payload}"
            else:
                algo_comparison_result_dict[DictionaryField.comparison] = "Failed on target dataset"
                continue
        else:
            algo_comparison_result_dict[DictionaryField.target_instances] = target_task.get(DictionaryField.instances)
            try:
                with open(target_task.get(DictionaryField.result_path), "rb") as f:
                    target_result_dict = pickle.load(f)
            except Exception as e:
                logger.warning(f"Error while loading serialized result: {e}. Skipping.")
                continue

            for primitive, payload in baseline_result_dict.items():
                if payload != target_result_dict[primitive]:
                    # Some comparison operators for primitive instances are not working properly, after fix string
                    # based comparison is not necessary

                    # broken = [instance for instance in payload if instance not in target_result_dict[primitive]]
                    broken = []
                    for baseline_instance in payload:
                        exist = False
                        for target_instance in target_result_dict[primitive]:
                            if str(baseline_instance) == str(target_instance):
                                exist = True
                        if not exist:
                            broken.append(baseline_instance)

                    #new = [instance for instance in target_result_dict[primitive] if instance not in payload]
                    new = []
                    for target_instance in target_result_dict[primitive]:
                        exist = False
                        for baseline_instance in payload:
                            if str(baseline_instance) == str(target_instance):
                                exist = True
                                break
                        if not exist:
                            new.append(target_instance)

                    if len(broken) == 0 and len(new) == 0:
                        comparison_result_string = (f"{comparison_result_string}\n"
                                                    f"All {primitive.upper()}s by {algorithm} are hold")
                        algo_comparison_result_dict[DictionaryField.comparison] = f"All instances are hold"
                        continue

                    algo_comparison_result_dict[DictionaryField.comparison] = (f"Broken instances: {len(broken)}; "
                                                                               f"New instances: {len(new)}")
                    if len(broken) != 0:
                        comparison_result_string = (f"{comparison_result_string}\n"
                                                    f"Broken instances for {primitive.upper()}:")
                        for broken_instance in broken:
                            if type(broken_instance) is desbordante.od.ListOD:
                                broken_instance_str = f"{broken_instance.lhs} : {broken_instance.rhs}"
                            elif type(broken_instance) is desbordante.ac.ACRanges:
                                broken_instance_str = f"column indices: {broken_instance.column_indices}; ranges: {broken_instance.ranges}"
                            elif type(broken_instance) is desbordante.ac.ACException:
                                broken_instance_str = f"column pairs: {broken_instance.column_pairs}"
                            else:
                                broken_instance_str = str(broken_instance)
                            comparison_result_string = f"{comparison_result_string}\n\t{broken_instance_str}"
                    if len(new) != 0:
                        comparison_result_string = (f"{comparison_result_string}\n"
                                                    f"New instances for {primitive.upper()}:")
                        for new_instance in new:
                            if type(new_instance) is desbordante.od.ListOD:
                                new_instance_str = f"{new_instance.lhs} : {new_instance.rhs}"
                            elif type(new_instance) is desbordante.ac.ACRanges:
                                new_instance_str = f"column indices: {new_instance.column_indices}; ranges: {new_instance.ranges}"
                            elif type(new_instance) is desbordante.ac.ACException:
                                new_instance_str = f"column pairs: {new_instance.column_pairs}"
                            else:
                                new_instance_str = str(new_instance)
                            comparison_result_string = f"{comparison_result_string}\n\t{new_instance_str}"
                else:
                    comparison_result_string = (f"{comparison_result_string}\n"
                                                f"All {primitive.upper()}s by {algorithm} are hold")
                    algo_comparison_result_dict[DictionaryField.comparison] = f"All instances are hold"
            comparison_result_string = f"{comparison_result_string}\n"

        comparison_result_dist.append(algo_comparison_result_dict)

    return comparison_result_dist, comparison_result_string
