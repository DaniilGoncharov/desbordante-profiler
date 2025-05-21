import sys
import yaml
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

from desbordante_profiler_package.core.enums import ProfileParameter
from desbordante_profiler_package.core.mining_algorithms import get_family_by_algorithm, get_algorithm_name_by_family

logger = logging.getLogger(__name__)


class TaskProfile:
    """Represents the profile of a single task including algorithm details and parameters."""

    def __init__(self,
                 family: Optional[str] = None,
                 algorithm: Optional[str] = None,
                 parameters: Optional[Dict[str, Any]] = None,
                 timeout: Optional[int] = None) -> None:
        self.family = family
        self.algorithm = algorithm
        self.parameters = parameters or {}
        self.timeout = timeout


class Profile:
    """Represents a collection of tasks."""

    def __init__(self,
                 name: str,
                 tasks: List[TaskProfile],
                 global_settings: Optional[Dict[str, Any]] = None) -> None:
        self.name = name
        self.tasks = tasks
        self.global_settings = global_settings or {}


def load_profile(profile_path: Path) -> Profile:
    logger.info(f"Loading profile from {profile_path}")
    try:
        with open(profile_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error reading YAML profile '{profile_path}': {e}")
        sys.exit(1)

    if not isinstance(config, dict):
        logger.error("Top-level YAML content must be a dict.")
        sys.exit(1)

    name = config.get(ProfileParameter.name, "UnnamedProfile")
    global_settings = config.get(ProfileParameter.global_settings, {})
    tasks_config = config.get(ProfileParameter.tasks, [])

    if not isinstance(tasks_config, list):
        logger.error(f"YAML '{ProfileParameter.tasks}' section must be a list.")
        sys.exit(1)

    tasks: List[TaskProfile] = []
    for idx, tcfg in enumerate(tasks_config):
        if not isinstance(tcfg, dict):
            logger.error(f"Task index {idx} must be a dict.")
            sys.exit(1)
        family = tcfg.get(ProfileParameter.family)
        algo = tcfg.get(ProfileParameter.algorithm)
        params = tcfg.get(ProfileParameter.parameters, {})
        timeout = tcfg.get(ProfileParameter.timeout, None)

        if not family and not algo:
            logger.warning(f"Task {idx} in profile has no '{ProfileParameter.family}' nor '{ProfileParameter.algorithm}' specified. Skipping.")
            continue
        if not algo:
            algo = get_algorithm_name_by_family(family)
        if not family:
            family = get_family_by_algorithm(algo, params)

        task_profile = TaskProfile(family=family, algorithm=algo, parameters=params, timeout=timeout)
        tasks.append(task_profile)

    profile = Profile(name=name, tasks=tasks, global_settings=global_settings)
    logger.info(f"Profile loaded: {name}")
    return profile






