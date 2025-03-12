import yaml
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

class TaskProfile:
    """Represents the profile of a single task including algorithm details and parameters."""
    def __init__(self,
                 family: Optional[str] = None,
                 algorithm: Optional[str] = None,
                 parameters: Optional[Dict[str, Any]] = None,
                 timeout: Optional[int] = None):
        self.family = family
        self.algorithm = algorithm
        self.parameters = parameters or {}
        self.timeout = timeout

    def __repr__(self):
        return (f"TaskProfile(family={self.family}, algorithm={self.algorithm}, "
                f"parameters={self.parameters}, timeout={self.timeout})")


class Profile:
    """Represents a collection of tasks."""
    def __init__(self,
                 name: str,
                 tasks: List[TaskProfile],
                 global_settings: Optional[Dict[str, Any]] = None):
        self.name = name
        self.tasks = tasks
        self.global_settings = global_settings or {}

    def __repr__(self):
        return (f"Profile(name='{self.name}', tasks={self.tasks}, "
                f"global_settings={self.global_settings})")


class ProfileLoader:
    """Handles loading and parsing task profiles from YAML file."""

    @staticmethod
    def load_profile(file_path: str) -> Profile:
        logger.info(f"Loading profile from {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error reading YAML profile '{file_path}': {e}")
            raise RuntimeError(f"Error reading YAML profile '{file_path}': {e}")

        if not isinstance(config, dict):
            raise ValueError("Top-level YAML content must be a dict.")

        name = config.get("name", "UnnamedProfile")
        global_settings = config.get("global_settings", {})
        tasks_config = config.get("tasks", [])

        if not isinstance(tasks_config, list):
            raise ValueError("YAML 'tasks' section must be a list.")

        tasks: List[TaskProfile] = []
        for idx, tcfg in enumerate(tasks_config):
            if not isinstance(tcfg, dict):
                raise ValueError(f"Task index {idx} must be a dict.")
            family = tcfg.get("family")
            algo = tcfg.get("algorithm")
            params = tcfg.get("parameters", {})
            timeout = tcfg.get("timeout", None)

            if not family and not algo:
                logger.warning(f"Task {idx} in profile has no 'task' nor 'algorithm' specified. Skipping.")
                continue
            if not algo:
                algo = get_algorithm_name_by_family(family)
            if not family:
                family = get_family_by_algorithm(algo, params)

            tp = TaskProfile(family=family, algorithm=algo, parameters=params, timeout=timeout)
            tasks.append(tp)

        prof = Profile(name=name, tasks=tasks, global_settings=global_settings)
        # logger.info(f"Profile loaded: {prof}")
        return prof

DEFAULT_ALGORITHMS = {
    'fd': 'hyfd',
    'afd': 'pyro',
    'ind': 'spider',
    'ucc': 'hpivalid',
    'od': 'fastod',
    'ar': 'apriori',
    'dd': 'split',
    'cfd': 'fd_first'
}

def get_algorithm_name_by_family(family):
    algorithm_name = DEFAULT_ALGORITHMS[family]
    return algorithm_name

def get_family_by_algorithm(algorithm, params):
    family_name = None

    match algorithm:
        case "split":
            family_name = "dd"
        case "apriori":
            family_name = "ar"
        case algorithm if algorithm in ("fastod", "order"):
            family_name = "od"
        case "fd_first":
            family_name = "cfd"
        case algorithm if algorithm in ("hpivalid", "hyucc", "pyroucc"):
            family_name = "ucc"
        case algorithm if algorithm in ("spider", "faida"):
            family_name = "ind"
        case algorithm if algorithm in ("pyro", "tane"):
            if params.get("error", 0):
                family_name = "afd"
            else:
                family_name = "fd"
        case algorithm if algorithm in ("hyfd", "fd_mine", "dfd"):
            family_name = "fd"
        case _:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
    return family_name

