from abc import ABC, abstractmethod
from typing import Dict, Any
from src.logging_conf import get_logger

logger = get_logger(__name__)

class BaseRulesEngine(ABC):
    """Abstract base class for rule-based decision engines."""

    @abstractmethod
    def handle_failure(self, run_info: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @abstractmethod
    def handle_success(self, run_info: Dict[str, Any]) -> Dict[str, Any]:
        pass


class PythonRulesEngine(BaseRulesEngine):
    """Implements rules-based logic for handling profiling task failures and successes."""

    def __init__(self, history_storage):
        self.history_storage = history_storage

    def handle_failure(self, run_info: Dict[str, Any]) -> Dict[str, Any]:
        """Determines the action to take based on failure type."""
        algo, err = run_info.get("algorithm"), run_info.get("error_type")

        if err == "memory":
            errors = self.history_storage.get_recent_errors(algo, "memory", limit=2)
            if len(errors) >= 2:
                logger.info(f"Rules: too many memory errors for {algo}, skipping algorithm.")
                return {"action": "skip"}
            else:
                logger.info(f"Rules: retry {algo}.")
                return {"action": "retry", "params": {}}

        elif err == "timeout":
            logger.info("Rules: timed out, cutting dataframe.")
            return {"action": "cut_df", "params": {"divisor": 2}}
        else:
            return {"action": "skip", "params": {}}

    def handle_success(self, run_info: Dict[str, Any]) -> Dict[str, Any]:
        logger.debug(f"Rules: success for {run_info.get('algorithm')}.")
        return {"action": "", "params": {}}
