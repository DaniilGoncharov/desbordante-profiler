import psutil
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from jinja2 import Environment, PackageLoader

logger = logging.getLogger(__name__)

DEFAULT_MEMORY_PERCENT = 0.75

def get_percent_of_available_memory(percent: float = DEFAULT_MEMORY_PERCENT) -> int:
    """Calculates a specified percentage of currently available system memory."""
    vm = psutil.virtual_memory()
    return int(vm.available * percent)

def get_correct_number_of_workers(workers: int) -> int:
    """Determines the actual number of workers to use based on available CPU cores."""
    available_cores = psutil.cpu_count(logical=True)
    if workers == 0:
        return available_cores
    else:
        return min(available_cores, workers)

def get_correct_bytes_mem_limit(mem_limit: Optional[int]) -> int:
    """Converts a memory limit from megabytes to bytes, or calculates a default."""
    return mem_limit * (1024 ** 2) if mem_limit else get_percent_of_available_memory()

def generate_markdown_digest_jinja(
    runs: List[Dict[str, Any]],
    run_dir: Path,
    baseline: Optional[Path],
    target: Optional[Path],
    template_name: str
) -> None:
    """Generates a Markdown digest using a Jinja2 template."""

    env = Environment(loader=PackageLoader("desbordante_profiler_package", "templates"),
                      trim_blocks=True, lstrip_blocks=True,
                      autoescape=False)

    try:
        template = env.get_template(template_name)
    except Exception as e:
        logger.warning(f"Error loading template '{template_name}' from package templates: {e}")
        return
    context = {
        "run_dir": str(run_dir),
        "runs": runs,
        "baseline": baseline,
        "target": target
    }

    try:
        markdown_content = template.render(context)
    except Exception as e:
        logger.warning(f"Error rendering template: {e}")
        return

    digest_file = run_dir / "digest.md"
    try:
        with open(digest_file, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        logger.info(f"Markdown digest saved to {digest_file}")
    except IOError as e:
        logger.warning(f"Error writing digest file: {e}")
