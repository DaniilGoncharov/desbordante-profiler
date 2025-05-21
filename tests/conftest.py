from typing import Any, Generator

import pytest
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import yaml

from desbordante_profiler_package.core.history import HistoryStorage
from desbordante_profiler_package.core.scheduler import TaskToRun
from desbordante_profiler_package.core.enums import Strategy, DictionaryField, TaskStatus, AlgorithmFamily, Algorithm

@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent

@pytest.fixture
def temp_dir() -> Generator[Path, Any, None]:
    d = Path(tempfile.mkdtemp())
    yield d
    shutil.rmtree(d)

@pytest.fixture
def sample_csv_data() -> str:
    return "col1,col2,col3\n1,a,x\n2,b,y\n3,c,z\n1,d,x"

@pytest.fixture
def sample_csv_path(temp_dir: Path, sample_csv_data: str) -> Path:
    csv_file = temp_dir / "sample.csv"
    with open(csv_file, "w") as f:
        f.write(sample_csv_data)
    return csv_file

@pytest.fixture
def sample_dataframe(sample_csv_data: str) -> pd.DataFrame:
    from io import StringIO
    return pd.read_csv(StringIO(sample_csv_data))

@pytest.fixture
def empty_history_storage(temp_dir: Path) -> HistoryStorage:
    history_file = temp_dir / "test_history.json"
    return HistoryStorage(filename=str(history_file))

@pytest.fixture
def sample_profile_content_minimal() -> dict:
    return {
        "name": "TestProfileMinimal",
        "tasks": [
            {
                "family": "fd",
                "algorithm": "hyfd",
                "parameters": {"threads": 3}
            }
        ]
    }

@pytest.fixture
def sample_profile_path(temp_dir: Path, sample_profile_content_minimal: dict) -> Path:
    profile_file = temp_dir / "sample_profile.yaml"
    with open(profile_file, 'w') as f:
        yaml.dump(sample_profile_content_minimal, f)
    return profile_file

@pytest.fixture
def sample_task_to_run(sample_dataframe: pd.DataFrame) -> TaskToRun:
    return TaskToRun(
        task_id="test-task-123",
        algorithm_family=str(AlgorithmFamily.fd),
        algorithm_name=str(Algorithm.hyfd),
        params={"max_fd_size": 3},
        data=sample_dataframe,
        rows=sample_dataframe.shape[0],
        cols=sample_dataframe.shape[1],
        data_hash="test_hash_123",
        timeout=60,
        strategy=str(Strategy.single_run),
        stage=1
    )

@pytest.fixture
def successful_run_info(sample_task_to_run: TaskToRun) -> dict:
    return {
        DictionaryField.task_id: sample_task_to_run.task_id,
        DictionaryField.algorithm: sample_task_to_run.algorithm_name,
        DictionaryField.algorithm_family: sample_task_to_run.algorithm_family,
        DictionaryField.params: sample_task_to_run.params,
        DictionaryField.result: TaskStatus.Success,
        DictionaryField.result_path: "path/to/result.pkl",
        DictionaryField.instances: 5,
        DictionaryField.data_hash: sample_task_to_run.data_hash,
        DictionaryField.rows: sample_task_to_run.rows,
        DictionaryField.cols: sample_task_to_run.cols,
    }

@pytest.fixture
def failed_run_info(sample_task_to_run: TaskToRun) -> dict:
    return {
        DictionaryField.task_id: sample_task_to_run.task_id,
        DictionaryField.algorithm: sample_task_to_run.algorithm_name,
        DictionaryField.algorithm_family: sample_task_to_run.algorithm_family,
        DictionaryField.params: sample_task_to_run.params,
        DictionaryField.result: TaskStatus.Failure,
        DictionaryField.error_type: TaskStatus.Timeout,
        DictionaryField.data_hash: sample_task_to_run.data_hash,
        DictionaryField.rows: sample_task_to_run.rows,
        DictionaryField.cols: sample_task_to_run.cols,
    }
