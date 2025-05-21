import pytest
import yaml
from pathlib import Path

from desbordante_profiler_package.core.profile_loader import load_profile, Profile, TaskProfile

def test_load_profile_valid(sample_profile_path: Path, sample_profile_content_minimal: dict):
    profile = load_profile(Path(sample_profile_path))
    assert isinstance(profile, Profile)
    assert profile.name == sample_profile_content_minimal["name"]
    assert len(profile.tasks) == 1
    task = profile.tasks[0]
    assert isinstance(task, TaskProfile)
    assert task.family == sample_profile_content_minimal["tasks"][0]["family"]
    assert task.algorithm == sample_profile_content_minimal["tasks"][0]["algorithm"]
    assert task.parameters == sample_profile_content_minimal["tasks"][0]["parameters"]

def test_load_profile_missing_file(temp_dir: Path):
    non_existent_file = temp_dir / "non_existent_profile.yaml"
    with pytest.raises(SystemExit):
        load_profile(Path(non_existent_file))

def test_load_profile_invalid_yaml(temp_dir: Path):
    invalid_yaml_file = temp_dir / "invalid.yaml"
    with open(invalid_yaml_file, "w") as f:
        f.write("name: Test\ntasks: [task1, task2")
    with pytest.raises(SystemExit):
        load_profile(Path(invalid_yaml_file))

def test_load_profile_task_no_family_no_algo(temp_dir: Path):
    content = {
        "name": "TestProfile",
        "tasks": [
            {"parameters": {"param": 1}}
        ]
    }
    profile_file = temp_dir / "profile_no_family_algo.yaml"
    with open(profile_file, 'w') as f:
        yaml.dump(content, f)

    profile = load_profile(Path(profile_file))
    assert len(profile.tasks) == 0

def test_load_profile_infer_algo_from_family(temp_dir: Path):
    content = {
        "name": "TestProfile",
        "tasks": [{"family": "fd"}]
    }
    profile_file = temp_dir / "profile_infer_algo.yaml"
    with open(profile_file, 'w') as f:
        yaml.dump(content, f)

    profile = load_profile(Path(profile_file))
    assert len(profile.tasks) == 1
    assert profile.tasks[0].family == "fd"
    assert profile.tasks[0].algorithm is not None

def test_load_profile_infer_family_from_algo(temp_dir: Path):
    content = {
        "name": "TestProfile",
        "tasks": [{"algorithm": "hyfd", "parameters": {}}]
    }
    profile_file = temp_dir / "profile_infer_family.yaml"
    with open(profile_file, 'w') as f:
        yaml.dump(content, f)

    profile = load_profile(Path(profile_file))
    assert len(profile.tasks) == 1
    assert profile.tasks[0].algorithm == "hyfd"
    assert profile.tasks[0].family == "fd"
