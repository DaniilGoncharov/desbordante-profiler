import pytest
import shutil
from pathlib import Path
from click.testing import CliRunner
from src.desbordante_profiler import main

@pytest.fixture
def sample_csv(tmp_path):
    data = """col1,col2
1,foo
2,bar
3,baz
"""
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text(data, encoding="utf-8")
    return csv_file

@pytest.fixture
def simple_profile_yaml(tmp_path):
    content = """
name: "IntegrationTestProfile"
tasks:
  - family: fd
    algorithm: hyfd
"""
    profile_file = tmp_path / "profile.yaml"
    profile_file.write_text(content, encoding="utf-8")
    return profile_file

def test_integration_full_cycle(sample_csv, simple_profile_yaml, tmp_path):

    runner = CliRunner()

    result = runner.invoke(main, [
        "--profile", str(simple_profile_yaml),
        "--data", str(sample_csv)
    ])

    assert result.exit_code == 0

    results_dir = Path(__file__).resolve().parent.parent / "results"
    assert results_dir.exists()

    sub_dirs = list(results_dir.glob("IntegrationTestProfile*"))
    assert len(sub_dirs) >= 1

    run_dir = sub_dirs[0]
    log_file = run_dir / "log.txt"
    digest_file = run_dir / "digest.md"

    assert log_file.exists()
    assert digest_file.exists()

    try:
        shutil.rmtree(run_dir)
    except Exception as e:
        print(f"Error while deleting temporary folder {run_dir}: {e}")
