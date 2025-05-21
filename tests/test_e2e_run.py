import pytest
from click.testing import CliRunner
from pathlib import Path
import shutil

from desbordante_profiler_package.profiler_cli.desbordante_profiler import cli
import desbordante_profiler_package.core.runner

PROJECT_ROOT = Path(__file__).resolve().parent.parent

TEST_PROFILE_NAME = "TestE2ERunProfile"
TEST_DATASET_NAME = "DummyData"


@pytest.fixture(scope="function")
def test_env(tmp_path, monkeypatch):
    data_content = "colA,colB,colC\n1,alpha,10\n2,beta,20\n3,gamma,30"
    data_file = tmp_path / f"{TEST_DATASET_NAME}.csv"
    data_file.write_text(data_content)

    profile_content = f"""
name: {TEST_PROFILE_NAME}
global_settings:
  rows: 2
tasks:
  - family: fd
    algorithm: hyfd
    timeout: 20
  - family: ucc
    timeout: 20
"""
    profile_file = tmp_path / "dummy_profile.yml"
    profile_file.write_text(profile_content)
    dummy_template_dir = tmp_path / "templates"
    dummy_template_dir.mkdir()
    dummy_profiling_template_file = dummy_template_dir / desbordante_profiler_package.core.runner.PROFILING_DIGEST
    dummy_profiling_template_file.write_text("Dummy MD Content: {{ run_dir }}")
    #monkeypatch.setattr(desbordante_profiler_package.core.runner, 'DEFAULT_MD_TEMPLATES_DIR', str(dummy_template_dir))
    results_base_dir = PROJECT_ROOT / "tests" / "results"
    return data_file, profile_file, results_base_dir


def test_run_command_creates_expected_structure(test_env):
    data_file, profile_file, results_base_dir = test_env

    runner = CliRunner()
    result = runner.invoke(cli, [
        "run",
        "--profile", str(profile_file),
        "--data", str(data_file),
        "--workers", "1",
        "--skip_results_check",
        "--mem_limit", "512"
    ], catch_exceptions=False)

    assert result.exit_code == 0, f"CLI command failed: {result.output}"

    expected_run_dir_prefix = f"{TEST_DATASET_NAME}_{TEST_PROFILE_NAME}_"

    found_dirs = list(results_base_dir.glob(f"{expected_run_dir_prefix}*"))
    assert len(found_dirs) == 1, \
        f"Expected 1 run directory starting with '{expected_run_dir_prefix}', found {len(found_dirs)}: {found_dirs}"
    run_dir = found_dirs[0]

    try:
        assert run_dir.is_dir(), f"Run directory {run_dir} not found or not a directory."

        profiling_log = run_dir / desbordante_profiler_package.core.runner.DEFAULT_LOG_FILE
        assert profiling_log.is_file(), f"{desbordante_profiler_package.core.runner.DEFAULT_LOG_FILE} not found in {run_dir}"

        digest_md = run_dir / "digest.md"
        assert digest_md.is_file(), f"digest.md not found in {run_dir}"

        result_txt = run_dir / "result.txt"
        assert result_txt.is_file(), f"result.txt not found in {run_dir}"

        serialized_data_dir = run_dir / "serialized_data"
        assert serialized_data_dir.is_dir(), f"serialized_data directory not found in {run_dir}"

        pkl_files = list(serialized_data_dir.glob("*.pkl"))
        assert len(pkl_files) == 2, f"Expected 2 .pkl files for 2 tasks, found {len(pkl_files)}"

    finally:
        if results_base_dir and results_base_dir.exists():
            shutil.rmtree(results_base_dir)
        history_file = PROJECT_ROOT / "tests" / "history.json"
        if history_file.exists():
            history_file.unlink()
            pass
