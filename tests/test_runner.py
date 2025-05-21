import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from desbordante_profiler_package.core.runner import run_profile_on_dataset
from desbordante_profiler_package.core.profile_loader import Profile, TaskProfile
from desbordante_profiler_package.core.history import HistoryStorage
from desbordante_profiler_package.core.enums import Strategy, ProfileParameter
from desbordante_profiler_package.core.scheduler import TaskToRun

@pytest.fixture
def mock_profile() -> Profile:
    profile = MagicMock(spec=Profile)
    profile.name = "MockProfile"
    profile.global_settings = {}
    task1_profile = MagicMock(spec=TaskProfile)
    task1_profile.family = "fd"
    task1_profile.algorithm = "hyfd"
    task1_profile.parameters = {"max_fd_size": 3}
    task1_profile.timeout = None
    profile.tasks = [task1_profile]
    return profile

@pytest.fixture
def mock_history_storage() -> MagicMock:
    history = MagicMock(spec=HistoryStorage)
    history.get_tasks_by_run_id.return_value = [{"task_id": "some_task", "result": "Success"}]
    return history


@patch('desbordante_profiler_package.core.runner.load_profile')
@patch('desbordante_profiler_package.core.runner.create_profiling_dir_tree')
@patch('desbordante_profiler_package.core.runner.add_file_handler')
@patch('desbordante_profiler_package.core.runner.get_dataframe_and_hash')
@patch('desbordante_profiler_package.core.runner.create_tasks_to_run')
@patch('desbordante_profiler_package.core.runner.CoreManager')
@patch('desbordante_profiler_package.core.runner.generate_markdown_digest_jinja')
def test_run_profile_on_dataset_happy_path(
    mock_generate_digest: MagicMock,
    mock_core_manager_class: MagicMock,
    mock_create_tasks: MagicMock,
    mock_get_df_hash: MagicMock,
    mock_add_file_handler: MagicMock,
    mock_create_dir_tree: MagicMock,
    mock_load_profile: MagicMock,
    mock_profile: Profile,
    mock_history_storage: MagicMock,
    sample_csv_path: Path,
    sample_dataframe: Path,
    temp_dir: Path
):
    run_id = "test-run-123"
    profile_path_str = "/fake/profile.yaml"
    dataset_path_str = str(sample_csv_path)
    delimiter = ","
    has_header = True
    mem_limit_bytes = 1024 * 1024 * 512
    workers = 2
    check_results = True
    try_parallel = True
    strategy = Strategy.auto_decision
    timeout_step = 300
    timeout_max = 1800
    prune_factor = 0.7
    min_rows = 1000

    mock_load_profile.return_value = mock_profile
    mock_run_dir = temp_dir / "test_run_dir_output"
    mock_create_dir_tree.return_value = mock_run_dir
    mock_df_hash = "mocked_df_hash_xyz"
    mock_get_df_hash.return_value = (sample_dataframe, mock_df_hash)

    mock_task_to_run = MagicMock(spec=TaskToRun)
    mock_create_tasks.return_value = [mock_task_to_run]

    mock_core_manager_instance = MagicMock()
    mock_core_manager_class.return_value = mock_core_manager_instance

    run_profile_on_dataset(
        run_id=run_id,
        profile_path=profile_path_str,
        dataset_path=dataset_path_str,
        delimiter=delimiter,
        has_header=has_header,
        mem_limit_bytes=mem_limit_bytes,
        workers=workers,
        check_results=check_results,
        try_parallel=try_parallel,
        strategy=strategy,
        timeout_step=timeout_step,
        timeout_max=timeout_max,
        prune_factor=prune_factor,
        min_rows=min_rows,
        history_storage=mock_history_storage
    )

    mock_load_profile.assert_called_once_with(profile_path_str)
    mock_create_dir_tree.assert_called_once_with(mock_profile.name, dataset_path_str)
    mock_add_file_handler.assert_called_once_with(mock_run_dir / "profiling.log")

    expected_rows_arg = mock_profile.global_settings.get(ProfileParameter.rows)
    expected_cols_arg = mock_profile.global_settings.get(ProfileParameter.columns)
    mock_get_df_hash.assert_called_once_with(
        dataset_path_str, delimiter, has_header, expected_rows_arg, expected_cols_arg
    )

    mock_create_tasks.assert_called_once_with(
        sample_dataframe, mock_df_hash, Strategy(strategy), mock_profile.tasks
    )

    mock_core_manager_class.assert_called_once_with(
        history_storage=mock_history_storage,
        run_dir=mock_run_dir,
        run_id=run_id,
        strategy=Strategy(strategy),
        timeout_step=timeout_step,
        timeout_max=timeout_max,
        prune_factor=prune_factor,
        min_rows=min_rows,
        check_results=check_results,
        try_parallel=try_parallel,
        mem_limit_bytes=mem_limit_bytes,
        workers=workers,
        global_timeout=mock_profile.global_settings.get(ProfileParameter.global_timeout)
    )
    mock_core_manager_instance.execute_tasks_to_run.assert_called_once_with([mock_task_to_run])

    mock_history_storage.get_tasks_by_run_id.assert_called_once_with(run_id)
    mock_generate_digest.assert_called_once_with(
        mock_history_storage.get_tasks_by_run_id.return_value,
        mock_run_dir,
        dataset_path_str,
        None,
        "profiling_digest_template.md.j2"
    )

@patch('desbordante_profiler_package.core.runner.load_profile')
@patch('desbordante_profiler_package.core.runner.create_profiling_dir_tree')
@patch('desbordante_profiler_package.core.runner.add_file_handler')
@patch('desbordante_profiler_package.core.runner.get_dataframe_and_hash')
@patch('desbordante_profiler_package.core.runner.create_tasks_to_run')
@patch('desbordante_profiler_package.core.runner.CoreManager')
@patch('desbordante_profiler_package.core.runner.generate_markdown_digest_jinja')
def test_run_profile_on_dataset_with_global_settings(
    mock_generate_digest: MagicMock,
    mock_core_manager_class: MagicMock,
    mock_create_tasks: MagicMock,
    mock_get_df_hash: MagicMock,
    mock_add_file_handler: MagicMock,
    mock_create_dir_tree: MagicMock,
    mock_load_profile: MagicMock,
    mock_history_storage: MagicMock,
    sample_csv_path: Path,
    sample_dataframe: Path
):
    profile_with_settings = MagicMock(spec=Profile)
    profile_with_settings.name = "ProfileWithSettings"
    profile_with_settings.global_settings = {
        ProfileParameter.rows: 100,
        ProfileParameter.columns: 5,
        ProfileParameter.global_timeout: 3600
    }
    task_profile = MagicMock(spec=TaskProfile)
    task_profile.family="fd"; task_profile.algorithm="hyfd"; task_profile.parameters={}; task_profile.timeout=None
    profile_with_settings.tasks = [task_profile]

    mock_load_profile.return_value = profile_with_settings
    mock_get_df_hash.return_value = (sample_dataframe, "hash_settings")
    mock_create_tasks.return_value = [MagicMock(spec=TaskToRun)]
    mock_core_manager_instance = MagicMock()
    mock_core_manager_class.return_value = mock_core_manager_instance


    run_profile_on_dataset(
        run_id="settings-run",
        profile_path=Path("/fake/profile_settings.yaml"),
        dataset_path=str(sample_csv_path),
        delimiter=";", has_header=False, mem_limit_bytes=1024, workers=1,
        check_results=False, try_parallel=False, strategy=Strategy.ask,
        timeout_step=100, timeout_max=1000, prune_factor=0.5, min_rows=50,
        history_storage=mock_history_storage
    )

    mock_get_df_hash.assert_called_once_with(
        str(sample_csv_path), ";", False,
        profile_with_settings.global_settings[ProfileParameter.rows],
        profile_with_settings.global_settings[ProfileParameter.columns]
    )

    mock_core_manager_class.assert_called_once()
    assert mock_core_manager_class.call_args[1]['global_timeout'] == profile_with_settings.global_settings[ProfileParameter.global_timeout]

    mock_create_tasks.assert_called_once_with(
        sample_dataframe,
        "hash_settings",
        Strategy.ask,
        profile_with_settings.tasks
    )


@patch('desbordante_profiler_package.core.runner.load_profile', side_effect=FileNotFoundError("Mocked Profile Load Error"))
def test_run_profile_on_dataset_profile_load_error(
    mock_history_storage: MagicMock,
    sample_csv_path: Path
):
    with pytest.raises(FileNotFoundError, match="Mocked Profile Load Error"):
        run_profile_on_dataset(
            run_id="error-run", profile_path=Path("/bad/profile.yaml"), dataset_path=sample_csv_path,
            delimiter=",", has_header=True, mem_limit_bytes=1024, workers=1,
            check_results=True, try_parallel=True, strategy=Strategy.auto_decision,
            timeout_step=300, timeout_max=1800, prune_factor=0.7, min_rows=1000,
            history_storage=mock_history_storage
        )


@patch('desbordante_profiler_package.core.runner.load_profile')
@patch('desbordante_profiler_package.core.runner.create_profiling_dir_tree')
@patch('desbordante_profiler_package.core.runner.add_file_handler')
@patch('desbordante_profiler_package.core.runner.get_dataframe_and_hash', side_effect=SystemExit("Mocked CSV Load Error"))
def test_run_profile_on_dataset_csv_load_error(
    mock_get_df_hash_error: MagicMock,
    mock_add_file_handler:MagicMock,
    mock_create_dir_tree:MagicMock,
    mock_load_profile: MagicMock,
    mock_profile: Profile,
    mock_history_storage: MagicMock,
    sample_csv_path: Path
):
    mock_load_profile.return_value = mock_profile

    with pytest.raises(SystemExit, match="Mocked CSV Load Error"):
        run_profile_on_dataset(
            run_id="csv-error-run", profile_path=Path("/fake/profile.yaml"), dataset_path=Path("/bad/data.csv"),
            delimiter=",", has_header=True, mem_limit_bytes=1024, workers=1,
            check_results=True, try_parallel=True, strategy=Strategy.auto_decision,
            timeout_step=300, timeout_max=1800, prune_factor=0.7, min_rows=1000,
            history_storage=mock_history_storage
        )

    mock_load_profile.assert_called_once()
    mock_create_dir_tree.assert_called_once()
    mock_add_file_handler.assert_called_once()
    mock_get_df_hash_error.assert_called_once()
