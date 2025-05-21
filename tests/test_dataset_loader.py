import pytest
import pandas as pd
from pathlib import Path
import hashlib

from desbordante_profiler_package.core.dataset_loader import get_dataframe_and_hash, calculate_data_hash

def test_get_dataframe_and_hash_valid_csv(sample_csv_path: Path, sample_csv_data: str):
    df, df_hash = get_dataframe_and_hash(str(sample_csv_path), delimiter=",", has_header=True, rows=None, cols=None)

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (4, 3)
    assert list(df.columns) == ["col1", "col2", "col3"]
    assert df_hash is not None

    hasher = hashlib.sha256()
    hasher.update(sample_csv_data.encode('utf-8'))
    expected_hash = calculate_data_hash(str(sample_csv_path))
    assert df_hash == expected_hash


def test_get_dataframe_and_hash_slicing(sample_csv_path: Path):
    df_sliced, _ = get_dataframe_and_hash(str(sample_csv_path), delimiter=",", has_header=True, rows=2, cols=2)
    assert df_sliced.shape == (2, 2)
    assert list(df_sliced.columns) == ["col1", "col2"]
    assert df_sliced.iloc[0, 0] == 1
    assert df_sliced.iloc[1, 1] == 'b'

def test_get_dataframe_and_hash_no_header(temp_dir: Path):
    no_header_data = "val1,val2\nval3,val4"
    no_header_file = temp_dir / "no_header.csv"
    with open(no_header_file, "w") as f:
        f.write(no_header_data)

    df, _ = get_dataframe_and_hash(str(no_header_file), delimiter=",", has_header=False, rows=None, cols=None)
    assert df.shape == (2, 2)
    assert list(df.columns) == [0, 1]

def test_get_dataframe_and_hash_file_not_found(temp_dir: Path):
    with pytest.raises(SystemExit):
        get_dataframe_and_hash(str(temp_dir / "nonexistent.csv"), ",", True, None, None)


def test_calculate_data_hash_consistent(sample_csv_path: Path):
    hash1 = calculate_data_hash(str(sample_csv_path))
    hash2 = calculate_data_hash(str(sample_csv_path))
    assert hash1 is not None
    assert hash1 == hash2

def test_calculate_data_hash_different_files(sample_csv_path: Path, temp_dir: Path):
    hash1 = calculate_data_hash(str(sample_csv_path))

    different_content_file = temp_dir / "different.csv"
    with open(different_content_file, "w") as f:
        f.write("colA,colB\n10,20")
    hash2 = calculate_data_hash(str(different_content_file))

    assert hash1 is not None
    assert hash2 is not None
    assert hash1 != hash2

def test_calculate_data_hash_non_existent_file(temp_dir: Path):
    file_hash = calculate_data_hash(str(temp_dir / "imaginary.txt"))
    assert file_hash is None
