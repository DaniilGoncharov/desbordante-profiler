import pytest
import pandas as pd
from src.data_module import CSVDataSource

@pytest.fixture
def sample_csv(tmp_path):
    data = """col1,col2
1,foo
2,bar
3,baz
"""
    file_path = tmp_path / "test.csv"
    file_path.write_text(data, encoding="utf-8")
    return file_path

def test_csv_data_source_ok(sample_csv):
    ds = CSVDataSource(file_path=str(sample_csv), delimiter=",", has_header=True)
    df = ds.load_data()

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (3, 2)
    assert df.columns.tolist() == ["col1", "col2"]
    assert df.iloc[0]["col1"] == 1

def test_csv_data_source_no_file():
    ds = CSVDataSource(file_path="non_existent.csv")
    with pytest.raises(RuntimeError, match="Failed to load CSV"):
        ds.load_data()
