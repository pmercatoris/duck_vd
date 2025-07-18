from pathlib import Path
from pytest_mock import MockerFixture

import pytest
from click.testing import CliRunner
import pyarrow as pa

from duck_vd.main import cli, DataFusionRunner, CACHE_DIR

@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()

@pytest.fixture
def mock_df_context(mocker: MockerFixture):
    """Fixture to mock the DataFusion SessionContext and its registration methods."""
    mock_ctx = mocker.MagicMock()
    mock_ctx.sql.return_value.to_arrow_table.return_value = pa.Table.from_pydict({})
    
    mocker.patch('datafusion.SessionContext', return_value=mock_ctx)
    mocker.patch('duck_vd.main.GoogleCloud')
    return mock_ctx

# --- Test Cases ---

def test_table_registration_logic(mocker: MockerFixture):
    """
    Verifies that the correct DataFusion registration method is called based on format.
    """
    mock_ctx = mocker.patch('datafusion.SessionContext').return_value
    mock_ctx.sql.return_value.to_arrow_table.return_value = pa.Table.from_pydict({})
    
    # Test Parquet
    runner_parquet = DataFusionRunner("data.parquet", "SELECT *", None, True)
    runner_parquet._execute_query()
    mock_ctx.register_parquet.assert_called_with("mytable", "data.parquet")

    # Test CSV
    runner_csv = DataFusionRunner("data.csv", "SELECT *", "csv", True)
    runner_csv._execute_query()
    mock_ctx.register_csv.assert_called_with("mytable", "data.csv")

    # Test JSON
    runner_json = DataFusionRunner("data.json", "SELECT *", "json", True)
    runner_json._execute_query()
    mock_ctx.register_json.assert_called_with("mytable", "data.json")

def test_cache_key_is_unique():
    """
    Ensures that the cache key is different for the same query on different paths.
    """
    query = "SELECT * FROM mytable"
    
    runner1 = DataFusionRunner("path/one", query, "parquet", False)
    runner2 = DataFusionRunner("path/two", query, "parquet", False)
    
    assert runner1.cache_file_path != runner2.cache_file_path

def test_clear_cache_command(runner: CliRunner):
    """
    Verifies that the --clear-cache command removes the cache directory.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / "dummy_file").touch()
    assert CACHE_DIR.exists()

    result = runner.invoke(cli, ['--clear-cache'])

    assert result.exit_code == 0
    assert "Cache cleared" in result.output
    assert not CACHE_DIR.exists()
