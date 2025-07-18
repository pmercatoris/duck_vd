from pathlib import Path
from pytest_mock import MockerFixture

import pytest
from click.testing import CliRunner
import pyarrow as pa

from duck_vd.main import cli, DataFusionRunner, CACHE_DIR

DUMMY_CSV_CONTENT = "id,name\n1,a\n2,b"

@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()

@pytest.fixture
def mock_df_runner(mocker: MockerFixture):
    """Fixture to mock the DataFusionRunner's execute and launch methods."""
    dummy_table = pa.Table.from_pydict({})
    mocker.patch.object(DataFusionRunner, '_execute_query', return_value=dummy_table)
    mocker.patch.object(DataFusionRunner, '_launch_visidata')

@pytest.fixture
def mock_df_context(mocker: MockerFixture):
    """Fixture to mock the DataFusion SessionContext and its registration methods."""
    mock_ctx = mocker.MagicMock()
    mock_ctx.sql.return_value.to_arrow_table.return_value = pa.Table.from_pydict({})
    
    mocker.patch('datafusion.SessionContext', return_value=mock_ctx)
    # Also mock the GoogleCloud object store
    mocker.patch('duck_vd.main.GoogleCloud')
    return mock_ctx

# --- Test Cases ---

def test_default_query_on_folder(runner: CliRunner, mock_df_runner):
    """Tests that the default 'SELECT *' query is used when none is provided."""
    result = runner.invoke(cli, ['gs://my-bucket/data/', '--file-format', 'parquet'])
    assert result.exit_code == 0
    assert "Executing query on path: gs://my-bucket/data/" in result.stderr

def test_custom_query_on_file(runner: CliRunner, mock_df_runner):
    """Tests a custom query on a single file."""
    query = "SELECT name FROM table WHERE id > 0"
    result = runner.invoke(cli, ['local/file.csv', '--query', query])
    assert result.exit_code == 0
    assert f"Executing query on path: local/file.csv" in result.stderr

def test_folder_requires_format(runner: CliRunner, mock_df_runner):
    """Ensures that using a folder path without --file-format raises an error."""
    # We need to mock the runner differently here to allow the exception to be raised
    mocker.patch.object(DataFusionRunner, '_launch_visidata')
    result = runner.invoke(cli, ['gs://my-bucket/data/'])
    assert result.exit_code != 0
    assert "The --file-format option is required for folder paths" in result.output

def test_table_registration_logic(mock_df_context):
    """
    Verifies that the correct DataFusion registration method is called based on format.
    """
    # Test Parquet
    runner_parquet = DataFusionRunner("data.parquet", "SELECT *", None, True)
    runner_parquet._execute_query()
    mock_df_context.register_parquet.assert_called_with("table", "data.parquet")

    # Test CSV
    runner_csv = DataFusionRunner("data.csv", "SELECT *", "csv", True)
    runner_csv._execute_query()
    mock_df_context.register_csv.assert_called_with("table", "data.csv")

    # Test JSON
    runner_json = DataFusionRunner("data.json", "SELECT *", "json", True)
    runner_json._execute_query()
    mock_df_context.register_json.assert_called_with("table", "data.json")

def test_cache_key_is_unique(tmp_path: Path):
    """
    Ensures that the cache key is different for the same query on different paths.
    """
    query = "SELECT * FROM table"
    
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