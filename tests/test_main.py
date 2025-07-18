import os
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner
import pyarrow as pa

from duck_vd.main import cli, DuckVdRunner, CACHE_DIR

# Dummy data for our tests
DUMMY_CSV_CONTENT = "id,name\n1,a\n2,b"

@pytest.fixture
def runner():
    """Fixture for invoking command-line interfaces."""
    return CliRunner()

@pytest.fixture
def mock_db(mocker):
    """Fixture to mock all DuckDB interactions."""
    # Create a realistic, empty pyarrow Table to be the return value
    dummy_table = pa.Table.from_pydict({})
    
    mock_con = mocker.MagicMock()
    mock_con.execute.return_value.fetch_arrow_table.return_value = dummy_table
    mocker.patch('duckdb.connect', return_value=mock_con)
    return mock_con

@pytest.fixture
def mock_exec(mocker):
    """Fixture to mock os.execvp to prevent VisiData from launching."""
    return mocker.patch('os.execvp')

# --- Test Cases ---

def test_local_file_execution(tmp_path, mock_db, mock_exec):
    """
    Tests the core logic of running a query on a local file.
    """
    # 1. Create a dummy CSV file
    dummy_csv = tmp_path / "test.csv"
    dummy_csv.write_text(DUMMY_CSV_CONTENT)

    # 2. Instantiate and run the runner
    app_runner = DuckVdRunner(query_or_path=str(dummy_csv), no_cache=False)
    app_runner.run()

    # 3. Assert that the correct query was executed
    expected_query = f"SELECT * FROM '{dummy_csv.as_posix()}';"
    mock_db.execute.assert_called_with(expected_query)

    # 4. Assert that VisiData was called with the correct cache file path
    assert mock_exec.call_count == 1
    assert mock_exec.call_args[0][0] == "vd"
    cached_file = Path(mock_exec.call_args[0][1][1])
    assert cached_file.parent == CACHE_DIR
    assert cached_file.exists()

def test_gcs_backend_is_selected(mocker, mock_db, mock_exec):
    """
    Verifies that the gcsfs backend is registered for gs:// URIs.
    """
    mocker.patch('duck_vd.main.gcsfs_available', True)
    mock_gcs_fs = mocker.patch('duck_vd.main.GCSFileSystem')

    app_runner = DuckVdRunner(query_or_path="gs://my-bucket/file.parquet", no_cache=True)
    app_runner.run()

    # Assert that gcsfs was used and httpfs was NOT
    mock_gcs_fs.assert_called_once()
    mock_db.register_filesystem.assert_called_once()
    assert not any("INSTALL httpfs" in call[0][0] for call in mock_db.execute.call_args_list)

def test_https_backend_is_selected(mock_db, mock_exec):
    """
    Verifies that the httpfs backend is loaded for https:// URLs.
    """
    app_runner = DuckVdRunner(query_or_path="https://example.com/file.parquet", no_cache=True)
    app_runner.run()

    # Assert that httpfs was used and gcsfs was NOT
    mock_db.register_filesystem.assert_not_called()
    assert any("INSTALL httpfs" in call[0][0] for call in mock_db.execute.call_args_list)

def test_cache_hit_skips_query(mocker, mock_exec):
    """
    Confirms that an existing cache file prevents a new query from running.
    """
    # 1. Mock the query execution method to spy on it
    mock_execute_query = mocker.patch.object(DuckVdRunner, '_execute_query')

    # 2. Create a dummy cache file
    query = "SELECT * FROM 'some_table'"
    app_runner = DuckVdRunner(query_or_path=query, no_cache=False)
    cache_file = app_runner.cache_file_path
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file.touch()

    # 3. Run the runner
    app_runner.run()

    # 4. Assert that the query was NOT executed and VisiData was called
    mock_execute_query.assert_not_called()
    mock_exec.assert_called_once_with("vd", ["vd", str(cache_file)])

def test_no_cache_forces_query(mocker, mock_exec):
    """
    Ensures the --no-cache flag forces a query even if a cache file exists.
    """
    # 1. Mock the query execution method and give it a valid return value
    dummy_table = pa.Table.from_pydict({})
    mock_execute_query = mocker.patch.object(DuckVdRunner, '_execute_query', return_value=dummy_table)

    # 2. Create a dummy cache file
    query = "SELECT * FROM 'some_table'"
    app_runner = DuckVdRunner(query_or_path=query, no_cache=True) # no_cache is True
    cache_file = app_runner.cache_file_path
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file.touch()

    # 3. Run the runner
    app_runner.run()

    # 4. Assert that the query WAS executed
    mock_execute_query.assert_called_once()

def test_clear_cache_command(runner):
    """
    Verifies that the --clear-cache command removes the cache directory.
    """
    # 1. Create a dummy cache directory and file
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / "dummy_file").touch()
    assert CACHE_DIR.exists()

    # 2. Invoke the CLI with the --clear-cache flag
    result = runner.invoke(cli, ['--clear-cache'])

    # 3. Assert the command succeeded and the directory is gone
    assert result.exit_code == 0
    assert "Cache cleared" in result.output
    assert not CACHE_DIR.exists()
