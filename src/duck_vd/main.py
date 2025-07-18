import hashlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import click
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from gcsfs import GCSFileSystem
    gcsfs_available = True
except ImportError:
    gcsfs_available = False

CACHE_DIR = Path.home() / ".cache" / "duck_vd"

class DuckVdRunner:
    """Encapsulates the logic for running a duck_vd query."""

    def __init__(self, query_or_path: str, no_cache: bool):
        self.query_or_path = query_or_path
        self.no_cache = no_cache
        self.final_query = self._prepare_query()
        self.cache_file_path = self._get_cache_path()

    def run(self):
        """Orchestrates the query execution, caching, and viewing."""
        if not self.no_cache and self.cache_file_path.exists():
            click.echo(f"[Using cached result] Opening: {self.cache_file_path}", err=True)
            self._launch_visidata(self.cache_file_path)
            return

        click.echo(f"Executing query: {self.final_query}", err=True)
        try:
            result_table = self._execute_query()
            self._write_to_cache(result_table)
            click.echo(f"Query successful. Result cached: {self.cache_file_path}", err=True)
            self._launch_visidata(self.cache_file_path)
        except duckdb.Error as e:
            click.secho(f"DuckDB Error: {e}", fg="red", err=True)
            raise click.Abort()
        except Exception as e:
            click.secho(f"An unexpected error occurred: {e}", fg="red", err=True)
            raise click.Abort()

    def _prepare_query(self) -> str:
        """Determines the final SQL query string."""
        if is_query(self.query_or_path):
            return self.query_or_path
        return f"SELECT * FROM '{self.query_or_path}';"

    def _get_cache_path(self) -> Path:
        """Generates the deterministic cache file path from the query hash."""
        query_hash = hashlib.sha256(self.final_query.encode()).hexdigest()
        return CACHE_DIR / f"{query_hash}.parquet"

    def _execute_query(self) -> pa.Table:
        """Connects to DuckDB, sets up backends, and runs the query."""
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(database=':memory:', read_only=False)

        uri = find_uri_in_string(self.query_or_path)
        if uri:
            if uri.startswith('gs://'):
                if not gcsfs_available:
                    click.secho("Error: gs:// path detected, but 'gcsfs' is not installed.", fg="red", err=True)
                    click.echo("Please run: uv pip install gcsfs", err=True)
                    raise click.Abort()
                
                click.echo("gs:// path detected. Registering gcsfs filesystem...", err=True)
                gcs = GCSFileSystem()
                con.register_filesystem(gcs)
            else:
                click.echo("Remote path detected. Loading httpfs extension...", err=True)
                _ = con.execute("INSTALL httpfs;")
                _ = con.execute("LOAD httpfs;")
        
        return con.execute(self.final_query).fetch_arrow_table()

    def _write_to_cache(self, result_table: pa.Table):
        """Writes a PyArrow Table to the cache file."""
        pq.write_table(result_table, str(self.cache_file_path))

    def _launch_visidata(self, path: Path):
        """Replaces the current process with VisiData."""
        os.execvp("vd", ["vd", str(path)])

def clear_cache(ctx: click.Context, _param: click.Parameter, value: Any):
    """Callback to clear the cache and exit."""
    if not value or ctx.resilient_parsing:
        return
    if CACHE_DIR.exists():
        click.echo(f"Clearing cache at: {CACHE_DIR}")
        shutil.rmtree(CACHE_DIR)
        click.echo("Cache cleared.")
    else:
        click.echo("Cache directory does not exist, nothing to do.")
    ctx.exit()

def find_uri_in_string(s: str) -> str | None:
    """Finds the first URI (gs://, s3://, http://, etc.) in a string."""
    match = re.search(r"['\"]?((?:gs|s3|https?)://[^'\"]+)['\"]?", s)
    if match:
        return match.group(1)
    return None

def is_query(input_string: str) -> bool:
    """Heuristic to determine if the input is a SQL query."""
    input_upper = input_string.upper()
    return any(
        keyword in input_upper
        for keyword in ("SELECT ", "FROM ", "WITH ", "VALUES ")
    )

@click.command()
@click.argument("query_or_path", type=str, required=False)
@click.option('--no-cache', is_flag=True, help='Bypass the cache for a fresh query result.')
@click.option(
    '--clear-cache',
    is_flag=True,
    callback=clear_cache,
    expose_value=False,
    is_eager=True,
    help='Clear the entire query result cache and exit.',
)
def cli(query_or_path: str | None, no_cache: bool):
    """
    A CLI tool to query data with DuckDB and view it in VisiData.
    """
    if not query_or_path:
        click.echo("Error: Missing argument 'QUERY_OR_PATH'.", err=True)
        click.echo("Run with --help for usage information.", err=True)
        sys.exit(1)

    try:
        _ = subprocess.run(["which", "vd"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        click.secho("Error: VisiData (vd) not found in your PATH.", fg="red", err=True)
        click.echo("Please install it to use this tool: https://www.visidata.org/install/", err=True)
        raise click.Abort()

    runner = DuckVdRunner(query_or_path, no_cache)
    runner.run()

if __name__ == "__main__":
    cli()