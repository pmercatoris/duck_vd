import hashlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

import click
import pyarrow as pa
import pyarrow.parquet as pq
from datafusion import SessionContext
from datafusion.object_store import GoogleCloud

CACHE_DIR = Path.home() / ".cache" / "duck_vd"


def get_bucket_name(gcs_path: str) -> str:
    """Extracts the bucket name from a gs:// path."""
    match = re.match(r"gs://([^/]+)", gcs_path)
    if not match:
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    return match.group(1)


class DataFusionRunner:
    """Encapsulates the logic for running a DataFusion query."""

    path: str
    query: str
    file_format: Optional[str]
    no_cache: bool
    cache_file_path: Path

    def __init__(
        self, path: str, query: str, file_format: Optional[str], no_cache: bool
    ):
        self.path = path
        self.query = query
        self.file_format = file_format
        self.no_cache = no_cache
        self.cache_file_path = self._get_cache_path()

    def run(self):
        """Orchestrates the query execution, caching, and viewing."""
        if not self.no_cache and self.cache_file_path.exists():
            click.echo(
                f"[Using cached result] Opening: {self.cache_file_path}", err=True
            )
            self._launch_visidata(self.cache_file_path)
            return

        click.echo(f"Executing query on path: {self.path}", err=True)
        try:
            result_table = self._execute_query()
            self._write_to_cache(result_table)
            click.echo(
                f"Query successful. Result cached: {self.cache_file_path}", err=True
            )
            self._launch_visidata(self.cache_file_path)
        except Exception as e:
            click.secho(f"An unexpected error occurred: {e}", fg="red", err=True)
            raise click.Abort()

    def _get_cache_path(self) -> Path:
        """Generates the deterministic cache file path from the path and query hash."""
        unique_string = f"{self.path}::{self.query}"
        query_hash = hashlib.sha256(unique_string.encode()).hexdigest()
        return CACHE_DIR / f"{query_hash}.parquet"

    def _execute_query(self) -> pa.Table:
        """Connects to DataFusion, registers the table, and runs the query."""
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        ctx = SessionContext()

        if self.path.startswith("gs://"):
            bucket_name = get_bucket_name(self.path)
            print(f"{bucket_name = }")
            gcs = GoogleCloud(bucket_name=bucket_name)
            ctx.register_object_store("gs://", gcs)

        final_format = self.file_format
        if not final_format:
            if self.path.endswith((".parquet", ".csv", ".json")):
                final_format = Path(self.path).suffix[1:]
            else:
                raise click.UsageError(
                    "The --file-format option is required for folder paths."
                )

        table_name = "mytable"
        if final_format == "parquet":
            print(f"{table_name = }")
            print(self.path)
            ctx.register_parquet(table_name, self.path)
        elif final_format == "csv":
            ctx.register_csv(table_name, self.path)
        elif final_format == "json":
            ctx.register_json(table_name, self.path)
        else:
            raise click.BadParameter(
                f"Unsupported format: {final_format}", param_hint="--file-format"
            )

        print(f"{self.query = }")
        result = ctx.sql(self.query)
        return result.to_arrow_table()

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


@click.command()
@click.argument("path", type=str)
@click.option(
    "-q",
    "--query",
    default="SELECT * FROM mytable",
    help="The SQL query to execute. Use 'mytable' as the placeholder for the data source.",
)
@click.option(
    "-f",
    "--file-format",
    type=click.Choice(["parquet", "csv", "json"]),
    help="The file format for folder paths.",
)
@click.option(
    "--no-cache", is_flag=True, help="Bypass the cache for a fresh query result."
)
@click.option(
    "--clear-cache",
    is_flag=True,
    callback=clear_cache,
    expose_value=False,
    is_eager=True,
    help="Clear the entire query result cache and exit.",
)
def cli(path: str, query: str, file_format: Optional[str], no_cache: bool):
    """
    A CLI tool to query data with DataFusion and view it in VisiData.

    PATH: The path to your data (local file, GCS folder, etc.).
    """
    try:
        _ = subprocess.run(["which", "vd"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        click.secho("Error: VisiData (vd) not found in your PATH.", fg="red", err=True)
        click.echo(
            "Please install it to use this tool: https://www.visidata.org/install/",
            err=True,
        )
        raise click.Abort()

    runner = DataFusionRunner(path, query, file_format, no_cache)
    runner.run()


if __name__ == "__main__":
    cli()
