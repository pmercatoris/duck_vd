import os
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import click
import duckdb


def is_url(path: str) -> bool:
    """Check if a given path is a URL."""
    try:
        result = urlparse(path)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def is_query(input_string: str) -> bool:
    """
    A simple heuristic to determine if the input is a SQL query.
    Returns True if it contains SELECT, FROM, WITH, or VALUES (case-insensitive).
    """
    input_upper = input_string.upper()
    return any(
        keyword in input_upper for keyword in ("SELECT ", "FROM ", "WITH ", "VALUES ")
    )


@click.command()
@click.argument("query_or_path", type=str)
def cli(query_or_path: str):
    """
    A CLI tool to query data with DuckDB and view it in VisiData.

    Takes a SQL QUERY or a file PATH as input.
    """
    # Ensure VisiData is installed
    try:
        subprocess.run(["which", "vd"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        click.secho(
            "Error: VisiData (vd) not found in your PATH.",
            fg="red",
        )
        click.echo(
            "Please install it to use this tool: https://www.visidata.org/install/"
        )
        raise click.Abort()

    final_query = ""
    if is_query(query_or_path):
        final_query = query_or_path
    elif is_url(query_or_path):
        final_query = f"SELECT * FROM '{query_or_path}';"
    else:
        # It's a path, wrap it in a SELECT statement
        path = Path(query_or_path).as_posix()
        final_query = f"SELECT * FROM '{path}';"

    click.echo(f"Executing query: {final_query}")

    try:
        # Connect to an in-memory DuckDB database
        con = duckdb.connect(database=":memory:", read_only=False)

        # Install and load httpfs extension for URL access
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        # Execute the query and fetch the result as an Arrow table
        result = con.execute(final_query).fetch_arrow_table()

        # Create a temporary file to store the Parquet output
        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".parquet"
        ) as tmp_file:
            tmp_file_path = tmp_file.name
            # Use pyarrow to write the result to a Parquet file
            import pyarrow.parquet as pq

            pq.write_table(result, tmp_file_path)

        click.echo(f"Query successful. Opening result in VisiData: {tmp_file_path}")

        # Use os.execvp to replace the current process with VisiData
        os.execvp("vd", ["vd", tmp_file_path])

    except duckdb.Error as e:
        click.secho(f"DuckDB Error: {e}", fg="red")
        raise click.Abort()
    except Exception as e:
        click.secho(f"An unexpected error occurred: {e}", fg="red")
        raise click.Abort()


if __name__ == "__main__":
    cli()
