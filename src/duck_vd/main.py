import os
import re
import subprocess
import tempfile

import click
import duckdb

# It's good practice to only import gcsfs if it's actually needed
try:
    from gcsfs import GCSFileSystem

    GCSFS_AVAILABLE = True
except ImportError:
    GCSFS_AVAILABLE = False


def find_uri_in_string(s: str) -> str | None:
    """Finds the first URI (gs://, s3://, http://, etc.) in a string."""
    # Regex to find a URI, whether it's quoted or not
    match = re.search(r"['\"]?((?:gs|s3|https?)://[^'\"]+)['\"]?", s)
    if match:
        return match.group(1)
    return None


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

    Takes a SQL QUERY or a file PATH (local, gs://, https://, etc.) as input.
    """
    # Ensure VisiData is installed
    try:
        subprocess.run(["which", "vd"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        click.secho("Error: VisiData (vd) not found in your PATH.", fg="red", err=True)
        click.echo(
            "Please install it to use this tool: https://www.visidata.org/install/",
            err=True,
        )
        raise click.Abort()

    final_query = (
        query_or_path
        if is_query(query_or_path)
        else f"SELECT * FROM '{query_or_path}';"
    )

    click.echo(f"Executing query: {final_query}", err=True)

    try:
        con = duckdb.connect(database=":memory:", read_only=False)

        # --- Conditional Backend Setup ---
        uri = find_uri_in_string(query_or_path)
        if uri:
            if uri.startswith("gs://"):
                if not GCSFS_AVAILABLE:
                    click.secho(
                        "Error: gs:// path detected, but 'gcsfs' is not installed.",
                        fg="red",
                        err=True,
                    )
                    click.echo("Please run: uv pip install gcsfs", err=True)
                    raise click.Abort()

                click.echo(
                    "gs:// path detected. Registering gcsfs filesystem...", err=True
                )
                gcs = GCSFileSystem()
                con.register_filesystem(gcs)
            else:
                # For https, s3, etc., use the built-in httpfs extension
                click.echo(
                    "Remote path detected. Loading httpfs extension...", err=True
                )
                con.execute("INSTALL httpfs;")
                con.execute("LOAD httpfs;")

        result = con.execute(final_query).fetch_arrow_table()

        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".parquet"
        ) as tmp_file:
            tmp_file_path = tmp_file.name
            import pyarrow.parquet as pq

            pq.write_table(result, tmp_file_path)

        click.echo(
            f"Query successful. Opening result in VisiData: {tmp_file_path}", err=True
        )
        os.execvp("vd", ["vd", tmp_file_path])

    except duckdb.Error as e:
        click.secho(f"DuckDB Error: {e}", fg="red", err=True)
        raise click.Abort()
    except Exception as e:
        click.secho(f"An unexpected error occurred: {e}", fg="red", err=True)
        raise click.Abort()


if __name__ == "__main__":
    cli()
