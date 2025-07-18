# duck_vd

A simple CLI tool to query local or remote data files (e.g., Parquet, CSV) with DuckDB and interactively view the results in VisiData.

## Features

- **Query Anything:** Run SQL queries on local files or remote objects in cloud storage.
- **Broad Support:** Natively handles local files, HTTPS URLs, and Google Cloud Storage (`gs://`) buckets.
- **Seamless Viewing:** Opens query results directly in VisiData, preserving data types by using the Parquet format for data transfer.
- **Glob Support:** Query multiple files at once using glob patterns (e.g., `gs://my-bucket/data/*.parquet`).

## Prerequisites

Before you begin, ensure you have the following installed:

1.  **Python 3.8+**
2.  **uv**: The project's package manager. [Installation instructions](https://github.com/astral-sh/uv).
3.  **VisiData**: The interactive data viewer. [Installation instructions](https://www.visidata.org/install/).
4.  **(Optional) Google Cloud SDK**: To access files in GCS, you need the `gcloud` CLI installed and authenticated.
    ```bash
    gcloud auth application-default login
    ```

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/pmercatoris/duck_vd.git
    cd duck_vd
    ```

2.  **Sync Dependencies:**
    Use `uv` to create a virtual environment and install all required dependencies.
    ```bash
    uv sync
    ```

3.  **Install the CLI:**
    Perform an editable install to make the `duck_vd` command available in your shell.
    ```bash
    uv pip install -e .
    ```
    You may need to activate the virtual environment to run the command: `source .venv/bin/activate`.

## Usage

The tool accepts a single argument: a path to a file, a URL, or a full SQL query as a string.

### Examples

**1. Query a local CSV file:**
```bash
duck_vd data/my_file.csv
```

**2. Query a remote Parquet file over HTTPS:**
```bash
duck_vd 'https://duckdb.org/data/holdings.parquet'
```

**3. Query a single file on Google Cloud Storage:**
```bash
duck_vd 'gs://my-gcs-bucket/path/to/file.parquet'
```

**4. Run a SQL query on a glob of files in GCS:**
```bash
duck_vd "SELECT * FROM 'gs://my-bucket/data/2023-*.parquet' WHERE city = 'Madrid';"
```

**5. Run an aggregate query and view the result:**
```bash
duck_vd "SELECT country, COUNT(*) AS num_records FROM 'gs://my-bucket/data/*.parquet' GROUP BY country ORDER BY num_records DESC;"
```

## How It Works

`duck_vd` intelligently selects the right backend for your data source:

-   **`gs://` URIs:** It uses the `gcsfs` library to handle authentication, automatically picking up your default `gcloud` credentials.
-   **`https://`, `s3://`, etc.:** It uses DuckDB's powerful built-in `httpfs` extension for optimized access.
-   **Local Files:** It reads directly from your local filesystem.

In all cases, it runs the query and saves the result to a temporary Parquet file, which is then passed to VisiData. This ensures that data types (like dates, numbers, etc.) are correctly preserved.
