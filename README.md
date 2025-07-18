# duck_vd

A simple CLI tool to query local or remote data files (e.g., Parquet, CSV) with DuckDB and interactively view the results in VisiData.

## Features

- **Query Anything:** Run SQL queries on local files or remote objects in cloud storage.
- **Intelligent Caching:** Automatically caches query results to `~/.cache/duck_vd/`, so subsequent identical queries are instantaneous.
- **Broad Support:** Natively handles local files, HTTPS URLs, and Google Cloud Storage (`gs://`) buckets.
- **Seamless Viewing:** Opens query results directly in VisiData, preserving data types by using the Parquet format.
- **Glob Support:** Query multiple files at once using glob patterns (e.g., `gs://my-bucket/data/*.parquet`).

## Prerequisites

This tool requires Python 3.8+ and a few command-line tools.

### macOS Setup Guide

This guide will walk you through setting up all the necessary dependencies on a Mac.

**Step 1: Install Homebrew**

Homebrew is a package manager for macOS that makes it easy to install software. If you don't already have it, open the `Terminal` app and paste in the following command:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
Follow the on-screen instructions. For more details, see the [official Homebrew website](https://brew.sh/).

**Step 2: Install Tools with Homebrew**

Once Homebrew is installed, you can install `visidata`, `uv`, and the `google-cloud-sdk` with a single command.
```bash
brew install visidata uv google-cloud-sdk
```

**Step 3: Log in to Google Cloud**

To access files stored in Google Cloud Storage (`gs://`), you need to log in with your Google account. Run the following command and follow the instructions in your web browser:
```bash
gcloud auth application-default login
```

You are now ready to proceed with the installation of `duck_vd`.

---

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

**2. Query a remote Parquet file (the result will be cached):**
```bash
duck_vd 'https://duckdb.org/data/holdings.parquet'
```

**3. Run a complex query on GCS (the result will be cached):**
```bash
duck_vd "SELECT country, COUNT(*) AS num_records FROM 'gs://my-bucket/data/*.parquet' GROUP BY country ORDER BY num_records DESC;"
```

### Cache Management

**Force a refresh (bypass the cache):**
Use the `--no-cache` flag to re-run a query without reading from the cache.
```bash
duck_vd --no-cache 'gs://my-gcs-bucket/path/to/file.parquet'
```

**Clear the entire cache:**
This will delete all stored query results.
```bash
duck_vd --clear-cache
```

## How It Works

`duck_vd` intelligently selects the right backend for your data source:

-   **`gs://` URIs:** It uses the `gcsfs` library to handle authentication, automatically picking up your default `gcloud` credentials.
-   **`https://`, `s3://`, etc.:** It uses DuckDB's powerful built-in `httpfs` extension for optimized access.
-   **Local Files:** It reads directly from your local filesystem.

For any query, it generates a unique hash of the SQL command. If a file with this hash exists in `~/.cache/duck_vd/`, it is opened instantly. Otherwise, the query is executed, and the result is saved to the cache for future use.