# duck_vd

A simple CLI tool to query local or remote data files (e.g., Parquet, CSV, JSON) with **Apache DataFusion** and interactively view the results in VisiData.

## Features

- **Powerful Query Engine:** Uses Apache DataFusion to run SQL queries on local files or remote objects in cloud storage.
- **Native Cloud Support:** Natively handles Google Cloud Storage (`gs://`) buckets with automatic `gcloud` authentication.
- **Intelligent Caching:** Automatically caches query results to `~/.cache/duck_vd/`, so subsequent identical queries on the same data are instantaneous.
- **Flexible Data Sources:** Query single files or entire folders of data.
- **Seamless Viewing:** Opens query results directly in VisiData, preserving data types by using the Parquet format.

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

The tool is invoked by providing a `PATH` to a data source, along with options to specify a query and the data format.

```bash
duck_vd [OPTIONS] PATH
```

### Arguments & Options

-   `PATH`: (Required) The path to your data source. Can be a local file, a GCS path (`gs://...`), or a folder.
-   `-q`, `--query`: The SQL query to execute. Use the special name `table` to refer to the data source. Defaults to `"SELECT * FROM table"`.
-   `-f`, `--file-format`: The format of the data (`parquet`, `csv`, `json`). **Required** when `PATH` is a folder.
-   `--no-cache`: Bypass the cache for a fresh query result.
-   `--clear-cache`: Clear the entire query result cache and exit.

### Examples

**1. View a local CSV file (default query):**
```bash
duck_vd local_data/my_file.csv
```

**2. Query a folder of Parquet files on GCS:**
```bash
duck_vd gs://my-bucket/data/ --file-format parquet
```

**3. Run a custom aggregate query on a folder of JSON files:**
```bash
duck_vd gs://my-bucket/logs/ --file-format json --query "SELECT level, COUNT(*) FROM table GROUP BY 1"
```

### Cache Management

**Force a refresh (bypass the cache):**
```bash
duck_vd --no-cache 'gs://my-gcs-bucket/path/to/data/' --file-format parquet
```

**Clear the entire cache:**
```bash
duck_vd --clear-cache
```

## How It Works

`duck_vd` uses Apache DataFusion to register your data source (whether local or on GCS) as a table named `table`. It then executes your SQL query against this table. DataFusion's native `object_store` support handles GCS authentication and performs optimized reads (predicate/projection pushdown) to only download the data it needs.

For caching, it generates a unique hash from the combination of the data path and the SQL query. If a file with this hash exists in `~/.cache/duck_vd/`, it is opened instantly. Otherwise, the query is executed, and the result is saved to the cache for future use.
