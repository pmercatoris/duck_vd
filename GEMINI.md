# Gemini Project Context: duck_vd

This file contains the key architectural and tooling decisions for the `duck_vd` project.

## 1. Project Goal

To create a Python CLI tool named `duck_vd` that allows users to run SQL queries against local or remote data files (e.g., Parquet, CSV) using DuckDB and view the results interactively in VisiData.

## 2. Core Architecture

- **Tool Type:** A single, integrated CLI tool written in Python.
- **Query Engine:** The tool will use the `duckdb` Python library.
- **Cloud Access:** It will leverage DuckDB's native cloud provider support (e.g., the `httpfs` extension for GCS/S3) to execute high-performance queries with projection and predicate pushdown.
- **Caching:**
    - A caching mechanism will be implemented to improve performance for repeated queries.
    - The SQL query string will be hashed to serve as a cache key.
    - Query results will be stored as **Parquet files** in a local cache directory.
- **Viewer Hand-off:** The tool launches `visidata` on a temporary Parquet file using `os.execvp`. This approach has been validated by the user and works correctly.

## 3. Project Structure

- The project follows the standard `src` layout, with the main package located at `src/duck_vd`.

## 4. Development Toolchain

- **Package Management:** `uv`
- **Linting & Formatting:** `ruff`
- **LSP:** `ty` (from Astral)
- **CLI Framework:** `click`

## 5. Project Status

- **Phase 1 (MVP):** Complete. The tool can execute queries/paths and open the results in VisiData via a temporary Parquet file.
- **Next:** Phase 2 (Caching).