# Gemini Project Context: duck_vd

This file contains the key architectural and tooling decisions for the `duck_vd` project.

## 1. Project Goal

To create a Python CLI tool named `duck_vd` that allows users to run SQL queries against local or remote data files (e.g., Parquet, CSV) using DuckDB and view the results interactively in VisiData.

## 2. Core Architecture

- **Tool Type:** A single, integrated CLI tool written in Python.
- **Query Engine:** The tool uses the `duckdb` Python library.
- **Cloud Access Strategy:** The tool uses a conditional backend approach:
    - For Google Cloud Storage (`gs://` URIs), it uses `gcsfs` to leverage the user's existing `gcloud` credentials seamlessly.
    - For other remote URLs (`https://`, `s3://`, etc.), it uses DuckDB's built-in `httpfs` extension.
- **Caching:**
    - Caching is implemented in `~/.cache/duck_vd/`.
    - The SQL query string is hashed (SHA256) to serve as the cache key (filename).
    - Cache can be bypassed with `--no-cache` and cleared with `--clear-cache`.
- **Viewer Hand-off:** The tool launches `visidata` on the resulting Parquet file (from cache or a new query) using `os.execvp`.

## 3. Project Structure

- The project follows the standard `src` layout, with the main package located at `src/duck_vd`.

## 4. Development Toolchain

- **Package Management:** `uv`
- **Linting & Formatting:** `ruff`
- **LSP:** `ty` (from Astral)
- **CLI Framework:** `click`
- **Cloud Dependencies:** `gcsfs`

## 5. Project Status

- **Phase 1 (MVP):** Complete.
- **Phase 2 (Caching):** Complete.
- **Next:** User feedback and potential feature enhancements.