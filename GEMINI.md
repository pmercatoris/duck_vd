# Gemini Project Context: duck_vd

This file contains the key architectural and tooling decisions for the `duck_vd` project.

## 1. Project Goal

To create a Python CLI tool named `duck_vd` that allows users to run SQL queries against local or remote data files (e.g., Parquet, CSV) using DuckDB and view the results interactively in VisiData.

## 2. Core Architecture

- **Tool Type:** A single, integrated CLI tool written in Python.
- **Query Engine:** The tool uses the `duckdb` Python library.
- **Cloud Access Strategy:** The tool uses a conditional backend approach:
    - For Google Cloud Storage (`gs://` URIs), it uses `gcsfs` to leverage the user's existing `gcloud` credentials seamlessly. This is handled by registering `gcsfs` with the DuckDB connection.
    - For other remote URLs (`https://`, `s3://`, etc.), it uses DuckDB's built-in `httpfs` extension.
- **Viewer Hand-off:** The tool launches `visidata` on a temporary Parquet file using `os.execvp`. This preserves data types and has been validated by the user.

## 3. Project Structure

- The project follows the standard `src` layout, with the main package located at `src/duck_vd`.

## 4. Development Toolchain

- **Package Management:** `uv`
- **Linting & Formatting:** `ruff`
- **LSP:** `ty` (from Astral)
- **CLI Framework:** `click`
- **Cloud Dependencies:** `gcsfs`, `fsspec`

## 5. Project Status

- **Phase 1 (MVP):** Complete. The tool can execute queries on local files, HTTPS URLs, and GCS paths, opening the results in VisiData. GCS authentication is working via `gcloud`.
- **Next:** Phase 2 (Caching).
