# Gemini Project Context: duck_vd

This file contains the key architectural and tooling decisions for the `duck_vd` project.

## 1. Project Goal

To create a Python CLI tool named `duck_vd` that allows users to run SQL queries against local or remote data files (e.g., Parquet, CSV) using **DataFusion** and view the results interactively in VisiData.

## 2. Core Architecture

- **Tool Type:** A single, integrated CLI tool written in Python.
- **Core Logic:** The application's logic is encapsulated in a `DataFusionRunner` class.
- **Query Engine:** The tool uses the **`datafusion`** Python library.
- **CLI Structure:** The tool uses a `PATH` argument for the data source and options for the query (`--query`) and format (`--file-format`).
- **Cloud Access Strategy:** It uses DataFusion's native `object_store` capabilities, which automatically discover and use `gcloud` credentials for GCS access.
- **Caching:**
    - Caching is implemented in `~/.cache/duck_vd/`.
    - A hash of the **data path and the SQL query** is used as the cache key.
    - Cache can be bypassed with `--no-cache` and cleared with `--clear-cache`.
- **Viewer Hand-off:** The tool launches `visidata` on the resulting Parquet file using `os.execvp`.

## 3. Project Structure

- The project follows the standard `src` layout, with the main package located at `src/duck_vd`.

## 4. Development Toolchain

- **Package Management:** `uv`
- **Linting & Formatting:** `ruff`
- **LSP & Type Checking:** `ty`, `basedpyright`
- **Testing:** `pytest`, `pytest-mock`
- **CLI Framework:** `click`
- **Core Engine:** `datafusion`

## 5. Testing

- A test suite is located in the `tests/` directory, focused on the `DataFusionRunner` class.
- External services and processes are mocked using `pytest-mock`.

## 6. Project Status

- **Current State:** The project has been refactored to use DataFusion as its core engine. The test suite is being finalized.

## 7. Future Improvements Roadmap

This section outlines potential future work to enhance the project.

### High Impact (User-Facing)
- **Query Normalization for Caching:**
    - **Goal:** Improve the cache hit rate by normalizing SQL queries before hashing.
    - **Implementation:** Convert query to lowercase, standardize whitespace, etc.
- **Flexible Output Options:**
    - **Goal:** Allow the tool to be used in scripts and other pipelines.
    - **Implementation:** Add an `--output` (`-o`) flag to control the output format (e.g., `vd`, `csv`, `json`, or save to a named file).
- **Granular Cache Management:**
    - **Goal:** Give users more control over the cache.
    - **Implementation:** Add subcommands like `duck_vd cache-list` and `duck_vd cache-path <query>`.

### Medium Impact (Developer Experience & Maintenance)
- **Configuration File:**
    - **Goal:** Allow users to customize the tool's behavior.
    - **Implementation:** Support a config file (e.g., `~/.config/duck_vd/config.toml`).
- **Codebase Refactoring:**
    - **Goal:** Improve code organization as more features are added.
    - **Implementation:** Split the logic from `main.py` into a more structured package (e.g., `cli.py`, `runner.py`, `caching.py`).

### Low Impact (Distribution)
- **Publish to PyPI:**
    - **Goal:** Make the tool easily installable for a wider audience.
    - **Implementation:** Add necessary metadata to `pyproject.toml` and use `uv publish`.
