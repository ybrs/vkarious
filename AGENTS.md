# Repository Guidelines

## Project Structure & Module Organization
- `src/vkarious/`: core package. CLI entry in `cli.py`; database helpers in `db.py`; SQL migrations in `migration/`.
- `tests/`: pytest suite for CLI and internals.
- `pyproject.toml`: project metadata (Hatchling), script entry point `vkarious= vkarious.cli:cli`.
- `README.md`: setup, configuration, and usage examples.

## Build, Test, and Development Commands
- `uv venv`: create a Python 3.11+ virtual env.
- `uv pip install -e .`: install in editable mode.
- `uv run vkarious --help`: verify CLI wiring.
- `uv run vkarious databases list`: example command; requires `VKA_DATABASE`.
- `uv run pytest -q`: run tests; adds `src/` to `PYTHONPATH` via pytest config.

## Coding Style & Naming Conventions
- Python: follow PEP 8; include type hints and docstrings for public APIs.
- Names: modules and functions use snake_case; classes use PascalCase; constants UPPER_SNAKE_CASE.
- Paths: prefer `pathlib.Path` for filesystem operations.
- CLI: use Click patterns (groups/commands) consistent with `cli.py`.
- Never use list compherensions or lambda in python code. 
- Write code object oriented as possible. 

## Testing Guidelines
- Framework: pytest (see `tool.pytest.ini_options` in `pyproject.toml`).
- Location/naming: place tests in `tests/` as `test_*.py` with `test_*` functions.
- CLI tests: use `click.testing.CliRunner()`; keep output stable and human-readable.
- Run `uv run pytest` before pushing; add tests with new features and for bug fixes.

## Commit & Pull Request Guidelines
- Commits: concise, imperative mood. Conventional Commits are welcome (e.g., `feat: add snapshot delete`, `docs: update README`).
- PRs: include a clear description, linked issues, reproduction/verification steps, and relevant CLI output (paste snippets). Ensure tests pass locally.

## Security & Configuration Tips
- Configure `VKA_DATABASE` with a non-production DSN during development. Do not commit secrets.
- Snapshot/restore operations can be destructive; test against disposable databases.

## Integration Tests
- Ensure PostgreSQL is running locally and accessible via a DSN.
- Create a virtual environment and install dependencies:
  - `uv venv --python /usr/bin/python3`
  - `uv pip install -e . pytest`
- Export a DSN for tests, e.g. `export VKA_DATABASE="postgresql:///postgres"`.
- Run the DDL logging tests (using peer auth by invoking as the `postgres` user):
  - `sudo -u postgres VKA_DATABASE=$VKA_DATABASE uv run pytest tests/test_ddl_integration.py -q`
