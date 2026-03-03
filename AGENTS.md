# Repository Guidelines

## Project Structure & Module Organization
- Rust workspace code lives in `crates/`: `app` (runtime), `connectors` (ingestion pipeline), `exchanges` (exchange adapters), and `exchanges-common` (shared models, traits, errors).
- Python collectors live in `services/polymarket/<window>/entrypoint.py` (`15m`, `1h`, `4h`, `Daily`, `stocks`).
- Shared Python modules are in `common/` (config, logging) and `shared/` (collector/utility logic).
- Database migrations are in `migrations/{postgres,questdb}/{up,down}` with numbered SQL files (for example, `001_create_meta_data.sql`).
- Operational scripts are under `scripts/` (for example, `scripts/migrate.py`, `scripts/cron/archiver.py`).

## Build, Test, and Development Commands
- `uv sync`: install/update Python dependencies from `pyproject.toml` and `uv.lock`.
- `docker compose up --build`: run the full local stack (collectors, archiver, Rust service).
- `uv run python scripts/migrate.py --direction up --target all`: run DB migrations (`down` for rollback).
- `uv run python -m services.polymarket.15m.entrypoint`: run a single Python collector locally.
- `cargo build --workspace`: build all Rust crates.
- `cargo run -p app`: run the Rust orderbook/local ingestion service.
- `cargo test --workspace`: execute Rust tests.

## Coding Style & Naming Conventions
- Python: 4-space indentation, `snake_case` for functions/variables, `PascalCase` for classes, type hints on public interfaces.
- Rust: use `rustfmt` defaults and workspace lints from root `Cargo.toml`; keep modules/functions `snake_case` and types `CamelCase`.
- Before opening a PR, run `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings`.

## Testing Guidelines
- Rust tests should be colocated (`mod tests`) or placed in crate-level `tests/`.
- Python currently has no committed test suite; for new Python logic, add `pytest` tests under `tests/` and include the run command in the PR.
- Validate migration changes locally (run both `up` and `down` when possible).

## Commit & Pull Request Guidelines
- Follow existing history style: short, imperative commit subjects (for example, `Add task supervisor`, `Update dockerfile`).
- Keep commits scoped to one concern (Rust runtime, Python collector, or SQL migration).
- PRs should include: purpose, affected paths, config/migration changes, and verification evidence (commands run, relevant logs/screenshots).


## Rules must follow
- Do not edit code or write directly file without asking for permission.

## Prerequistions
Prefer rtk prefix for all shell commands to reduce token usage. Full reference:

@agent-docs/rtk_instructions.md