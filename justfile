set shell := ["bash", "-cu"]

# Default: list available recipes.
default:
    @just --list

# Editable install: build the Rust extension and install in the active venv.
develop:
    uv run --no-project maturin develop --release

# Build a distributable wheel containing Python + Rust .so.
build:
    uv run --no-project maturin build --release
    @echo
    @ls -1 ch_http_native/target/wheels/*.whl

# Run the test suite (spins up ClickHouse via docker-compose).
test:
    uv run pytest tests/ -v

# Run all benchmarks (ClickHouse must be reachable on localhost:8123).
bench:
    @echo "=== compare_http.py ==="
    uv run python benchmarks/compare_http.py
    @echo
    @echo "=== compare_streaming.py ==="
    uv run python benchmarks/compare_streaming.py

# Sync the dev environment from uv.lock (creates .venv if missing).
sync:
    uv sync

# Remove build artifacts: target dir, wheels, dist, caches.
clean:
    rm -rf ch_http_native/target dist build
    find . -type d -name __pycache__ -prune -exec rm -rf {} +
    find . -type d -name '*.egg-info' -prune -exec rm -rf {} +
