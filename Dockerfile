FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev --compile-bytecode

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Prevent uv from ever trying to download or sync at runtime
ENV UV_NO_SYNC=1
ENV UV_PYTHON_DOWNLOADS=never
# Ensure Python doesn't buffer logs so you see them in real-time
ENV PYTHONUNBUFFERED=1 

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY . .

CMD ["uv", "run", "--no-sync", "main.py"]