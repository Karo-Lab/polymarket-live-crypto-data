# FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# WORKDIR /app

# COPY . .

# RUN uv sync

# RUN mkdir -p /app/data /app/logs

# CMD ["uv", "run", "main.py"]
# 
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Set path for Rust/Cargo
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project


FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY . .

ENV PATH="/app/.venv/bin:$PATH"

RUN mkdir -p /app/data /app/logs

CMD ["uv", "run", "main.py"]