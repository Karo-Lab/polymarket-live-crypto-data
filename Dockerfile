FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY . .

RUN uv sync

RUN mkdir -p /app/data /app/logs

CMD ["uv", "run", "main.py"]