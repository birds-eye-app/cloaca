FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/


WORKDIR /app
COPY uv.lock .
COPY pyproject.toml .
COPY README.md .
RUN uv sync --frozen

COPY . .
CMD ["uv", "run", "fastapi", "run", "src/cloaca/main.py"]