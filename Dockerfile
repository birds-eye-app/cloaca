FROM python:3.12.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

ENV UV_PYTHON_PREFERENCE=only-system
ENV UV_LINK_MODE=copy

WORKDIR /app
COPY uv.lock pyproject.toml README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable
CMD ["uv", "run", "fastapi", "run", "src/cloaca/main.py"]
