# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.11.21 AS uv

FROM python:3.14-slim AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock .python-version README.md LICENSE ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.14-slim
# Take the distribution's security fixes that landed after the base image was built: the vulnerability scan in CI
# fails on fixable HIGH findings. Nothing is installed at run time, so pip (and what it vendors) leaves the image.
RUN apt-get update \
    && apt-get upgrade --yes --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip uninstall --yes --quiet pip \
    && useradd --system --uid 10001 --home-dir /app ratchet
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
# The example workflows the compose stack serves. Your own image copies your package instead.
COPY examples ./examples
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1
USER ratchet
EXPOSE 8000
ENTRYPOINT ["ratchet"]
CMD ["api", "--host", "0.0.0.0", "--port", "8000"]
