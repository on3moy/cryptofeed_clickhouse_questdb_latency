# ── Builder stage ─────────────────────────────────────────────
# Build the image for what we need to use!
# Full image — has gcc/build tools needed to compile C extension packages
FROM python:3.12-bookworm AS builder

# Copy the uv binary from its official image instead of installing via pip
COPY --from=ghcr.io/astral-sh/uv:0.6.6 /uv /usr/local/bin/uv

# UV_COMPILE_BYTECODE=1:    pre-compile .pyc files — faster container startup
# UV_LINK_MODE=copy:        copy files instead of hardlink (required across filesystems e.g. cache mounts)
# UV_PYTHON_DOWNLOADS=never: use the image's Python — don't let uv download its own
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /cryptofeed

# Install dependencies only — cache mount avoids re-downloading on rebuild
# bind mounts make uv.lock and pyproject.toml available without COPYing them into the layer
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project
    # --frozen:              fail if uv.lock is out of sync with pyproject.toml
    # --no-dev:              skip dev dependencies
    # --no-install-project:  don't install this repo as a package, just its deps

# ── Runtime stage ─────────────────────────────────────────────
# 🦖 Only use the .venv which is needed! We dont need the full build image.

# Slim image — no build tools, significantly smaller final image
FROM python:3.12-slim-bookworm

# Copy only the installed .venv from builder — leaves build tools behind
COPY --from=builder /cryptofeed/.venv /cryptofeed/.venv

# Prepend .venv/bin so `python` and installed scripts resolve from the venv
ENV PATH="/cryptofeed/.venv/bin:$PATH"

WORKDIR /cryptofeed

# Copy application source — done last so code changes don't invalidate dep layers
COPY src/ /cryptofeed/src

# Entrypoint — exec form (JSON array) is preferred over shell form
CMD ["python", "/cryptofeed/src/script/main_runner.py"]
