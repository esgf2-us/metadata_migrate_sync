FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# No need for a venv in a container
ENV UV_PROJECT_ENVIRONMENT=/usr/local

# Install the dependencies
COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --no-dev --no-cache --no-editable --no-install-project

# Install the app separately to avoid rebuilding the image when the app changes
COPY src/  README.md /app/
RUN uv sync --frozen --no-dev --no-cache --no-editable

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT ["esgf15mms"]

# Run the application
# CMD ["esgf15mms"]