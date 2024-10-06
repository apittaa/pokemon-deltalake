# Build the base image
FROM python:3.12.6-slim-bookworm AS base

WORKDIR /app/

ENV PYTHONPATH "${PYTHONPATH}:/app/"

# Build the development image
FROM base AS dev

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    docker.io \
    curl \
    unzip \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*  # Clean up apt cache

# Install DuckDB CLI
RUN curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip -o duckdb-cli.zip && \
    unzip duckdb-cli.zip -d /usr/local/bin/ && \
    rm duckdb-cli.zip

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir pre-commit==4.0.0 && \
    pip install --no-cache-dir poetry==1.8.2 && \
    if [ -f poetry.lock ]; then poetry config virtualenvs.create false; fi && \
    if [ -f pyproject.toml ]; then poetry install --only main --no-root; fi

COPY src/ /app/src/

# Expose the port the app runs on
EXPOSE 8080

# Build the pipeline image
FROM base AS prod

COPY pyproject.toml /app/
RUN pip install --upgrade pip && \
    pip install poetry==1.8.2 && \
    if [ -f poetry.lock ]; then poetry config virtualenvs.create false; fi && \
    poetry install --only main --no-root

COPY src/ /app/src/

ENTRYPOINT ["python", "/app/src/main.py"]
