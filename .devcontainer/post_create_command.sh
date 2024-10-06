#!/bin/bash

# Make the zsh.sh script executable and run it
chmod +x .devcontainer/zsh.sh
.devcontainer/zsh.sh

# Configure Poetry
poetry config --local virtualenvs.path .venv

# Initialize Poetry project if pyproject.toml doesn't exist
if [ ! -f pyproject.toml ]; then
    poetry init --no-interaction
fi

# Install dependencies
poetry install --no-root

