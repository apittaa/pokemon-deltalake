#!/bin/bash

# Add safe.directory for the workspace
git config --global --add safe.directory /workspace

# Set user.name and user.email from personal git config OR use default
# This is useful when you want to use different git user for personal projects
# and work projects
USER_NAME=$(git config --file /root/.gitconfig-personal --get user.name)
USER_EMAIL=$(git config --file /root/.gitconfig-personal --get user.email)

if [ -n "$USER_NAME" ]; then
    git config --global user.name "$USER_NAME"
else
    echo "User name not found in .gitconfig-personal. Using default."
fi

if [ -n "$USER_EMAIL" ]; then
    git config --global user.email "$USER_EMAIL"
else
    echo "User email not found in .gitconfig-personal. Using default."
fi

# Install pre-commit hooks
pre-commit install