#!/bin/sh

# Exit immediately if a command exits with a non-zero status.
set -e

# Run database migrations
echo "Running database migrations..."
alembic upgrade head

# Then exec the container's main process (what's set as CMD in Dockerfile or command in docker-compose)
echo "Migrations complete. Starting application..."
exec "$@"