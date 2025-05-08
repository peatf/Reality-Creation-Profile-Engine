# Use a consistent Python base image (adjust version as needed)
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# Default log level, can be overridden by docker-compose environment
ENV LOG_LEVEL="INFO"

# Set work directory
WORKDIR /app/services/kg

# Install system dependencies if any (e.g., for specific drivers)
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# Copy only requirements first to leverage Docker cache
COPY ./services/kg/requirements.txt /app/services/kg/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/services/kg/requirements.txt

# Copy the rest of the application code
# Need to copy shared modules from src/ and config/ as well
COPY ./config /app/config/
COPY ./src /app/src/
COPY ./services/chart_calc/app/schemas /app/services/chart_calc/app/schemas/
COPY ./services/kg /app/services/kg/

# Add the app directory to the Python path
# This allows imports like 'from app.core...' and 'from config...'
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Command to run the application
# This runs the main script which starts the consumers
CMD ["python", "/app/services/kg/main.py"]

# Optional: Add health check script and use HEALTHCHECK instruction later
# COPY ./services/kg/healthcheck.sh /app/healthcheck.sh
# RUN chmod +x /app/healthcheck.sh
# HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD ["/app/healthcheck.sh"]