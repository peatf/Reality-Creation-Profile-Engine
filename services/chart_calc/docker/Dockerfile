# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies if any (e.g., for certain Python packages)
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Install pip and upgrade
RUN pip install --upgrade pip

# Copy requirements first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY ./app /app/app
# If you have other directories at the root of services/chart_calc to copy, add them here
# e.g. COPY ./config /app/config

# Expose port
EXPOSE 8000

# Healthcheck
# Probes the /health endpoint. Adjust interval, timeout, retries as needed.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import httpx; exit(0) if httpx.get('http://localhost:8000/health').status_code == 200 else exit(1)"
# Note: httpx needs to be in requirements.txt for this healthcheck.

# Command to run the application
# The command should refer to the location of main.py inside the WORKDIR /app
# So, app.main refers to /app/app/main.py
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]