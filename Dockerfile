# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# Set the working directory in the container
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
COPY services/kg/requirements.txt services/kg/requirements.txt
COPY services/chart_calc/requirements.txt services/chart_calc/requirements.txt

# Install build dependencies and curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download ephemeris file
RUN mkdir /app/data && \
    curl -o /app/data/de421.bsp https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/planets/de421.bsp

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r services/kg/requirements.txt && \
    pip install --no-cache-dir -r services/chart_calc/requirements.txt

# Copy the rest of the application code
COPY . .

# Copy entrypoint script and make it executable
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
 
# Expose the port the app runs on
EXPOSE 8000

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
 
# Command to run the application (will be passed to entrypoint.sh)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "warning"]