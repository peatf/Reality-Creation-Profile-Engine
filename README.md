# Reality Creation Profile Engine

This project provides an event-sourced, service-oriented engine for generating user typology profiles based on assessment responses and potentially other data sources like astrological or human design information.

## Architecture Overview

The system is designed as a collection of microservices communicating via events (likely Kafka), with data persisted in a relational database (Postgres/TimescaleDB).

Key services include:
*   **typology_engine:** Calculates assessment scores and typology profiles.
*   **chart_calc:** (Placeholder) Intended for astrological calculations.
*   **chart_calc:** (Placeholder) Intended for astrological calculations. Now supports North Node calculation.
*   **log_writer:** (Placeholder) Handles writing events to the log/database.
*   **insight_service:** (Placeholder) Generates higher-level insights from profile data.

## Quick Start (Local Development)

1.  **Start Infrastructure:**
    ```bash
    docker compose -f infra/docker/docker-compose.yml up -d
    ```
    This will start Postgres/TimescaleDB, Redis, Zookeeper, and Kafka.

2.  **Install Dependencies (Example for Typology Engine):**
    Assuming you have a `requirements-dev.txt` for the Python services:
    ```bash
    # Navigate to the relevant service directory if needed
    # cd services/typology_engine
    pip install -r requirements-dev.txt # Adjust path as necessary
    ```
    *(Note: `requirements-dev.txt` needs to be created)*

3.  **Run Tests (Example for Typology Engine):**
    ```bash
    # Navigate to the root or relevant service directory
    pytest # Adjust command/path based on test setup
    ```
    *(Note: Test setup needs to be configured)*