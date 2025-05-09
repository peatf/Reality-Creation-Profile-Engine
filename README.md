# Reality Creation Profile Engine

This project provides an event-sourced, service-oriented engine for generating user typology profiles based on assessment responses and potentially other data sources like astrological or human design information.

## Architecture Overview

The system is designed as a collection of microservices communicating via events (likely Kafka), with data persisted in a relational database (Postgres/TimescaleDB) and a graph database (Neo4j).

Key services include:
*   **Main API (`main.py`):** Entry point, handles profile creation requests, proxies to other services.
*   **Chart Calculation Service (`services/chart_calc`):** Calculates Astrology and Human Design charts. Uses external Human Design API.
*   **Knowledge Graph Service (`services/kg`):** Consumes events (Chart Calculated, Typology Assessed) and populates a Neo4j graph database.
*   **Typology Engine (`services/typology_engine`):** Calculates assessment scores and typology profiles.
*   **(Core Logic in `src/`):** Contains shared components like auth, caching, DB models, location services, synthesis engine, etc.

## Quick Start (Local Development with Docker)

1.  **Build & Start Services:**
    ```bash
    docker compose up --build -d
    ```
    This will build the images and start the main API, chart_calc service, kg service, Kafka, Neo4j, Redis, and other dependencies defined in `docker-compose.yml`.

2.  **Check Service Health:**
    *   Main API: `http://localhost:8000/health` (primary) or `http://localhost:8000/health/db`
    *   Chart Calc Service: `http://localhost:8001/health` (Note: `docker-compose.yml` for `chart_calc` uses port 9000, main API proxies to 8001. This might need alignment or clarification if `chart_calc` is directly accessed.)
    *   KG Service: (Health check endpoint TBD)
 
## Testing
 
Tests are run using `pytest` within a Docker environment to ensure consistency.
 
1.  **Run All Tests via Docker Compose:**
    This is the recommended way to run the full test suite, as it uses the same environment defined for services.
    ```bash
    docker compose run --rm test
    ```
    The `test` service in `docker-compose.yml` is configured to run `python3 -m pytest`.
 
2.  **Run Specific Tests (inside the `test` container):**
    If you need to run specific tests or with more options, you can get a shell into the `test` container:
    ```bash
    docker compose run --rm test sh
    # Inside the container's /app directory:
    python3 -m pytest services/kg/tests/
    python3 -m pytest tests/cache/ -m "not integration"
    ```
 
3.  **Local Pytest (Alternative, ensure environment matches):**
    If running `pytest` directly on your host (outside Docker), ensure all environment variables listed below are set correctly to point to services (e.g., running Docker containers for Kafka, Redis, Neo4j, Postgres). This is more complex to manage than using the `test` Docker service.
    ```bash
    # Example:
    # export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    # export REDIS_URL=redis://localhost:6379/0
    # ... etc. for all services
    pytest
    ```
 
## Environment Variables

The application and its services rely on several environment variables for configuration. These can be set directly in the environment or via a `.env` file in the project root (loaded by some services for local development).

**Required for Core Functionality:**

*   `HD_API_KEY`: API key for the external HumanDesignAPI.nl service (used by `chart_calc` via `src/human_design/client.py`).
*   `GEO_API_KEY`: Google Geocoding API key (potentially used by HD API client).
*   `JWT_PRIVATE_KEY_PATH`: Path to the JWT private key file. For Docker services (api, test), this is typically `/app/config/keys/dummy_jwt_private_key.pem`.
*   `JWT_PUBLIC_KEY_PATH`: Path to the JWT public key file. For Docker services, typically `/app/config/keys/dummy_jwt_public_key.pem`.
*   `PASSWORD_PEPPER`: A secret string used for password hashing.
*   `DATABASE_URL`: PostgreSQL connection string (e.g., `postgresql+asyncpg://user:pass@host:port/db`). The `api` service in Docker Compose uses `postgresql+asyncpg`.

**JWT Configuration (Defaults provided in `src/auth/jwt.py` if not overridden by env vars):**

*   `JWT_ALGORITHM`: (Default: `RS256`)
*   `JWT_ACCESS_TTL_SECONDS`: (Default: `900`)
*   `JWT_REFRESH_TTL_SECONDS`: (Default: `1209600`)
*   `JWT_ISSUER`: (Default: `rcpe-api`)
*   `JWT_AUDIENCE`: (Default: `rcpe-client`)

**Neo4j Configuration (Defaults provided in `services/kg/app/core/config.py`):**

*   `NEO4J_URI`: (Default: `bolt://localhost:7687` locally, `bolt://neo4j:7687` in Docker)
*   `NEO4J_USER`: (Default: `neo4j`)
*   `NEO4J_PASSWORD`: (Default: `password`)
*   `NEO4J_DATABASE`: (Default: `neo4j`)

**Kafka Configuration (Defaults provided in `services/kg/app/core/config.py`):**

*   `KAFKA_BOOTSTRAP_SERVERS`: (Default: `kafka:9092` in Docker for `services/kg`, `localhost:9092` potentially for local dev. Ensure consistency.)
*   `KAFKA_SCHEMA_REGISTRY_URL`: (Default: `http://schema-registry:8081` in Docker)
*   `KAFKA_CHART_CONSUMER_DLQ_TOPIC`: (Default: `dlq_chart_events`)
*   `KAFKA_TYPOLOGY_CONSUMER_DLQ_TOPIC`: (Default: `dlq_typology_events`)

**Redis Configuration (Defaults likely in `src/cache/connection.py` or via env var `REDIS_URL`):**

*   `REDIS_URL`: (e.g., `redis://localhost:6379/0` locally, `redis://redis:6379/0` in Docker)

## JWT Keys

Dummy JWT keys for development are provided in `config/keys/`.
*   Private Key: `config/keys/dummy_jwt_private_key.pem`
*   Public Key: `config/keys/dummy_jwt_public_key.pem`
 
The `docker-compose.yml` for the `api` and `test` services sets `JWT_PRIVATE_KEY_PATH` and `JWT_PUBLIC_KEY_PATH` to `/app/config/keys/dummy_jwt_private_key.pem` and `/app/config/keys/dummy_jwt_public_key.pem` respectively, using volume mounts to make these files available.
 
## Knowledge Graph Migration Note

The previous monolithic `enginedef.json` has been deprecated. Human Design definitions are now loaded from individual JSON files within the `src/knowledge_graph/HDDefinitions/` directory. The `src/human_design/interpreter.py` module handles loading and structuring this data into the `HD_KNOWLEDGE_BASE`. Synthesis rules and tests should now rely on this structured knowledge base.