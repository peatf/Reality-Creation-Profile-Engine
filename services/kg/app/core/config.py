from pydantic_settings import BaseSettings, SettingsConfigDict
import os

# Load .env file if it exists, for local development
# In production, environment variables should be set directly.
if os.path.exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

class Neo4jSettings(BaseSettings):
    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = "password"
    database: str = "neo4j"  # Default Neo4j database

    model_config = SettingsConfigDict(env_prefix='NEO4J_')

class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "kafka:9093"  # Default for within Docker network
    schema_registry_url: str = "http://schema-registry:8081" # Default for within Docker

    model_config = SettingsConfigDict(env_prefix='KAFKA_')

# Instantiate settings
neo4j_settings = Neo4jSettings()
kafka_settings = KafkaSettings()

if __name__ == "__main__":
    # For testing the configuration loading
    print("Neo4j Configuration:")
    print(f"  URI: {neo4j_settings.uri}")
    print(f"  User: {neo4j_settings.user}")
    # Password is intentionally not printed for security
    print(f"  Database: {neo4j_settings.database}")
    print("\nKafka Configuration:")
    print(f"  Bootstrap Servers: {kafka_settings.bootstrap_servers}")
    print(f"  Schema Registry URL: {kafka_settings.schema_registry_url}")
    print("\nTo override Neo4j, set environment variables like NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE.")
    print("To override Kafka, set environment variables like KAFKA_BOOTSTRAP_SERVERS, KAFKA_SCHEMA_REGISTRY_URL.")