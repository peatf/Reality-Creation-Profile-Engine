from testcontainers.core.container import DockerContainer

class SchemaRegistryContainer(DockerContainer):
    """Custom Testcontainer for Confluent Schema Registry."""
    def __init__(self, image="confluentinc/cp-schema-registry:7.6.0", kafka_bootstrap_servers="PLAINTEXT://kafka:9092"):
        # Using 7.6.0 to match other confluent images in docker-compose.yml
        super().__init__(image=image)
        self.with_exposed_ports(8081)
        # Environment variables for the schema registry container
        self.with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry") # Internal hostname
        self.with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081") # Listen on all interfaces inside container
        # Link to Kafka using the service name defined in docker-compose or test setup
        self.with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafka_bootstrap_servers)

    def get_connection_url(self) -> str:
        """Returns the URL to connect to the Schema Registry from the host."""
        # Use the host and mapped port to connect from the test code running on the host
        host = self.get_container_host_ip()
        port = self.get_exposed_port(8081)
        return f"http://{host}:{port}"

    def get_internal_connection_url(self) -> str:
        """Returns the URL for inter-container communication (e.g., from test service)."""
        # Assumes docker network allows resolution by service name 'schema-registry' on port 8081
        # This might need adjustment based on actual network setup.
        # For tests running inside docker-compose, using service name is typical.
        return "http://schema-registry:8081" # Hardcoding based on typical docker-compose service name