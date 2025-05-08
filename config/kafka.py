KAFKA_BOOTSTRAP_SERVERS = ["kafka01:9092", "kafka02:9092"]
SCHEMA_REGISTRY_URL = "http://schema-registry:8081" # Added Schema Registry URL
TOPIC_TYPOLOGY_ASSESSED = "TYPOLOGY_ASSESSED"
# Add other topics if needed, e.g.:
# TOPIC_CHART_CALCULATED = "CHART_CALCULATED"

def load_avro_schema(path: str):
    # This is a placeholder. In a real scenario, you'd load and parse the Avro schema.
    # For confluent_kafka, the AvroProducer handles schema loading from file path directly
    # or from a schema string. For simplicity, we'll assume the producer can take the path.
    # If using a schema registry, this function might not be needed here,
    # as the producer would fetch it.
    # For now, returning the path itself as the producer might handle it.
    # A more robust implementation would read the file and return its content as a string
    # or a parsed schema object if the library requires it.
    with open(path, "r") as f:
        return f.read()