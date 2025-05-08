from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import KafkaError
import logging
from config.kafka import KAFKA_BOOTSTRAP_SERVERS, load_avro_schema # Assuming load_avro_schema is in config.kafka

# Configure logger
logger = logging.getLogger(__name__)

# Attempt to load the Avro schema once when the module is loaded.
# This makes it available for the producer factory.
try:
    # Path relative to the project root, adjust if necessary
    TYPOLOGY_ASSESSED_SCHEMA_PATH = "schemas/kafka/typology_assessed.avsc"
    typology_assessed_value_schema_str = load_avro_schema(TYPOLOGY_ASSESSED_SCHEMA_PATH)
except FileNotFoundError:
    logger.error(f"Avro schema file not found at {TYPOLOGY_ASSESSED_SCHEMA_PATH}. Kafka producer may not work correctly.")
    typology_assessed_value_schema_str = None # Or a default minimal schema / raise error
except Exception as e:
    logger.error(f"Error loading Avro schema: {e}")
    typology_assessed_value_schema_str = None


def get_typology_producer() -> AvroProducer:
    if not typology_assessed_value_schema_str:
        logger.error("Typology assessed Avro schema not loaded. Cannot create AvroProducer.")
        # Depending on desired behavior, could raise an exception or return None/mock.
        # For now, let's allow it to proceed and potentially fail at AvroProducer instantiation
        # if the schema string is truly required by it directly and is None.
        # However, AvroProducer can also take a file path.
        pass # Allow it to try with the path if schema string is None

    producer_config = {
        "bootstrap.servers": ",".join(KAFKA_BOOTSTRAP_SERVERS),
        "schema.registry.url": "http://schema-registry:8081" # This should be configurable
    }
    # The AvroProducer can take the schema string directly or a file path.
    # If typology_assessed_value_schema_str is None due to load failure,
    # we can pass the path, and it might try to load it itself.
    # Or, ensure load_avro_schema returns a valid schema string or raises.
    try:
        # Prefer passing the schema string if loaded successfully
        if typology_assessed_value_schema_str:
            return AvroProducer(producer_config, default_value_schema=typology_assessed_value_schema_str)
        else:
            # Fallback to path if schema string loading failed, though this might also fail
            # if the path was the issue in the first place.
            # This assumes AvroProducer can take a file path for default_value_schema.
            # According to confluent-kafka docs, it expects a schema string or Schema object.
            # So, if typology_assessed_value_schema_str is None, this will likely fail.
            # A robust solution would ensure schema is loaded or handle failure gracefully.
            logger.warning(f"Attempting to create AvroProducer with schema path due to load failure: {TYPOLOGY_ASSESSED_SCHEMA_PATH}")
            # This line will likely cause an error if typology_assessed_value_schema_str is None
            # and AvroProducer strictly needs a schema string.
            # For a robust app, ensure schema is loaded or raise an error earlier.
            return AvroProducer(producer_config, default_value_schema=load_avro_schema(TYPOLOGY_ASSESSED_SCHEMA_PATH))

    except SerializerError as e:
        logger.error(f"Error creating AvroProducer with schema: {e}. Ensure schema registry is accessible and schema is valid.")
        raise  # Re-raise the exception to indicate failure to create producer
    except KafkaError as e:
        logger.error(f"Kafka-related error creating AvroProducer: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while creating AvroProducer: {e}")
        raise