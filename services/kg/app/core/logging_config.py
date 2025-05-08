import logging
import sys
from pythonjsonlogger import jsonlogger

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):
            # Add a timestamp in ISO 8601 format if not already present
            log_record['timestamp'] = record.created
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname

        # Add module and line number for better context
        log_record['module'] = record.module
        log_record['lineno'] = record.lineno
        log_record['pathname'] = record.pathname


def setup_logging(log_level_str: str = "INFO"):
    """
    Configures structured JSON logging for the application.
    """
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove any existing handlers to avoid duplicate logs if this is called multiple times
    # for handler in root_logger.handlers[:]:
    #     root_logger.removeHandler(handler)
        
    # Add a stream handler with JSON formatter for stdout
    # Only add if no similar handler exists or if we want to replace.
    # For simplicity, we assume this is called once at app startup.
    if not any(isinstance(h, logging.StreamHandler) and isinstance(h.formatter, CustomJsonFormatter) for h in root_logger.handlers):
        log_handler = logging.StreamHandler(sys.stdout)
        formatter = CustomJsonFormatter('%(timestamp)s %(level)s %(name)s %(module)s %(lineno)d %(message)s')
        log_handler.setFormatter(formatter)
        root_logger.addHandler(log_handler)
        root_logger.info(f"Structured JSON logging configured with level: {logging.getLevelName(log_level)}")
    else:
        root_logger.info(f"Structured JSON logging already configured. Current level: {logging.getLevelName(root_logger.getEffectiveLevel())}")


if __name__ == '__main__':
    # Example usage:
    # Call this at the beginning of your application
    setup_logging(log_level_str="DEBUG")

    # Test logs from different loggers
    logger1 = logging.getLogger("my_app.module1")
    logger2 = logging.getLogger("my_app.module2")

    logger1.debug("This is a debug message from module1.")
    logger1.info("This is an info message from module1.", extra={"key1": "value1", "user_id": "user123"})
    logger1.warning("This is a warning from module1.")
    
    try:
        x = 1 / 0
    except ZeroDivisionError:
        logger1.error("Error dividing by zero in module1.", exc_info=True)

    logger2.info("Info message from module2.", extra={"service": "kg-service"})
    
    # Test root logger directly
    logging.info("Root logger info message.")