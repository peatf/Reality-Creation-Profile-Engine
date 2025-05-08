from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
import logging
from datetime import datetime, timezone

from confluent_kafka import KafkaError

from src.schemas.typology import AssessmentRequest, AssessmentResult
from services.typology_engine.engine import TypologyEngine
from services.typology_engine.models import IncompleteAssessmentError, InvalidSubmissionError
from services.kafka.producer import get_typology_producer
from config.kafka import TOPIC_TYPOLOGY_ASSESSED
# TYPOLOGY_QUESTIONS import removed as TypologyEngine handles its own config loading.

router = APIRouter()
logger = logging.getLogger(__name__)

# Placeholder for user authentication
def get_current_user() -> str:
    # In a real app, this would be part of your authentication system
    return "test_user_id"

# Placeholder for spec loading
class MockSpec:
    version: str = "1.0.0" # Default mock version

def load_spec() -> MockSpec:
    # In a real app, this would load your specification details
    return MockSpec()


# Placeholder for storage service
def get_storage_service():
    # In a real app, this would return an instance of a storage service
    return None

def get_typology_engine():
    # In a real app, this might load the spec from a file or DB
    # For now, assuming TypologyEngine can be instantiated directly
    # and has access to the questions spec if needed for validation.
    # The engine itself is in services/typology_engine/engine.py
    # The spec is in assets/typology_questions.yml
    # TypologyEngine constructor might need the path to the spec.
    # Let's assume it loads it internally or is configured elsewhere.
    return TypologyEngine()

@router.post("/typology/assess", response_model=AssessmentResult)
async def assess_typology(
    request: AssessmentRequest,
    engine: TypologyEngine = Depends(get_typology_engine),
    user_id: str = Depends(get_current_user)
    # storage_service = Depends(get_storage_service) # For future use
):
    """
    Handles assessment submissions, passes data to the TypologyEngine,
    emits a Kafka event, and returns the computed result.
    """
    try:
        # The TypologyEngine is expected to be in services/typology_engine/engine.py
        # and have a method calculate_scores(answers: Dict[str, str])
        # The result_data is a dictionary, e.g., {"typology_name": "Analyst", "confidence": 0.85, ...}
        result_data_dict = engine.calculate_scores(request.answers)
        
        # Convert dict to AssessmentResult for response and easy access
        assessment_result_obj = AssessmentResult(**result_data_dict)

        # Emit Kafka event
        try:
            producer = get_typology_producer()
            if producer:
                event = {
                    "user_id": user_id,
                    "typology_name": assessment_result_obj.typology_name,
                    "confidence": assessment_result_obj.confidence,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "spec_version": load_spec().version
                }
                producer.produce(topic=TOPIC_TYPOLOGY_ASSESSED, value=event)
                producer.flush(timeout=5) # Add a timeout to flush
                logger.info(f"Kafka event emitted for user {user_id}, typology {assessment_result_obj.typology_name}")
            else:
                logger.error("Kafka producer is not available. Event not emitted.")
        except KafkaError as e: # Specific catch for KafkaError
            err_msg = "N/A"
            err_code = "N/A"
            try:
                err_msg = e.str() # Recommended way to get string from KafkaError
                err_code = e.name() # e.g., '_FAIL', or e.code() for the integer
            except Exception as ie:
                err_msg = f"Could not extract info from KafkaError: {str(ie)}"
            logger.error(f"Kafka emission failed (caught as KafkaError). User: {user_id}. Code: {err_code}. Message: {err_msg}")
        except Exception as e: # Generic catch-all
            # Check if it's a KafkaError that wasn't caught by the specific block
            if isinstance(e, KafkaError):
                err_msg = "N/A"
                err_code = "N/A"
                try:
                    err_msg = e.str()
                    err_code = e.name()
                except Exception as ie:
                    err_msg = f"Could not extract info from KafkaError (in generic Exception block): {str(ie)}"
                logger.error(f"An unexpected error occurred (is KafkaError). User: {user_id}. Type: {type(e)}. Code: {err_code}. Message: {err_msg}. Original error: {repr(e)}")
            else:
                logger.error(f"An unexpected error occurred. User: {user_id}. Type: {type(e)}. Error: {str(e)}")


        # Simulate storage
        logger.info(f"Assessment submission processed. Result: {assessment_result_obj}")
        # store_typology_result(user_id="anonymous", result=assessment_result_obj) # Placeholder

        return assessment_result_obj

    except IncompleteAssessmentError as e:
        logger.error(f"Incomplete assessment: {e}")
        raise HTTPException(status_code=422, detail=str(e))
    except InvalidSubmissionError as e:
        logger.error(f"Invalid submission: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error during typology assessment: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Placeholder for the main app to include this router
# In main.py, you would add:
# from src.routers import typology
# app.include_router(typology.router, prefix="/api/v1", tags=["typology"])