from typing import Dict, Any
from pydantic import BaseModel

class AssessmentRequest(BaseModel):
    answers: Dict[str, str]  # question_id → selected_answer_key

class AssessmentResult(BaseModel):
    typology_name: str
    confidence: float
    trace: Dict[str, Any]