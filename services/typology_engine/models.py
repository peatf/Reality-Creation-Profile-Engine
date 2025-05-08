from pydantic import BaseModel, Field
from typing import List, Dict, Any, Literal

class MetaConfig(BaseModel):
    minimum_items_per_category: int
    cronbach_alpha_threshold: float

class SpectrumAnswerDetail(BaseModel):
    label: str
    value: Literal[-1, 0, 1]

class SpectrumAnswers(BaseModel):
    left: SpectrumAnswerDetail
    balanced: SpectrumAnswerDetail
    right: SpectrumAnswerDetail

class SpectrumQuestion(BaseModel):
    id: str
    text: str
    answers: SpectrumAnswers

class Spectrum(BaseModel):
    id: str
    name: str
    description: str
    left_label: str
    right_label: str
    questions: List[SpectrumQuestion]

class MasteryAnswer(BaseModel):
    id: str
    label: str
    value: str # String value used for influence mapping

class MasteryQuestion(BaseModel):
    id: str
    text: str
    answers: List[MasteryAnswer]

class MasteryDimension(BaseModel):
    id: str
    name: str
    description: str
    questions: List[MasteryQuestion]

class ScoringRules(BaseModel):
    spectrum_priority_order: List[str]
    placement_mapping: Dict[str, str]
    mastery_spectrum_influence: Dict[str, Dict[str, Dict[str, float]]] # {dimension_id: {answer_value: {spectrum_id: influence_value}}}

class TypologyProfileTips(BaseModel):
    ideal_approaches: Dict[str, Any] # Can contain strings or lists
    common_misalignments: List[str]
    shifts_needed: List[str]
    acceptance_permissions: List[str]
    energy_support_tools: List[str]

class TypologyProfileDescription(BaseModel):
    essence: str
    strength: str
    challenge: str
    approach: str
    growth: str

class TypologyProfile(BaseModel):
    id: str
    typology_name: str
    headline: str
    description: TypologyProfileDescription
    tips: TypologyProfileTips

class ResultsConfig(BaseModel):
    typology_profiles: List[TypologyProfile]

class TypologyConfig(BaseModel):
    version: str
    released_at: str # Could be date, but string is safer for parsing
    meta: MetaConfig
    spectrums: List[Spectrum]
    mastery_dimensions: List[MasteryDimension] = Field(..., alias='mastery_dimensions')
    scoring_rules: ScoringRules = Field(..., alias='scoring_rules')
    results: ResultsConfig
# Custom Error Classes
class IncompleteAssessmentError(ValueError):
    """Custom exception for incomplete assessment submissions."""
    pass

class InvalidSubmissionError(ValueError):
    """Custom exception for invalid submission data (e.g., bad answer keys)."""
    pass