import logging
# src/knowledge_graph/ontology.py
# Defines the core ontology using RDFLib for the Reality Creation Profile Engine.

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL, XSD

logger = logging.getLogger(__name__) # Initialize logger

# --- Namespace Definitions ---
# Define a custom namespace for our ontology elements
RCPE = Namespace("http://example.com/reality_creation_profile_engine#")

# --- Core Classes ---
# Define the main entities in our domain

# User representing the individual
User = RCPE.User
Person = RCPE.Person # Alias or broader class if needed

# Profile Components
PsychologicalProfile = RCPE.PsychologicalProfile
AstrologicalChart = RCPE.AstrologicalChart
HumanDesignChart = RCPE.HumanDesignChart

# Assessment Specifics
AssessmentTypology = RCPE.AssessmentTypology # The overall typology pair (e.g., Structured-Fluid)
AssessmentSpectrum = RCPE.AssessmentSpectrum # Represents one of the 6 spectrums
SpectrumPlacement = RCPE.SpectrumPlacement # Represents the user's placement (e.g., Balanced, Fluid)
MasteryValue = RCPE.MasteryValue # Represents a dominant value from Part 2 (e.g., creative-expression)

# Astrology Specifics
AstrologicalObject = RCPE.AstrologicalObject # Planets, Nodes, Angles
Planet = RCPE.Planet
Asteroid = RCPE.Asteroid # If needed later
Node = RCPE.Node # Lunar Nodes
Angle = RCPE.Angle # Asc, MC, etc.
ZodiacSign = RCPE.ZodiacSign
AstrologicalHouse = RCPE.AstrologicalHouse
Aspect = RCPE.Aspect # Astrological aspect

# Human Design Specifics
HDType = RCPE.HDType
HDAuthority = RCPE.HDAuthority
HDProfile = RCPE.HDProfile
HDCenter = RCPE.HDCenter
HDGate = RCPE.HDGate
HDChannel = RCPE.HDChannel
HDVariable = RCPE.HDVariable # Represents one of the 4 arrows/transformations
HDColor = RCPE.HDColor
HDTone = RCPE.HDTone
HDBase = RCPE.HDBase

# Synthesis Concepts
ManifestationDimension = RCPE.ManifestationDimension # Willpower, Magnetism, etc.
Insight = RCPE.Insight # A generated piece of text insight
Recommendation = RCPE.Recommendation # A generated recommendation
ManifestationPotential = RCPE.ManifestationPotential # A specific potential derived from factors
ManifestationChallenge = RCPE.ManifestationChallenge # A specific challenge derived from factors

# --- Properties ---
# Define relationships and attributes

# User Relationships
hasProfile = RCPE.hasProfile # User -> PsychologicalProfile
hasAstrologyChart = RCPE.hasAstrologyChart # User -> AstrologicalChart
hasHumanDesignChart = RCPE.hasHumanDesignChart # User -> HumanDesignChart

# Psychological Profile Properties
hasTypology = RCPE.hasTypology # PsychologicalProfile -> AssessmentTypology
hasSpectrumPlacement = RCPE.hasSpectrumPlacement # PsychologicalProfile -> SpectrumPlacement
hasDominantMasteryValue = RCPE.hasDominantMasteryValue # PsychologicalProfile -> MasteryValue
hasEnergyFocus = RCPE.hasEnergyFocus # PsychologicalProfile -> Literal (e.g., "Expansion: 60%")

# Spectrum Placement Properties
onSpectrum = RCPE.onSpectrum # SpectrumPlacement -> AssessmentSpectrum
hasPlacementValue = RCPE.hasPlacementValue # SpectrumPlacement -> Literal (e.g., "Balanced", "Fluid")

# Assessment Typology Properties
hasPrimaryComponent = RCPE.hasPrimaryComponent # AssessmentTypology -> SpectrumPlacement
hasSecondaryComponent = RCPE.hasSecondaryComponent # AssessmentTypology -> SpectrumPlacement
typologyName = RCPE.typologyName # AssessmentTypology -> Literal

# Astrological Chart Properties
hasObject = RCPE.hasObject # AstrologicalChart -> AstrologicalObject
hasHouseCusp = RCPE.hasHouseCusp # AstrologicalChart -> AstrologicalHouse
hasAngle = RCPE.hasAngle # AstrologicalChart -> Angle
hasAspect = RCPE.hasAspect # AstrologicalChart -> Aspect

# Astrological Object/Angle/House Properties
isInSign = RCPE.isInSign # AstrologicalObject/Angle/House -> ZodiacSign
isInHouse = RCPE.isInHouse # AstrologicalObject -> AstrologicalHouse
longitude = RCPE.longitude # -> Literal (XSD.float)
signLongitude = RCPE.signLongitude # -> Literal (XSD.float)
latitude = RCPE.latitude # -> Literal (XSD.float)
speed = RCPE.speed # -> Literal (XSD.float)
houseNumber = RCPE.houseNumber # -> Literal (XSD.integer)
signName = RCPE.signName # -> Literal

# Aspect Properties
aspectType = RCPE.aspectType # Aspect -> Literal (e.g., "Square")
involvesObject = RCPE.involvesObject # Aspect -> AstrologicalObject (use twice)
orb = RCPE.orb # Aspect -> Literal (XSD.float)
isApplying = RCPE.isApplying # Aspect -> Literal (XSD.boolean)

# Human Design Chart Properties
hasHDType = RCPE.hasHDType # HumanDesignChart -> HDType
hasHDAuthority = RCPE.hasHDAuthority # HumanDesignChart -> HDAuthority
hasHDProfile = RCPE.hasHDProfile # HumanDesignChart -> HDProfile
hasHDCenter = RCPE.hasHDCenter # HumanDesignChart -> HDCenter
hasHDGate = RCPE.hasHDGate # HumanDesignChart -> HDGate
hasHDChannel = RCPE.hasHDChannel # HumanDesignChart -> HDChannel
hasHDVariable = RCPE.hasHDVariable # HumanDesignChart -> HDVariable

# Human Design Component Properties
centerName = RCPE.centerName # HDCenter -> Literal
isDefined = RCPE.isDefined # HDCenter -> Literal (XSD.boolean)
gateNumber = RCPE.gateNumber # HDGate -> Literal (XSD.integer)
isActive = RCPE.isActive # HDGate -> Literal (XSD.boolean)
channelName = RCPE.channelName # HDChannel -> Literal
connectsCenter = RCPE.connectsCenter # HDChannel -> HDCenter (use twice)
variableName = RCPE.variableName # HDVariable -> Literal (e.g., "Determination")
variableOrientation = RCPE.variableOrientation # HDVariable -> Literal ("Left" or "Right")
hasColor = RCPE.hasColor # HDVariable/HDGate -> HDColor
hasTone = RCPE.hasTone   # HDColor -> HDTone
hasBase = RCPE.hasBase   # HDTone -> HDBase

# Synthesis Properties
influencesDimension = RCPE.influencesDimension # Astro/HD/Psych Component -> ManifestationDimension
dimensionName = RCPE.dimensionName # ManifestationDimension -> Literal
derivedFrom = RCPE.derivedFrom # Insight/Recommendation -> Various Profile Components
hasPotential = RCPE.hasPotential # User/Profile -> ManifestationPotential
hasChallenge = RCPE.hasChallenge # User/Profile -> ManifestationChallenge
insightText = RCPE.insightText # Insight -> Literal
recommendationText = RCPE.recommendationText # Recommendation -> Literal

# General Properties
name = RCPE.name # Generic name property
description = RCPE.description # Generic description property
value = RCPE.value # Generic value property

# --- Ontology Initialization Function ---

def initialize_ontology_graph() -> Graph:
    """
    Initializes an RDFLib Graph with basic ontology definitions (classes and properties).
    """
    g = Graph()
    g.bind("rcpe", RCPE)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("owl", OWL)
    g.bind("xsd", XSD)

    # Define Classes (as RDFS.Class or OWL.Class)
    classes = [
        User, Person, PsychologicalProfile, AstrologicalChart, HumanDesignChart,
        AssessmentTypology, AssessmentSpectrum, SpectrumPlacement, MasteryValue,
        AstrologicalObject, Planet, Node, Angle, ZodiacSign, AstrologicalHouse, Aspect,
        HDType, HDAuthority, HDProfile, HDCenter, HDGate, HDChannel, HDVariable,
        HDColor, HDTone, HDBase,
        ManifestationDimension, Insight, Recommendation, ManifestationPotential, ManifestationChallenge
    ]
    for cls in classes:
        g.add((cls, RDF.type, RDFS.Class))
        g.add((cls, RDF.type, OWL.Class)) # Can declare as OWL Class too

    # Define Properties (as RDF.Property or specific OWL properties)
    properties = [
        hasProfile, hasAstrologyChart, hasHumanDesignChart, hasTypology,
        hasSpectrumPlacement, hasDominantMasteryValue, hasEnergyFocus, onSpectrum,
        hasPlacementValue, hasPrimaryComponent, hasSecondaryComponent, typologyName,
        hasObject, hasHouseCusp, hasAngle, hasAspect, isInSign, isInHouse,
        longitude, signLongitude, latitude, speed, houseNumber, signName,
        aspectType, involvesObject, orb, isApplying, hasHDType, hasHDAuthority,
        hasHDProfile, hasHDCenter, hasHDGate, hasHDChannel, hasHDVariable,
        centerName, isDefined, gateNumber, isActive, channelName, connectsCenter,
        variableName, variableOrientation, hasColor, hasTone, hasBase,
        influencesDimension, dimensionName, derivedFrom, hasPotential, hasChallenge,
        insightText, recommendationText, name, description, value
    ]
    for prop in properties:
        g.add((prop, RDF.type, RDF.Property))
        # Optionally define as OWL.ObjectProperty or OWL.DatatypeProperty
        # Example: g.add((hasProfile, RDF.type, OWL.ObjectProperty))
        # Example: g.add((longitude, RDF.type, OWL.DatatypeProperty))
        # Example: g.add((longitude, RDFS.range, XSD.float))

    # Add some basic labels for clarity (optional)
    g.add((User, RDFS.label, Literal("User")))
    g.add((AstrologicalChart, RDFS.label, Literal("Astrological Chart")))
    g.add((HumanDesignChart, RDFS.label, Literal("Human Design Chart")))
    g.add((PsychologicalProfile, RDFS.label, Literal("Psychological Profile")))
    g.add((ManifestationDimension, RDFS.label, Literal("Manifestation Dimension")))
    g.add((influencesDimension, RDFS.label, Literal("influences dimension")))

    logger.info("Ontology graph initialized with basic definitions.")
    return g

# Example Usage
if __name__ == "__main__":
    ontology_graph = initialize_ontology_graph()
    print(f"Initialized ontology graph with {len(ontology_graph)} triples.")
    # You can serialize the basic ontology structure if needed
    # print(ontology_graph.serialize(format="turtle"))