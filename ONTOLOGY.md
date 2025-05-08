# Reality Creation Profile Engine - Ontology

This document outlines the core ontology used in the Reality Creation Profile Engine, defined using RDFLib. The ontology structures the data extracted from psychological assessments, astrological charts, and Human Design charts, enabling synthesis and insight generation.

**Namespace:** `http://example.com/reality_creation_profile_engine#` (Prefix: `rcpe`)

## Core Classes

These represent the main entities within the user's profile.

*   **`rcpe:User`**: Represents the individual user of the system.
*   **`rcpe:PsychologicalProfile`**: Holds data derived from the Reality Creation Assessment.
*   **`rcpe:AstrologicalChart`**: Holds data derived from the user's astrological birth chart.
*   **`rcpe:HumanDesignChart`**: Holds data derived from the user's Human Design chart.

### Assessment Classes

*   **`rcpe:AssessmentTypology`**: The user's overall assessment typology pair (e.g., Structured-Fluid).
*   **`rcpe:AssessmentSpectrum`**: One of the six core assessment spectrums (e.g., Cognitive Alignment).
*   **`rcpe:SpectrumPlacement`**: The user's placement on a specific spectrum (e.g., Balanced, Fluid).
*   **`rcpe:MasteryValue`**: A dominant theme or value identified from the mastery assessment (e.g., creative-expression).

### Astrology Classes

*   **`rcpe:AstrologicalObject`**: A general class for celestial bodies and points (Planets, Nodes, Angles).
*   **`rcpe:Planet`**: Specific planets (Sun, Moon, Mercury, etc.).
*   **`rcpe:Node`**: Lunar nodes (North Node, South Node).
*   **`rcpe:Angle`**: Chart angles (Ascendant, Midheaven).
*   **`rcpe:ZodiacSign`**: The 12 zodiac signs.
*   **`rcpe:AstrologicalHouse`**: The 12 astrological houses.
*   **`rcpe:Aspect`**: An angular relationship between two AstrologicalObjects.

### Human Design Classes

*   **`rcpe:HDType`**: The user's HD Type (Generator, Projector, etc.).
*   **`rcpe:HDStrategy`**: The user's HD Strategy (Informing, Responding, etc.).
*   **`rcpe:HDAuthority`**: The user's inner authority (Sacral, Emotional, etc.).
*   **`rcpe:HDProfile`**: The user's HD Profile (e.g., 1/3, 5/1).
*   **`rcpe:HDCenter`**: One of the 9 HD centers.
*   **`rcpe:HDGate`**: One of the 64 HD gates.
*   **`rcpe:HDChannel`**: One of the 36 HD channels.
*   **`rcpe:HDVariable`**: Represents concepts like Determination, Cognition, Environment, and the broader categories of Motivation and Perspective, which have specific term-based entries.
*   **`rcpe:HDColor`**, **`rcpe:HDTone`**, **`rcpe:HDBase`**: Sub-levels of Variable/Gate information.

### Human Design Term Definitions

The `enginedef.json` file under `src/knowledge_graph/` contains detailed definitions for various Human Design terms. These terms are loaded into the `HD_KNOWLEDGE_BASE` by the `src/human_design/interpreter.py`.

**JSON Schema Shape for Terms:**

Each term object generally follows this structure:

```json
{
  "term_name": "string (Unique name of the term)",
  "definition": "string (Detailed explanation of the term)",
  "role_in_manifestation": "string (How this term relates to reality creation/manifestation)",
  "weighted_importance": "string|number (e.g., 'critical', 7 - indicates relevance)",
  "cross_references": ["string" (List of related terms or concepts)],
  "details": "object|null (Further structured details if applicable)"
}
```

**Categories and `term_name` Values:**

#### Types
*   Category: `KnowledgeBaseKeys.TYPES`
*   Terms:
    *   `Manifestor`
    *   `Generator`
    *   `Manifesting Generator`
    *   `Projector`
    *   `Reflector`

#### Strategies
*   Category: `KnowledgeBaseKeys.STRATEGIES`
*   Terms:
    *   `Informing`
    *   `Responding`
    *   `Waiting for the Invitation`
    *   `Waiting a Lunar Cycle`

#### Authorities
*   Category: `KnowledgeBaseKeys.AUTHORITIES` (Note: `interpreter.py` maps these to simpler keys like "Emotional", "Sacral")
*   Terms:
    *   `Emotional Authority`
    *   `Sacral Authority`
    *   `Splenic Authority`
    *   `Ego Authority`
    *   `Self-Projected Authority`
    *   `Environmental Authority`
    *   `Lunar Authority`

#### Motivation (Variables)
*   Category: `KnowledgeBaseKeys.VARIABLES` (subkey: `KnowledgeBaseKeys.MOTIVATION`)
*   Terms:
    *   `Fear – Communalist (Left / Strategic)`
    *   `Fear – Separatist (Right / Receptive)`
    *   `Hope – Theist (Left / Strategic)`
    *   `Hope – Antitheist (Right / Receptive)`
    *   `Desire – Leader (Left / Strategic)`
    *   `Desire – Follower (Right / Receptive)`
    *   `Need – Master (Left / Strategic)`
    *   `Need – Novice (Right / Receptive)`
    *   `Guilt – Conditioner (Left / Strategic)`
    *   `Guilt – Conditioned (Right / Receptive)`
    *   `Innocence – Observer (Left / Strategic)`
    *   `Innocence – Observed (Right / Receptive)`

#### Perspective (Variables)
*   Category: `KnowledgeBaseKeys.VARIABLES` (subkey: `KnowledgeBaseKeys.PERSPECTIVE`)
*   Terms:
    *   `Survival (Left – Focused)`
    *   `Survival (Right – Peripheral)`
    *   `Possibility (Left – Focused)`
    *   `Possibility (Right – Peripheral)`
    *   `Perspective (Left – Focused)`
    *   `Perspective (Right – Peripheral)`
    *   `Understanding (Left – Focused)`
    *   `Understanding (Right – Peripheral)`
    *   `Evaluation (Left – Focused)`
    *   `Evaluation (Right – Peripheral)`
    *   `Judgment (Left – Focused)`
    *   `Judgment (Right – Peripheral)`

### Synthesis Classes

*   **`rcpe:ManifestationDimension`**: A conceptual dimension of manifestation (e.g., Willpower, Magnetism).
*   **`rcpe:Insight`**: A piece of synthesized textual insight generated by the engine.
*   **`rcpe:Recommendation`**: A suggested action or focus area based on the profile.
*   **`rcpe:ManifestationPotential`**: A specific potential identified in the profile.
*   **`rcpe:ManifestationChallenge`**: A specific challenge identified in the profile.

## Core Properties

These define the relationships and attributes of the classes.

### User Relationships

*   **`rcpe:hasProfile`**: `User` -> `PsychologicalProfile`
*   **`rcpe:hasAstrologyChart`**: `User` -> `AstrologicalChart`
*   **`rcpe:hasHumanDesignChart`**: `User` -> `HumanDesignChart`

### Psychological Profile Properties

*   **`rcpe:hasTypology`**: `PsychologicalProfile` -> `AssessmentTypology`
*   **`rcpe:hasSpectrumPlacement`**: `PsychologicalProfile` -> `SpectrumPlacement`
*   **`rcpe:hasDominantMasteryValue`**: `PsychologicalProfile` -> `MasteryValue`
*   **`rcpe:hasEnergyFocus`**: `PsychologicalProfile` -> `Literal` (e.g., "Expansion: 60%")

### Assessment Component Properties

*   **`rcpe:onSpectrum`**: `SpectrumPlacement` -> `AssessmentSpectrum`
*   **`rcpe:hasPlacementValue`**: `SpectrumPlacement` -> `Literal` (e.g., "Balanced")
*   **`rcpe:hasPrimaryComponent`**: `AssessmentTypology` -> `SpectrumPlacement`
*   **`rcpe:hasSecondaryComponent`**: `AssessmentTypology` -> `SpectrumPlacement`
*   **`rcpe:typologyName`**: `AssessmentTypology` -> `Literal`

### Astrological Chart Properties

*   **`rcpe:hasObject`**: `AstrologicalChart` -> `AstrologicalObject`
*   **`rcpe:hasHouseCusp`**: `AstrologicalChart` -> `AstrologicalHouse`
*   **`rcpe:hasAngle`**: `AstrologicalChart` -> `Angle`
*   **`rcpe:hasAspect`**: `AstrologicalChart` -> `Aspect`

### Astrology Component Properties

*   **`rcpe:isInSign`**: `AstrologicalObject`/`Angle`/`House` -> `ZodiacSign` (or Literal)
*   **`rcpe:isInHouse`**: `AstrologicalObject` -> `AstrologicalHouse`
*   **`rcpe:longitude`**: `AstrologicalObject`/`Angle`/`House` -> `Literal` (xsd:float)
*   **`rcpe:signLongitude`**: `AstrologicalObject`/`Angle`/`House` -> `Literal` (xsd:float)
*   **`rcpe:latitude`**: `AstrologicalObject` -> `Literal` (xsd:float)
*   **`rcpe:speed`**: `AstrologicalObject` -> `Literal` (xsd:float)
*   **`rcpe:houseNumber`**: `AstrologicalHouse` -> `Literal` (xsd:integer)
*   **`rcpe:signName`**: `ZodiacSign` -> `Literal`
*   **`rcpe:aspectType`**: `Aspect` -> `Literal` (e.g., "Square")
*   **`rcpe:involvesObject`**: `Aspect` -> `AstrologicalObject` (used twice per aspect)
*   **`rcpe:orb`**: `Aspect` -> `Literal` (xsd:float)
*   **`rcpe:isApplying`**: `Aspect` -> `Literal` (xsd:boolean)

### Human Design Chart Properties

*   **`rcpe:hasHDType`**: `HumanDesignChart` -> `HDType`
*   **`rcpe:hasHDStrategy`**: `HumanDesignChart` -> `HDStrategy`
*   **`rcpe:hasHDAuthority`**: `HumanDesignChart` -> `HDAuthority`
*   **`rcpe:hasHDProfile`**: `HumanDesignChart` -> `HDProfile`
*   **`rcpe:hasHDCenter`**: `HumanDesignChart` -> `HDCenter`
*   **`rcpe:hasHDGate`**: `HumanDesignChart` -> `HDGate`
*   **`rcpe:hasHDChannel`**: `HumanDesignChart` -> `HDChannel`
*   **`rcpe:hasHDVariable`**: `HumanDesignChart` -> `HDVariable`

### Human Design Component Properties

*   **`rcpe:centerName`**: `HDCenter` -> `Literal`
*   **`rcpe:isDefined`**: `HDCenter` -> `Literal` (xsd:boolean)
*   **`rcpe:gateNumber`**: `HDGate` -> `Literal` (xsd:integer)
*   **`rcpe:isActive`**: `HDGate` -> `Literal` (xsd:boolean)
*   **`rcpe:channelName`**: `HDChannel` -> `Literal`
*   **`rcpe:connectsCenter`**: `HDChannel` -> `HDCenter` (used twice per channel)
*   **`rcpe:variableName`**: `HDVariable` -> `Literal` (e.g., "Determination")
*   **`rcpe:variableOrientation`**: `HDVariable` -> `Literal` ("Left" or "Right")
*   **`rcpe:hasColor`**: `HDVariable`/`HDGate` -> `HDColor`
*   **`rcpe:hasTone`**: `HDColor` -> `HDTone`
*   **`rcpe:hasBase`**: `HDTone` -> `HDBase`

### Synthesis Properties

*   **`rcpe:influencesDimension`**: `[Profile Component]` -> `ManifestationDimension`
*   **`rcpe:dimensionName`**: `ManifestationDimension` -> `Literal`
*   **`rcpe:derivedFrom`**: `Insight`/`Recommendation` -> `[Profile Component]` (multiple)
*   **`rcpe:hasPotential`**: `User`/`Profile` -> `ManifestationPotential`
*   **`rcpe:hasChallenge`**: `User`/`Profile` -> `ManifestationChallenge`
*   **`rcpe:insightText`**: `Insight` -> `Literal`
*   **`rcpe:recommendationText`**: `Recommendation` -> `Literal`

### General Properties

*   **`rcpe:name`**: Generic name.
*   **`rcpe:description`**: Generic description.
*   **`rcpe:value`**: Generic value.