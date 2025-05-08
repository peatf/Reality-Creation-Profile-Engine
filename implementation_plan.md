# Reality Creation Profile Engine: Implementation Plan

This plan outlines the key phases and steps involved in building the Reality Creation Profile Engine, incorporating the requirements for FastAPI, Railway deployment, specific libraries, and the integration of Astrology, Human Design, and the existing assessment logic.

---

## Phase 1: Project Setup & Foundational Architecture (Weeks 1-2)

1.  **Initialize Project:**
    *   Set up a Python project structure (e.g., using Poetry or pipenv for dependency management).
    *   Initialize FastAPI application.
    *   Set up Git repository.
2.  **Dependency Selection & Setup:**
    *   **Astrology:** Choose and integrate either `Flatlib` or `PySwisseph`. Evaluate based on ease of use, performance, and ephemeris handling.
    *   **Human Design:** Implement a client to proxy requests to `HumanDesignAPI.nl`. Define the data structure expected from this API.
    *   **Web Framework:** FastAPI (as specified).
    *   **Data Validation:** Pydantic (integrates well with FastAPI).
    *   **Database/Knowledge Representation:** Plan for a Knowledge Graph (KG) approach as recommended in the architecture document. Initially, this might be implemented using graph libraries (like `networkx` for prototyping or `RDFLib` for semantic structure) or a dedicated graph database suitable for Railway (e.g., ArangoDB via service, or potentially simpler storage initially like JSON files or PostgreSQL with JSONB, migrating later).
3.  **Basic FastAPI Structure:**
    *   Set up basic API endpoints for health checks and initial testing.
    *   Configure basic logging and error handling.
4.  **Dockerization Setup:**
    *   Create initial `Dockerfile` for the FastAPI application.
    *   **Ephemeris Handling:** Add steps to the `Dockerfile` to download the required ephemeris files (Swiss Ephemeris data files) during the build process and configure the application to access them from the specified path (`/opt/ephemeris`). *Note: Ensure Railway's build environment supports downloading ~500MB during build.*
    *   Set up `docker-compose.yml` for local development environment.

---

## Phase 2: Data Modeling & Core Logic Implementation (Weeks 3-6)

5.  **Unified Knowledge Model/Ontology Design:**
    *   Based on the architecture documents, design the core ontology/schema to represent concepts from Astrology, Human Design, and the Assessment. Define entities (User, AstroChart, HDChart, PsychProfile, Insight, etc.), attributes, and key relationships (e.g., `influences`, `correlatesWith`, `determinedBy`).
    *   Focus on mapping concepts related to the 5 manifestation dimensions (Willpower, Magnetism, Coherence, Imagination, Embodiment) across the three systems.
    *   Represent this schema using chosen KG approach (e.g., RDFLib classes/predicates, graph database schema).
6.  **Astrology Module Implementation:**
    *   Implement functions to calculate astrological charts based on birth data using the chosen library (Flatlib/PySwisseph).
    *   Parse the JSON schema from `Astrological Manifestation Analyst Construction.md` into Python data structures or load it into the KG.
    *   Develop logic to extract relevant astrological factors (planetary positions, aspects, house placements) related to the manifestation dimensions.
7.  **Human Design Module Implementation:**
    *   Implement the proxy client for `HumanDesignAPI.nl`.
    *   Define functions to fetch and parse HD chart data (Type, Authority, Profile, Centers, Gates, Channels).
    *   Integrate logic based on `Human Design System...txt` and `Human Design Variables...txt` to interpret the fetched data, focusing on manifestation mechanics and Variable concepts relevant to the synthesis.
8.  **Assessment Logic Porting & Module:**
    *   Translate the JavaScript assessment logic (`redesigned-assessment.js`, `redesigned-scoring.js`, `redesigned-part2-mastery.js`, `redesigned-results.js`, `cross-spectrum-insights.js`) into Python.
    *   Implement functions to:
        *   Receive assessment responses.
        *   Calculate typology scores, placements, and the typology pair (including mastery influence).
        *   Calculate energy focus scores.
        *   Apply cross-spectrum insight rules.
9.  **Data Ingestion & KG Population:**
    *   Develop services/functions to take user input (birth data, assessment responses) and populate the KG with the corresponding AstroChart, HDChart, and PsychProfile data according to the defined ontology.

---

## Phase 3: Synthesis Engine & API Development (Weeks 7-9)

10. **Synthesis Rule Engine:**
    *   Implement the core synthesis logic based on the integration strategies outlined in the architecture documents.
    *   Start with mapping direct correlations identified between the three systems (e.g., mapping assessment spectrum scores to relevant Astro/HD factors).
    *   Develop rules (potentially using a rule engine or custom logic querying the KG) to generate synthesized insights based on combinations of factors across the different profiles.
    *   Incorporate the cross-spectrum insight rules from `cross-spectrum-insights.js`.
11. **API Endpoint Development:**
    *   Create FastAPI endpoints:
        *   `/profile/create`: Accepts user birth data and assessment responses. Triggers calculation, KG population, and synthesis. Returns a profile ID.
        *   `/profile/{profile_id}`: Retrieves the synthesized profile insights (potentially including Astro data, HD data, Assessment results, and synthesized interpretations).
    *   Ensure endpoints return JSON, with interpretations potentially embedding Markdown as specified.
    *   Implement request/response validation using Pydantic models.
12. **Narrative Generation (Initial):**
    *   Implement initial narrative generation, possibly using template-based approaches combined with the structured insights from the synthesis engine. Retrieve relevant descriptions from `redesigned-results.js` (ported to Python) and the Astro/HD research files.

---

## Phase 4: Testing, Deployment & Iteration (Weeks 10-12)

13. **Testing:**
    *   **Unit Tests:** Write unit tests for individual modules (Astrology calcs, HD logic, Assessment scoring, Synthesis rules).
    *   **Integration Tests:** Test the interaction between modules (e.g., data flow from input to KG to synthesis engine).
    *   **API Tests:** Test the FastAPI endpoints for correctness, error handling, and performance.
    *   **Validation:** Perform initial validation of synthesized insights against expert knowledge or test cases.
14. **Railway Deployment Setup:**
    *   Configure `railway.json` or use Railway's dashboard settings for deployment.
    *   Ensure Dockerfile builds correctly on Railway and ephemeris files are accessible.
    *   Set up environment variables (e.g., API keys for HumanDesignAPI.nl if needed).
15. **Deployment & Monitoring:**
    *   Deploy the application to Railway's free tier.
    *   Set up basic monitoring and logging on Railway.
16. **Documentation:**
    *   Document the API endpoints (FastAPI can auto-generate Swagger/OpenAPI docs).
    *   Document the project structure, setup, and deployment process.
    *   Document the core ontology/schema design.
17. **Review & Iteration Planning:**
    *   Review the initial deployed version.
    *   Plan for future iterations based on feedback and potential enhancements (e.g., more sophisticated narrative generation using LLMs with RAG, deeper KG reasoning, incorporating user goals).

---

## Architectural Overview (Conceptual)

```mermaid
graph TD
    subgraph User Input
        UI(Frontend/Client App) -- Birth Data, Assessment Responses --> API
    end

    subgraph Backend (FastAPI on Railway)
        API(API Gateway / FastAPI)
        API -- Validate & Route --> CalcEngines
        API -- Store/Retrieve --> KG[Knowledge Graph / DB]
        API -- Request Synthesis --> SynthEng[Synthesis Engine]
        API -- Format & Return --> Output(JSON Response w/ Markdown)

        subgraph Calculation Engines
            AstroCalc(Astrology Module <br> -  JPL-based Ephemeris: jplephem High-Level Wrapper: Skyfield<br> - Ephemeris @ /opt/ephemeris)
            HDCalc(Human Design Module <br> - Proxy HumanDesignAPI.nl)
            AssessCalc(Assessment Module <br> - Ported JS Logic)
        end

        CalcEngines -- Processed Data --> KGPopulator(KG Population Service)
        KGPopulator -- Populates --> KG

        SynthEng -- Queries --> KG
        SynthEng -- Generates Insights --> Narrator(Narrative Service)
        Narrator -- Formatted Insights --> API
    end

    style KG fill:#f9f,stroke:#333,stroke-width:2px
    style AstroCalc fill:#ccf,stroke:#333
    style HDCalc fill:#cdf,stroke:#333
    style AssessCalc fill:#cfc,stroke:#333
    style SynthEng fill:#fcc,stroke:#333
```

---

## Data Flow Overview (Conceptual)

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant AstroMod as Astrology Module
    participant HDMod as Human Design Module
    participant AssessMod as Assessment Module
    participant KG as Knowledge Graph
    participant SynthEng as Synthesis Engine

    Client->>+API: POST /profile/create (Birth Data, Assessment Responses)
    API->>AstroMod: Calculate Chart(Birth Data)
    AstroMod-->>API: Astrological Data
    API->>HDMod: Get Chart(Birth Data)
    HDMod-->>API: Human Design Data
    API->>AssessMod: Calculate Scores(Assessment Responses)
    AssessMod-->>API: Assessment Results (Typology, Scores)
    API->>+KG: Store Integrated Profile (Astro, HD, Assess)
    KG-->>-API: Profile ID
    API->>+SynthEng: Synthesize Insights(Profile ID)
    SynthEng->>+KG: Query Profile Data(Profile ID)
    KG-->>-SynthEng: Profile Data
    SynthEng-->>API: Synthesized Insights
    API-->>-Client: { profile_id: ..., status: "processing" / "complete" }

    Client->>+API: GET /profile/{profile_id}
    API->>+SynthEng: Retrieve Insights(Profile ID)
    SynthEng->>+KG: Query Profile & Insights(Profile ID)
    KG-->>-SynthEng: Profile & Insights Data
    SynthEng-->>API: Formatted Insights (JSON w/ Markdown)
    API-->>-Client: Synthesized Profile Response
