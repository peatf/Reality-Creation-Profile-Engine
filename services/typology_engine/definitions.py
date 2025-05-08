# src/assessment/definitions.py
# Static definitions for the Reality Creation Assessment questions and structure.

# --- Part 1: Typology Spectrums ---
TYPOLOGY_SPECTRUMS = [
    {
        "id": "cognitive-alignment",
        "name": "Cognitive Alignment",
        "description": "How you mentally interact with reality",
        "leftLabel": "Rational",
        "rightLabel": "Intuitive",
        "questions": [
            {
                "id": "cognitive-q1",
                "text": "When a new idea lands, your first instinct is to:",
                "options": [
                    {"id": "cognitive-q1-left", "text": "Break it down logically: you want evidence before you let it in", "value": "left"},
                    {"id": "cognitive-q1-balanced", "text": "Weigh it out: feel the logic, but also listen to your intuition", "value": "balanced"},
                    {"id": "cognitive-q1-right", "text": "Trust your instincts or your gut if it clicks in your body, that's enough", "value": "right"}
                ]
            },
            {
                "id": "cognitive-q2",
                "text": "What feels most trustworthy to you when making important choices?",
                "options": [
                    {"id": "cognitive-q2-left", "text": "Thorough analysis and careful reasoning.", "value": "left"},
                    {"id": "cognitive-q2-balanced", "text": "A blend of logic and intuitive guidance.", "value": "balanced"},
                    {"id": "cognitive-q2-right", "text": "Inner knowing and spontaneous insight.", "value": "right"}
                ]
            }
        ]
    },
    {
        "id": "perceptual-focus",
        "name": "Perceptual Focus",
        "description": "Clarity and openness in manifestation",
        "leftLabel": "Definitive",
        "rightLabel": "Receptive",
        "questions": [
            {
                "id": "perceptual-q1",
                "text": "When envisioning your goals, you prefer:",
                "options": [
                    {"id": "perceptual-q1-left", "text": "Precise, detailed visions of exactly what you want.", "value": "left"},
                    {"id": "perceptual-q1-balanced", "text": "A general vision with some specifics, leaving space for surprises.", "value": "balanced"},
                    {"id": "perceptual-q1-right", "text": "Staying open and allowing your vision to evolve naturally over time.", "value": "right"}
                ]
            },
            {
                "id": "perceptual-q2",
                "text": "You feel most comfortable manifesting when:",
                "options": [
                    {"id": "perceptual-q2-left", "text": "You know exactly what you're aiming for.", "value": "left"},
                    {"id": "perceptual-q2-balanced", "text": "You have clear intentions but remain flexible in details.", "value": "balanced"},
                    {"id": "perceptual-q2-right", "text": "You trust life to surprise you in positive ways.", "value": "right"}
                ]
            }
        ]
    },
    {
        "id": "kinetic-drive",
        "name": "Kinetic Drive",
        "description": "How you approach action and momentum",
        "leftLabel": "Deliberate",
        "rightLabel": "Spontaneous",
        "questions": [
            {
                "id": "kinetic-q1",
                "text": "Your typical action-taking style looks like:",
                "options": [
                    {"id": "kinetic-q1-left", "text": "Thoughtful and intentional, with detailed plans.", "value": "left"},
                    {"id": "kinetic-q1-balanced", "text": "Adaptive: adjusting your pacing based on current circumstances.", "value": "balanced"},
                    {"id": "kinetic-q1-right", "text": "Quick and spontaneous: trusting instinct to guide you.", "value": "right"}
                ]
            },
            {
                "id": "kinetic-q2",
                "text": "Momentum feels most natural to you when:",
                "options": [
                    {"id": "kinetic-q2-left", "text": "Following a structured action plan.", "value": "left"},
                    {"id": "kinetic-q2-balanced", "text": "Flowing between structured and spontaneous bursts of activity.", "value": "balanced"},
                    {"id": "kinetic-q2-right", "text": "Acting in spontaneous bursts without detailed planning.", "value": "right"}
                ]
            }
        ]
    },
    {
        "id": "choice-navigation",
        "name": "Choice Navigation",
        "description": "Decision-making style",
        "leftLabel": "Calculative",
        "rightLabel": "Fluid",
        "questions": [
            {
                "id": "choice-q1",
                "text": "When making important decisions, you tend to:",
                "options": [
                    {"id": "choice-q1-left", "text": "Carefully weigh every option to ensure clarity.", "value": "left"},
                    {"id": "choice-q1-balanced", "text": "Use logic and intuition equally, adjusting as needed.", "value": "balanced"},
                    {"id": "choice-q1-right", "text": "Trust your intuition and let decisions unfold naturally.", "value": "right"}
                ]
            },
            {
                "id": "choice-q2",
                "text": "Your approach to uncertainty usually involves:",
                "options": [
                    {"id": "choice-q2-left", "text": "Making precise plans to minimize unpredictability.", "value": "left"},
                    {"id": "choice-q2-balanced", "text": "Finding a comfortable balance between planning and flexibility.", "value": "balanced"},
                    {"id": "choice-q2-right", "text": "Staying open and responding intuitively as things unfold.", "value": "right"}
                ]
            }
        ]
    },
    {
        "id": "resonance-field",
        "name": "Resonance Field",
        "description": "Emotional interaction in manifestation",
        "leftLabel": "Regulated",
        "rightLabel": "Expressive",
        "questions": [
            {
                "id": "resonance-q1",
                "text": "When strong emotions arise, you usually:",
                "options": [
                    {"id": "resonance-q1-left", "text": "Pause to regulate and stabilize before taking action.", "value": "left"},
                    {"id": "resonance-q1-balanced", "text": "Allow yourself to feel emotions fully, then adjust as needed.", "value": "balanced"},
                    {"id": "resonance-q1-right", "text": "Let your emotions immediately shape your actions and next steps.", "value": "right"}
                ]
            },
            {
                "id": "resonance-q2",
                "text": "Your most effective manifestations occur when you:",
                "options": [
                    {"id": "resonance-q2-left", "text": "Intentionally cultivate emotional stability.", "value": "left"},
                    {"id": "resonance-q2-balanced", "text": "Allow emotions to inform your process but maintain some stability.", "value": "balanced"},
                    {"id": "resonance-q2-right", "text": "Freely channel emotional energy into creating outcomes.", "value": "right"}
                ]
            }
        ]
    },
    {
        "id": "manifestation-rhythm",
        "name": "Manifestation Rhythm",
        "description": "Sustainability and adaptability over time",
        "leftLabel": "Structured",
        "rightLabel": "Dynamic",
        "questions": [
            {
                "id": "rhythm-q1",
                "text": "You maintain long-term consistency best when:",
                "options": [
                    {"id": "rhythm-q1-left", "text": "Following predictable cycles and clear routines.", "value": "left"},
                    {"id": "rhythm-q1-balanced", "text": "Balancing consistent routines with occasional shifts in approach.", "value": "balanced"},
                    {"id": "rhythm-q1-right", "text": "Frequently adjusting your methods based on inspiration and changing energy.", "value": "right"}
                ]
            },
            {
                "id": "rhythm-q2",
                "text": "Your ideal creative process looks most like:",
                "options": [
                    {"id": "rhythm-q2-left", "text": "A structured series of clearly defined steps.", "value": "left"},
                    {"id": "rhythm-q2-balanced", "text": "A sustainable rhythm blending structure and flow.", "value": "balanced"},
                    {"id": "rhythm-q2-right", "text": "A dynamic process that evolves as inspiration strikes.", "value": "right"}
                ]
            }
        ]
    },
]

# --- Part 2: Mastery Sections ---
MASTERY_SECTIONS = [
  {
    "id": 'core-priorities',
    "title": 'Core Priorities & Values',
    "description": 'These questions help identify your deep values and non-negotiable priorities in the manifestation process.',
    "progress": 25,
    "questions": [
      {
        "id": "core-q1",
        "text": "When it comes to crafting a life, what is the one area that you refuse to negotiate on? The thing that, if missing, would make everything else feel hollow?",
        "options": [
          {"id": "core-q1-creative", "text": "The ability to express myself fully and create from truth", "value": "creative-expression"},
          {"id": "core-q1-financial", "text": "A reality where resources flow with ease and choice is never limited", "value": "financial-abundance"},
          {"id": "core-q1-emotional", "text": "Depth, safety, and connection in how I experience emotions", "value": "emotional-fulfillment"},
          {"id": "core-q1-autonomy", "text": "The power to direct my own life without restriction", "value": "personal-autonomy"},
          {"id": "core-q1-relationships", "text": "Relationships that feel deep, real, and nourishing", "value": "deep-relationships"},
          {"id": "core-q1-spiritual", "text": "A connection to something greater than myself, a thread of meaning that runs through everything", "value": "spiritual-connection"}
        ]
      },
      {
        "id": "core-q2",
        "text": "Which of these experiences, when you imagine living it fully, fills you with a deep sense of rightness? Which of these gives you access to the sensation of a \"mission complete\"?",
        "options": [
          {"id": "core-q2-mastery", "text": "Reaching a level of mastery in my craft where my work is recognized and deeply valued", "value": "craft-mastery"},
          {"id": "core-q2-wealth", "text": "Creating a reality where money is a source of expansion, over limitation, where wealth and security flow with ease", "value": "wealth-security"},
          {"id": "core-q2-peace", "text": "Living in a state of emotional steadiness, where peace and well-being are my default, not something I have to chase", "value": "emotional-peace"},
          {"id": "core-q2-freedom", "text": "Holding the reins of my own life, where my time, choices, and direction are entirely my own", "value": "personal-freedom"},
          {"id": "core-q2-connection", "text": "Being surrounded by relationships that feel like home, nourishing, real, and deeply connected", "value": "deep-connection"},
          {"id": "core-q2-meaning", "text": "Feeling anchored in something beyond myself, where meaning, mystery, and a higher connection guide my path", "value": "higher-meaning"}
        ]
      },
      {
        "id": "core-q3",
        "text": "When you think of how you want to feel most often, which of these do you most want to consistently experience in your reality?",
        "options": [
          {"id": "core-q3-confidence", "text": "Confidence & Self Trust", "value": "confidence-trust"},
          {"id": "core-q3-peace", "text": "Peace & Ease", "value": "peace-ease"},
          {"id": "core-q3-choice", "text": "Choice & Autonomy", "value": "choice-autonomy"},
          {"id": "core-q3-stability", "text": "Stability & Security", "value": "stability-security"},
          {"id": "core-q3-passion", "text": "Passion & Inspiration", "value": "passion-inspiration"},
          {"id": "core-q3-joy", "text": "Joy & Excitement", "value": "joy-excitement"}
        ]
      }
    ]
  },
  {
    "id": 'growth-areas',
    "title": 'Growth & Permission Areas',
    "description": 'These questions help diagnose your current growth areas and friction points in your manifestation process.',
    "progress": 50,
    "questions": [
      {
        "id": "growth-q1",
        "text": "Where do you often find friction or frustration in your manifestation journey?",
        "options": [
          {"id": "growth-q1-consistency", "text": "Staying committed and consistent long-term", "value": "consistency-challenge"},
          {"id": "growth-q1-clarity", "text": "Getting clear and decisive about what I truly want", "value": "clarity-challenge"},
          {"id": "growth-q1-action", "text": "Taking inspired action consistently", "value": "action-challenge"},
          {"id": "growth-q1-intuition", "text": "Trusting my intuition over external opinions", "value": "intuition-challenge"},
          {"id": "growth-q1-emotions", "text": "Maintaining emotional stability", "value": "emotion-challenge"},
          {"id": "growth-q1-receiving", "text": "Staying open and receptive, letting things unfold without forcing the outcome", "value": "receiving-challenge"}
        ]
      },
      {
        "id": "growth-q2",
        "text": "Which statement resonates most deeply with your most recent challenges?",
        "options": [
          {"id": "growth-q2-doubt", "text": "\"I often second-guess or doubt my decisions.\"", "value": "decision-doubt"},
          {"id": "growth-q2-action", "text": "\"I can clearly envision what I want but struggle to act consistently.\"", "value": "action-gap"},
          {"id": "growth-q2-focus", "text": "\"I act impulsively and struggle to sustain long-term focus.\"", "value": "focus-challenge"},
          {"id": "growth-q2-emotions", "text": "\"I frequently feel emotionally overwhelmed or blocked.\"", "value": "emotional-block"},
          {"id": "growth-q2-burnout", "text": "\"I push too hard, causing burnout instead of flow.\"", "value": "burnout-pattern"},
          {"id": "growth-q2-commitment", "text": "\"I hesitate to fully commit, waiting for certainty.\"", "value": "commitment-hesitation"}
        ]
      },
      {
        "id": "growth-q3",
        "text": "In which area do you tend to experience the most internal resistance?",
        "options": [
          {"id": "growth-q3-trust", "text": "Trusting my own perceptions and intuition", "value": "self-trust-resistance"},
          {"id": "growth-q3-risk", "text": "Taking calculated risks toward my desires", "value": "risk-resistance"},
          {"id": "growth-q3-emotion", "text": "Allowing myself to fully express my emotions authentically", "value": "emotional-expression-resistance"},
          {"id": "growth-q3-vision", "text": "Clearly defining and sticking to a specific vision", "value": "vision-clarity-resistance"},
          {"id": "growth-q3-momentum", "text": "Maintaining a consistent rhythm and momentum", "value": "momentum-resistance"},
          {"id": "growth-q3-control", "text": "Letting go of control and allowing the unexpected", "value": "control-resistance"}
        ]
      }
    ]
  },
  {
    "id": 'alignment-needs',
    "title": 'Acceptance & Alignment Needs',
    "description": 'These questions help pinpoint areas requiring acceptance or alignment adjustments in your manifestation process.',
    "progress": 75,
    "questions": [
      {
        "id": "alignment-q1",
        "text": "Which of the following feels most relieving to imagine accepting about yourself?",
        "options": [
          {"id": "alignment-q1-cycles", "text": "\"I naturally move in cycles; my momentum comes in waves.\"", "value": "accept-cycles"},
          {"id": "alignment-q1-structure", "text": "\"I thrive best with structure, not constant spontaneity.\"", "value": "accept-structure"},
          {"id": "alignment-q1-emotions", "text": "\"My emotions deeply influence my outcomes, and that's okay.\"", "value": "accept-emotions"},
          {"id": "alignment-q1-clarity", "text": "\"Clarity for me emerges gradually rather than instantly.\"", "value": "accept-gradual-clarity"},
          {"id": "alignment-q1-intuition", "text": "\"Trusting intuition can serve me as powerfully as logic.\"", "value": "accept-intuition"},
          {"id": "alignment-q1-flexibility", "text": "\"My desire for spaciousness and flexibility outweighs my need for certainty.\"", "value": "accept-flexibility"}
        ]
      },
      {
        "id": "alignment-q2",
        "text": "Which area do you sense you are trying hardest to force or control?",
        "options": [
          {"id": "alignment-q2-outcomes", "text": "Outcomes and timing of my manifestations", "value": "control-outcomes"},
          {"id": "alignment-q2-emotions", "text": "Emotions and inner states", "value": "control-emotions"},
          {"id": "alignment-q2-consistency", "text": "Consistency and long-term momentum", "value": "control-consistency"},
          {"id": "alignment-q2-clarity", "text": "Clarity and specificity of my vision", "value": "control-clarity"},
          {"id": "alignment-q2-decisions", "text": "Decision-making and certainty", "value": "control-decisions"},
          {"id": "alignment-q2-intuition", "text": "My intuitive impulses and inspiration", "value": "control-intuition"}
        ]
      }
    ]
  },
  {
    "id": 'energy-patterns',
    "title": 'Natural Energy Patterns',
    "description": 'These questions help uncover your hidden energetic preferences and needs for optimal manifestation.',
    "progress": 100,
    "questions": [
      {
        "id": "energy-q1",
        "text": "When things are flowing and success feels natural, it's usually because you:",
        "options": [
          {"id": "energy-q1-instructions", "text": "Had clear, step-by-step instructions", "value": "clear-instructions"},
          {"id": "energy-q1-intuition", "text": "Followed your intuitive instincts spontaneously", "value": "intuitive-instincts"},
          {"id": "energy-q1-inspired", "text": "Felt emotionally inspired and connected to the task", "value": "emotional-inspiration"},
          {"id": "energy-q1-rhythm", "text": "Found the right rhythm between structure and flexibility", "value": "balanced-rhythm"},
          {"id": "energy-q1-clarity", "text": "Allowed yourself time to ease into the clarity of the goal", "value": "gradual-clarity"},
          {"id": "energy-q1-trust", "text": "Let go of control and trusted the process would unfold naturally", "value": "process-trust"}
        ]
      },
      {
        "id": "energy-q2",
        "text": "If you look closely at when you've struggled or felt resistance, it often involved:",
        "options": [
          {"id": "energy-q2-rigid", "text": "Trying to adhere to overly rigid routines", "value": "rigid-routines"},
          {"id": "energy-q2-intuition", "text": "Ignoring intuitive signals or inner guidance", "value": "ignored-intuition"},
          {"id": "energy-q2-emotions", "text": "Suppressing or disconnecting from your emotional states", "value": "suppressed-emotions"},
          {"id": "energy-q2-clarity", "text": "Expecting immediate clarity without allowing yourself to experiment", "value": "forced-clarity"},
          {"id": "energy-q2-cycles", "text": "Forcing consistent action instead of embracing your natural energy cycles", "value": "ignored-cycles"},
          {"id": "energy-q2-control", "text": "Overplanning and trying to control outcomes tightly", "value": "overcontrolling"}
        ]
      },
      {
        "id": "energy-q3",
        "text": "You feel most energized and naturally productive when you:",
        "options": [
          {"id": "energy-q3-routines", "text": "Work within clear routines that offer structure and predictability", "value": "structured-productivity"},
          {"id": "energy-q3-freedom", "text": "Have the freedom to follow your own flow throughout the day", "value": "flexible-productivity"},
          {"id": "energy-q3-emotions", "text": "Feel emotionally connected to what you're creating, it has meaning", "value": "emotional-productivity"},
          {"id": "energy-q3-motivation", "text": "Ride spontaneous bursts of inspiration and act on them quickly", "value": "spontaneous-productivity"},
          {"id": "energy-q3-change", "text": "Shift your approach often, based on what the moment calls for", "value": "adaptive-productivity"},
          {"id": "energy-q3-balance", "text": "Blend planning with intuition, adjusting as you go", "value": "balanced-productivity"}
        ]
      },
      {
        "id": "energy-q4",
        "text": "Your ideal supportive environment feels:",
        "options": [
          {"id": "energy-q4-calm", "text": "Calm, structured, and predictable", "value": "structured-environment"},
          {"id": "energy-q4-stimulating", "text": "Stimulating, flexible, and evolving", "value": "dynamic-environment"},
          {"id": "energy-q4-supportive", "text": "Deeply supportive emotionally", "value": "emotionally-supportive-environment"},
          {"id": "energy-q4-inspiring", "text": "Inspiring, adaptable, and intuitively affirming", "value": "inspiring-environment"},
          {"id": "energy-q4-balanced", "text": "Balanced between clear planning and open-ended exploration", "value": "balanced-environment"},
          {"id": "energy-q4-pressure-free", "text": "Free from pressure, allowing gradual clarity and organic flow", "value": "pressure-free-environment"}
        ]
      }
    ]
  }
]