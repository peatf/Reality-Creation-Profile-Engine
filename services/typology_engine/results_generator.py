# src/assessment/results_generator.py
# Handles generating the descriptive text results for the assessment.

import logging
from typing import Dict, Any, List, Optional

# Import definitions (assuming definitions.py is in the same directory)
# We might not need definitions directly if all data comes from scorer results
# from .definitions import TYPOLOGY_SPECTRUMS, MASTERY_SECTIONS

logger = logging.getLogger(__name__)

# --- Ported Data Structures from redesigned-results.js ---

TYPOLOGY_DESCRIPTIONS = {
    # Cognitive Alignment - Structured Side
    "cognitive-alignment-strongLeft": {
        "name": "Highly Rational",
        "description": "You orient to reality firmly through logic, analysis, and tangible evidence. Understanding nearly always comes through reason, and you need verifiable clarity before accepting something as real.",
        "phrases": {
            "identity": "a logical thinker who values concrete evidence above all",
            "strength": "analytical reasoning and evidence-based evaluation",
            "approach": "You gather evidence, analyze patterns, and make decisions through systematic reasoning.",
            "challenge": "allowing intuitive insights when logical paths aren't immediately obvious",
            "growth": "learning to honor intuitive signals alongside your logical framework"
        }
    },
    "cognitive-alignment-leftLeaning": {
        "name": "Rational",
        "description": "You orient to reality through logic, analysis, and evidence. Understanding comes through reason, you need clarity you can track before you let something in as real.",
        "phrases": {
            "identity": "a predominantly logical thinker who appreciates structured reasoning",
            "strength": "methodical thinking and clear analysis",
            "approach": "You tend to seek logical explanations and evidence while remaining somewhat open to intuitive insights.",
            "challenge": "recognizing when intuition offers value that logic cannot provide",
            "growth": "developing comfort with both evidence-based and intuitive decision-making"
        }
    },
    "cognitive-alignment-balanced": {
        "name": "Synthesizing",
        "description": "You're a bridge between realms, braiding logic with inner knowing. You listen to both mind and intuition, translating between the two as you move through your process.",
        "phrases": {
            "identity": "a synthesizer who bridges logical and intuitive understanding",
            "strength": "integrating multiple ways of knowing and processing information",
            "approach": "You naturally weave between analytical thinking and intuitive insights, using both to navigate reality.",
            "challenge": "trusting when to lead with logic and when to follow intuition",
            "growth": "refining your ability to determine which approach serves best in different contexts"
        }
    },
    "cognitive-alignment-rightLeaning": {
        "name": "Intuitively Guided",
        "description": "You tend to trust inner knowing over pure logic. While you understand the value of analysis, you often find your truth through felt sense and inner resonance.",
        "phrases": {
            "identity": "an intuitive thinker who values inner knowing",
            "strength": "sensing truth and possibilities beyond logical constructs",
            "approach": "You follow intuitive signals first, then use logic to understand what your intuition has revealed.",
            "challenge": "grounding intuitive insights so others can understand them",
            "growth": "developing ways to validate and communicate intuitive knowledge"
        }
    },
    "cognitive-alignment-strongRight": {
        "name": "Highly Intuitive",
        "description": "You deeply trust what the body and spirit know before the mind can categorize it. Your truth rises almost exclusively through sensation, symbols, and inner resonance, rarely needing external proof to be felt as real.",
        "phrases": {
            "identity": "a highly intuitive being who navigates primarily through inner signals",
            "strength": "direct knowing that bypasses rational analysis",
            "approach": "You rely strongly on felt sense, inner vision, and energetic resonance to navigate reality.",
            "challenge": "translating intuitive knowledge into forms others can understand",
            "growth": "creating bridges between your intuitive knowing and shared reality"
        }
    },
    # Perceptual Focus
    "perceptual-focus-strongLeft": {
        "name": "Highly Definitive",
        "description": "You require exceptional clarity and detail in your vision. Precise specificity is essential for you, and you deeply believe that clearly articulated intentions manifest most effectively.",
        "phrases": {
            "identity": "a visionary with crystal-clear focus",
            "strength": "creating detailed and precise visions of what you desire",
            "approach": "You define your goals with remarkable specificity, clarifying exactly what you want before taking action.",
            "challenge": "allowing for beneficial variations that weren't in your original plan",
            "growth": "maintaining clarity while embracing unexpected beneficial outcomes"
        }
    },
    "perceptual-focus-leftLeaning": {
        "name": "Definitive",
        "description": "You feel strongest with a sharp, dialed-in vision. Specificity gives your energy a direction to flow toward. Clarity is a spell, it calls things in.",
        "phrases": {
            "identity": "a focused creator who values clarity",
            "strength": "defining clear intentions that guide your manifestation process",
            "approach": "You tend to focus on specific outcomes while remaining somewhat open to variations.",
            "challenge": "maintaining flexibility when reality offers something different but valuable",
            "growth": "holding vision firmly but not rigidly"
        }
    },
    "perceptual-focus-balanced": {
        "name": "Adaptive",
        "description": "You let clarity and curiosity sit side by side. You prefer to hold a vision without clenching it, staying precise and open at once.",
        "phrases": {
            "identity": "an adaptive creator who values both clarity and openness",
            "strength": "holding clear intentions while remaining receptive to organic development",
            "approach": "You naturally balance clear vision with openness to how things might unfold in unexpected ways.",
            "challenge": "knowing when to focus more specifically versus when to remain open",
            "growth": "refining your ability to shift between clarity and receptivity as needed"
        }
    },
    "perceptual-focus-rightLeaning": {
        "name": "Receptive",
        "description": "You keep your hands open. You don't lock into a vision, you listen for what's arriving. The future reveals itself as you move.",
        "phrases": {
            "identity": "a receptive creator who values openness and emergence",
            "strength": "remaining open to unexpected possibilities and synchronicities",
            "approach": "You tend to set general intentions while staying highly receptive to how things actually unfold.",
            "challenge": "bringing enough focus to manifest specific outcomes when needed",
            "growth": "developing the ability to apply focus without restricting flow"
        }
    },
    "perceptual-focus-strongRight": {
        "name": "Highly Receptive",
        "description": "You maintain exceptional openness to what wants to emerge. Rather than defining specific outcomes, you allow life to reveal itself organically, trusting that what arrives is often better than what could be planned.",
        "phrases": {
            "identity": "a highly receptive being who trusts life's unfolding",
            "strength": "surrendering to the natural flow of emergence and synchronicity",
            "approach": "You set very general intentions and then listen deeply to what life is bringing forward.",
            "challenge": "creating enough structure to manifest when specific outcomes are needed",
            "growth": "balancing complete receptivity with intentional creation"
        }
    },
    # Kinetic Drive
    "kinetic-drive-strongLeft": {
        "name": "Highly Deliberate",
        "description": "You move with exceptional intentionality and structure. Detailed plans, clear steps, and reliable systems are essential to your process, providing the foundation for all your actions.",
        "phrases": {
            "identity": "a methodical creator who thrives with comprehensive planning",
            "strength": "creating thorough, structured plans that guide consistent action",
            "approach": "You develop detailed plans with clear steps before taking significant action.",
            "challenge": "adapting quickly when circumstances require deviation from plans",
            "growth": "maintaining structure while developing greater flexibility"
        }
    },
    "kinetic-drive-leftLeaning": {
        "name": "Deliberate",
        "description": "You like to move with intention. Plans, steps, systems, they help you feel rooted. Structure is your launchpad.",
        "phrases": {
            "identity": "a purposeful creator who values intentional planning",
            "strength": "taking focused action guided by clear strategies",
            "approach": "You typically create plans before acting while allowing some room for adjustment.",
            "challenge": "recognizing when spontaneous action would be more effective",
            "growth": "balancing structured approach with timely opportunism"
        }
    },
    "kinetic-drive-balanced": {
        "name": "Rhythmic",
        "description": "You tune into the beat of the moment. You know when to push and when to pause, riding the natural rhythm of action and rest.",
        "phrases": {
            "identity": "a rhythmic creator who honors natural cycles",
            "strength": "harmonizing structured action with spontaneous inspiration",
            "approach": "You blend planning with intuitive timing, respecting the natural flow of energy.",
            "challenge": "maintaining momentum when neither structure nor inspiration feels available",
            "growth": "deepening your understanding of your unique energy cycles"
        }
    },
    "kinetic-drive-rightLeaning": {
        "name": "Spontaneous",
        "description": "You act in the moment the spark hits. Instinct leads. Planning takes the backseat. Your momentum comes from inspired action.",
        "phrases": {
            "identity": "an inspired creator who trusts spontaneous action",
            "strength": "taking immediate action when inspiration strikes",
            "approach": "You typically follow intuitive impulses while maintaining some awareness of overall direction.",
            "challenge": "sustaining momentum when immediate inspiration isn't present",
            "growth": "developing light structures that support without restricting flow"
        }
    },
    "kinetic-drive-strongRight": {
        "name": "Highly Spontaneous",
        "description": "You move almost exclusively in response to immediate inspiration. Detailed planning feels restrictive, and your greatest momentum comes from following energy as it arises in the moment.",
        "phrases": {
            "identity": "a highly intuitive creator who thrives on immediate impulse",
            "strength": "taking bold, inspired action without hesitation",
            "approach": "You act primarily on intuitive inspiration with minimal planning or preparation.",
            "challenge": "maintaining consistency when not feeling immediately inspired",
            "growth": "honoring your spontaneous nature while developing sustainable rhythms"
        }
    },
    # Choice Navigation
    "choice-navigation-strongLeft": {
        "name": "Highly Calculative",
        "description": "You approach decisions through comprehensive analysis. Every choice is carefully weighed, options are methodically evaluated, and you strongly prefer moving forward only when the path ahead is clear.",
        "phrases": {
            "identity": "a systematic decision-maker who values thorough analysis",
            "strength": "making carefully considered choices with clear rationale",
            "approach": "You thoroughly analyze options, weighing pros and cons before making decisions.",
            "challenge": "making timely decisions when complete information isn't available",
            "growth": "developing comfort with some uncertainty in the decision process"
        }
    },
    "choice-navigation-leftLeaning": {
        "name": "Calculative",
        "description": "You prefer a pause before the plunge. You look at the map, trace the paths, and make your move from strategy.",
        "phrases": {
            "identity": "a thoughtful decision-maker who values clarity",
            "strength": "making well-considered choices with awareness of potential outcomes",
            "approach": "You typically evaluate options before deciding while remaining somewhat open to intuitive guidance.",
            "challenge": "recognizing when analysis is creating unnecessary delay",
            "growth": "balancing thoughtful consideration with decisive action"
        }
    },
    "choice-navigation-balanced": {
        "name": "Responsive",
        "description": "You can shift between plan and pull, sensing when a decision needs logic, and when it just needs a yes from your body.",
        "phrases": {
            "identity": "an adaptive decision-maker who uses multiple inputs",
            "strength": "appropriately matching your decision style to the situation at hand",
            "approach": "You naturally integrate logical analysis with intuitive guidance when making choices.",
            "challenge": "determining which approach serves best in ambiguous situations",
            "growth": "deepening trust in your ability to choose the right approach for each decision"
        }
    },
    "choice-navigation-rightLeaning": {
        "name": "Fluid",
        "description": "You follow the river, not the roadmap. Your choices rise from the current of inner guidance, not external structure.",
        "phrases": {
            "identity": "an intuitive decision-maker who trusts inner guidance",
            "strength": "making choices that align with deeper knowings and energy flows",
            "approach": "You typically follow intuitive signals while maintaining some awareness of logical considerations.",
            "challenge": "explaining your choices to those who need logical rationales",
            "growth": "honoring your intuitive process while developing ways to communicate it"
        }
    },
    "choice-navigation-strongRight": {
        "name": "Highly Fluid",
        "description": "Your decisions emerge almost exclusively from intuitive knowing. You deeply trust the flow of life to guide your choices, with minimal need for analytical consideration or external validation.",
        "phrases": {
            "identity": "a highly intuitive navigator who follows energy currents",
            "strength": "making aligned choices through direct inner knowing",
            "approach": "You trust your intuitive signals implicitly, letting decisions emerge organically.",
            "challenge": "communicating your process to those who rely on logical decision frameworks",
            "growth": "creating bridges between your intuitive process and collaborative decisions"
        }
    },
    # Resonance Field
    "resonance-field-strongLeft": {
        "name": "Highly Regulated",
        "description": "You approach emotions with exceptional intentionality. Careful management of your emotional state is a core practice, and you excel at creating emotional stability to support your manifestation process.",
        "phrases": {
            "identity": "an emotionally disciplined creator who values stability",
            "strength": "maintaining consistent emotional tone through intentional practices",
            "approach": "You carefully cultivate beneficial emotional states and mindfully process challenging emotions.",
            "challenge": "allowing emotional authenticity when it doesn't match your intended state",
            "growth": "balancing emotional regulation with emotional honesty"
        }
    },
    "resonance-field-leftLeaning": {
        "name": "Regulated",
        "description": "You approach your emotional state with intention. You work with feeling like a sculptor, shaping it to support your path.",
        "phrases": {
            "identity": "an intentional creator who values emotional mastery",
            "strength": "directing emotional energy toward desired outcomes",
            "approach": "You typically manage your emotional state while allowing some natural expression.",
            "challenge": "recognizing when emotional control becomes emotional suppression",
            "growth": "developing greater emotional fluidity while maintaining core stability"
        }
    },
    "resonance-field-balanced": {
        "name": "Attuned",
        "description": "You can read the emotional weather inside and around you. You let feeling move, but you also know how to steady yourself in the storm.",
        "phrases": {
            "identity": "an emotionally attuned creator who values both expression and stability",
            "strength": "navigating emotional currents with awareness and balance",
            "approach": "You naturally allow emotional movement while maintaining a centered presence.",
            "challenge": "finding the right balance between expression and regulation in each situation",
            "growth": "deepening your emotional intelligence across varied circumstances"
        }
    },
    "resonance-field-rightLeaning": {
        "name": "Expressive",
        "description": "Your emotions are part of the magic. You don't try to control the tides, you ride them. Feeling leads the way.",
        "phrases": {
            "identity": "an emotionally expressive creator who values authenticity",
            "strength": "allowing genuine emotional energy to fuel your creation process",
            "approach": "You typically follow emotional currents while maintaining some awareness of emotional impact.",
            "challenge": "creating stability when emotional waters run turbulent",
            "growth": "honoring emotional expression while developing emotional resilience"
        }
    },
    "resonance-field-strongRight": {
        "name": "Highly Expressive",
        "description": "You experience emotions with remarkable intensity and authenticity. Rather than managing emotions, you surrender to their flow, allowing them to guide your creative process in profound ways.",
        "phrases": {
            "identity": "a deeply feeling creator who navigates through emotional currents",
            "strength": "accessing powerful creative energy through emotional authenticity",
            "approach": "You dive fully into emotional experiences, letting them guide your manifestation process.",
            "challenge": "maintaining functioning when processing intense emotional states",
            "growth": "developing emotional resilience without dampening emotional depth"
        }
    },
    # Manifestation Rhythm
    "manifestation-rhythm-strongLeft": {
        "name": "Highly Structured",
        "description": "You thrive with exceptionally clear timelines, consistent cycles, and established rituals. Structured routines are essential to your process, providing the container through which your creative energy flows most effectively.",
        "phrases": {
            "identity": "a methodical creator who values consistent routines",
            "strength": "maintaining reliable rhythms that produce steady results",
            "approach": "You establish clear structures and follow them with remarkable consistency.",
            "challenge": "adapting when external circumstances disrupt your established patterns",
            "growth": "developing flexible structures that can evolve as needed"
        }
    },
    "manifestation-rhythm-leftLeaning": {
        "name": "Structured",
        "description": "You thrive with timelines, cycles, and steady rituals. For you structure is not a cage, it's your container for creation.",
        "phrases": {
            "identity": "a consistent creator who values reliable rhythms",
            "strength": "establishing supportive routines that build momentum",
            "approach": "You typically create and follow structured patterns while allowing some variation.",
            "challenge": "recognizing when structures need to evolve or be released",
            "growth": "developing structures that support without restricting growth"
        }
    },
    "manifestation-rhythm-balanced": {
        "name": "Sustainable",
        "description": "You walk the middle path between consistency and flow. You build momentum that doesn't burn out.",
        "phrases": {
            "identity": "a balanced creator who values sustainable momentum",
            "strength": "maintaining progress through both structure and flexibility",
            "approach": "You naturally balance consistent practices with adaptability to changing conditions.",
            "challenge": "knowing when to lean more toward structure versus flow",
            "growth": "refining your ability to shift rhythms while maintaining momentum"
        }
    },
    "manifestation-rhythm-rightLeaning": {
        "name": "Dynamic",
        "description": "You're always evolving. You change how you create based on who you are right now, not who you were last week. Nothing is fixed, everything moves.",
        "phrases": {
            "identity": "an adaptable creator who values evolution and change",
            "strength": "adjusting your approach to match current energy and circumstances",
            "approach": "You typically follow what feels alive now while maintaining some awareness of continuity.",
            "challenge": "creating enough consistency to build long-term momentum",
            "growth": "honoring your need for variation while establishing gentle continuity"
        }
    },
    "manifestation-rhythm-strongRight": {
        "name": "Highly Dynamic",
        "description": "Your creative process is in constant evolution. You deeply resist fixed patterns and thrive when able to completely reinvent your approach based on current inspiration and energy levels.",
        "phrases": {
            "identity": "a highly fluid creator who thrives through constant renewal",
            "strength": "bringing fresh energy through frequent reinvention of your process",
            "approach": "You follow what feels most alive in each moment, allowing your process to transform continuously.",
            "challenge": "completing longer-term projects that require sustained focus",
            "growth": "developing minimalist continuity that supports without restricting evolution"
        }
    }
}

TYPOLOGY_PAIRS = {
    "strongly-structured-strongly-structured": {
        "name": "Master Architect",
        "description": "You build with exceptional precision and methodology. You see reality like a detailed blueprint and bring visions to life through meticulous planning, clear systems, and consistent implementation. Your remarkable strength is in creating robust frameworks that reliably produce results.",
        "phrases": {
            "essence": "disciplined creation through comprehensive systems",
            "strength": "creating highly structured frameworks that consistently produce results",
            "challenge": "allowing space for the unexpected within your carefully designed plans",
            "approach": "You meticulously design and implement systems, ensuring each element serves the greater structure.",
            "growth": "learning to maintain your powerful structure while allowing for inspired deviation"
        }
    },
    "strongly-structured-structured": {
        "name": "Strategic Architect",
        "description": "You build with intention. You see reality like a blueprint and bring visions to life through clarity, planning, and steady movement. Your strength is in creating grounded systems that actually work. You're not just manifesting dreams, you're engineering them.",
        "phrases": {
            "essence": "methodical creation through clear structure",
            "strength": "designing and implementing effective systems that produce reliable results",
            "challenge": "recognizing when flexibility would better serve your objectives",
            "approach": "You carefully plan and structure your manifestation process, ensuring each step builds logically upon the last.",
            "growth": "developing the ability to adapt your structured approach when circumstances change"
        }
    },
    "structured-structured": { # Alias for consistency
        "name": "Strategic Architect",
        "description": "You build with intention. You see reality like a blueprint and bring visions to life through clarity, planning, and steady movement. Your strength is in creating grounded systems that actually work. You're not just manifesting dreams, you're engineering them.",
        "phrases": {
            "essence": "methodical creation through clear structure",
            "strength": "designing and implementing effective systems that produce reliable results",
            "challenge": "recognizing when flexibility would better serve your objectives",
            "approach": "You carefully plan and structure your manifestation process, ensuring each step builds logically upon the last.",
            "growth": "developing the ability to adapt your structured approach when circumstances change"
        }
    },
    "strongly-structured-balanced": {
        "name": "Structured Integrator",
        "description": "You lead with powerful structure but recognize the value of adaptation. Your mind organizes with remarkable precision, while still leaving space for organic development. You excel at creating systems that maintain their integrity while allowing for natural evolution.",
        "phrases": {
            "essence": "structured creation with space for adaptation",
            "strength": "building robust systems that remain adaptable to changing conditions",
            "challenge": "determining when to maintain structure versus when to allow flexibility",
            "approach": "You create clear frameworks first, then allow for mindful adjustments as implementation proceeds.",
            "growth": "further refining your ability to determine when structure serves and when flexibility is needed"
        }
    },
    "structured-balanced": {
        "name": "Practical Synthesizer",
        "description": "You lead with structure but make space for intuitive influence. Your mind organizes with ease, and your process flexes just enough to invite in surprise. You're pragmatic but not rigid. You listen for alignment before locking in a plan.",
        "phrases": {
            "essence": "practical creation with room for adaptation",
            "strength": "balancing clear structure with openness to organic development",
            "challenge": "trusting intuitive adjustments within your structured approach",
            "approach": "You create frameworks that provide direction while remaining open to refinement as you progress.",
            "growth": "deepening your trust in both structured planning and intuitive adjustment"
        }
    },
    "strongly-structured-fluid": {
        "name": "Structured Visionary",
        "description": "You bring powerful structure to intuitive vision. You excel at creating robust systems that serve inspired ideas, bringing exceptional discipline to creative flow. Your unique strength lies in translating ethereal concepts into practical reality with remarkable precision.",
        "phrases": {
            "essence": "bringing disciplined structure to intuitive vision",
            "strength": "implementing highly organized systems that serve inspired ideas",
            "challenge": "allowing intuitive guidance to sometimes redirect your structured approach",
            "approach": "You receive intuitive insights, then apply methodical processes to manifest them in tangible form.",
            "growth": "developing greater trust in the interplay between precise structure and intuitive guidance"
        }
    },
    "structured-fluid": {
        "name": "Grounded Visionary",
        "description": "You bring the sky to the ground. Structure is your anchor, intuition is your compass. You create from a steady center, but you're not afraid to pivot when inspiration calls. You hold form and flow in the same hand.",
        "phrases": {
            "essence": "bringing practical form to intuitive guidance",
            "strength": "translating inspired ideas into workable structures",
            "challenge": "maintaining momentum when structure and inspiration seem at odds",
            "approach": "You balance structural thinking with intuitive listening, finding practical ways to implement inspired ideas.",
            "growth": "developing even greater harmony between your structured and intuitive aspects"
        }
    },
    "structured-strongly-fluid": {
        "name": "Anchor for Inspiration",
        "description": "You bring essential grounding to powerful vision. Structure serves as your foundation while deep intuition guides your direction. You excel at creating just enough form to channel remarkably fluid creative energy, without restricting its natural flow.",
        "phrases": {
            "essence": "providing structural support for highly intuitive creation",
            "strength": "anchoring visionary insights with practical implementation",
            "challenge": "creating enough structure without dampening powerful inspiration",
            "approach": "You receive strong intuitive guidance, then develop appropriate structures to bring it into form.",
            "growth": "refining your ability to determine exactly how much structure serves each inspired vision"
        }
    },
    "balanced-strongly-structured": {
        "name": "Adaptive Strategist",
        "description": "You bring important flexibility to powerful structure. Your adaptability complements your exceptional discipline, allowing you to refine methodical approaches through responsive adjustment. You excel at maintaining structural integrity while evolving implementation.",
        "phrases": {
            "essence": "bringing adaptability to highly structured creation",
            "strength": "maintaining structural integrity while allowing responsive evolution",
            "challenge": "knowing when to preserve structure versus when to introduce adaptation",
            "approach": "You respect established systems while introducing mindful adjustments to optimize outcomes.",
            "growth": "developing even greater discernment about when each approach best serves"
        }
    },
    "balanced-structured": {
        "name": "Integrated Strategist",
        "description": "You adapt with intention. Your ability to synthesize meets your gift for execution. You're a shapeshifter who knows how to build. You move between insight and implementation with grace, grounding your vision in form.",
        "phrases": {
            "essence": "adaptive creation with meaningful structure",
            "strength": "integrating multiple approaches while maintaining practical focus",
            "challenge": "trusting your adaptability within structured contexts",
            "approach": "You move fluidly between different perspectives, using structure to integrate what emerges.",
            "growth": "deepening your trust in both your adaptive nature and structural abilities"
        }
    },
    "balanced-balanced": {
        "name": "Harmonic Integrator",
        "description": "You are the center point. You naturally hold paradox without needing to collapse it. Structure and flow, logic and feeling, none of it is separate in your world. You make wholeness feel like home.",
        "phrases": {
            "essence": "integrated creation that honors multiple approaches",
            "strength": "harmonizing seemingly opposite approaches into cohesive wholes",
            "challenge": "maintaining your center when external forces pull toward extremes",
            "approach": "You naturally perceive multiple perspectives and weave them together in balanced implementation.",
            "growth": "deepening your capacity to embody integration even in challenging circumstances"
        }
    },
    "balanced-fluid": {
        "name": "Flowing Harmonizer",
        "description": "You lead with inner harmony and move through life like water. You invite clarity but don't demand it. Your creative process is intuitive, expansive, and still somehow grounded. You trust the rhythm of things.",
        "phrases": {
            "essence": "harmonious creation through adaptable flow",
            "strength": "maintaining center while flowing with intuitive guidance",
            "challenge": "creating enough structure to manifest intuitive insights",
            "approach": "You listen deeply to inner guidance while maintaining enough form to bring it into reality.",
            "growth": "developing greater capacity to anchor your intuitive flow when needed"
        }
    },
    "balanced-strongly-fluid": {
        "name": "Flow Navigator",
        "description": "You bring essential grounding to highly intuitive creation. While honoring powerful creative currents, you maintain enough center to navigate them effectively. You excel at staying oriented within expansive vision, helping inspiration find tangible expression.",
        "phrases": {
            "essence": "providing centered awareness within highly intuitive flow",
            "strength": "remaining oriented while swimming in deep creative currents",
            "challenge": "maintaining enough structure to translate powerful inspiration into form",
            "approach": "You honor intuitive guidance while providing just enough framework to manifest its essence.",
            "growth": "refining your ability to remain effectively centered within highly fluid creation"
        }
    },
    "fluid-strongly-structured": {
        "name": "Visionary Builder",
        "description": "You begin with intuitive vision and bring it into form through exceptional discipline. Your intuition guides what wants to emerge, while your remarkable structural abilities determine how to manifest it effectively. You excel at bridging inspiration and methodical implementation.",
        "phrases": {
            "essence": "bringing intuitive vision into highly structured form",
            "strength": "translating creative inspiration into robust practical systems",
            "challenge": "maintaining creative flow while implementing detailed structure",
            "approach": "You allow intuition to guide direction, then apply methodical processes to manifest the vision.",
            "growth": "developing even greater harmony between your intuitive and highly structured aspects"
        }
    },
    "fluid-structured": {
        "name": "Intuitive Implementer",
        "description": "You begin with feeling and shape it into form. Vision comes first, but you know how to follow through. Your intuition guides the what, and your structured self handles the how. You're the bridge between idea and action.",
        "phrases": {
            "essence": "intuitive creation supported by practical implementation",
            "strength": "receiving inspired guidance and bringing it into tangible form",
            "challenge": "maintaining the essence of inspiration during the structuring process",
            "approach": "You allow intuition to lead while developing supportive structures to manifest what emerges.",
            "growth": "refining your ability to preserve inspired energy throughout implementation"
        }
    },
    "fluid-balanced": {
        "name": "Visionary Harmonizer",
        "description": "You lead with intuition but always keep one foot in the world. You're tapped into possibility while still attuned to what's needed now. You dance between realms, receiving, refining, and translating energy into form.",
        "phrases": {
            "essence": "intuitive creation with adaptive implementation",
            "strength": "bringing inspired vision into reality through flexible approaches",
            "challenge": "creating enough structure without limiting intuitive guidance",
            "approach": "You follow intuitive direction while remaining responsive to what emerges during implementation.",
            "growth": "developing greater trust in both your intuitive guidance and adaptive abilities"
        }
    },
    "fluid-fluid": {
        "name": "Quantum Manifestor",
        "description": "You create from the unseen. You don't manifest through steps, you manifest through state. You trust timing, you trust energy, and you trust yourself. Your reality bends in response to your being.",
        "phrases": {
            "essence": "intuitive creation through energetic alignment",
            "strength": "manifesting through resonance rather than linear action",
            "challenge": "bringing inspired visions into tangible form without restricting their essence",
            "approach": "You align your energy with desired outcomes, allowing reality to reshape around your frequency.",
            "growth": "developing greater capacity to maintain your state while engaging with practical reality"
        }
    },
    "fluid-strongly-fluid": {
        "name": "Ethereal Creator",
        "description": "You create almost entirely through energy, resonance, and state. Linear processes feel foreign to your deeply intuitive nature. You manifest by becoming the frequency of what you desire, trusting the universe to rearrange itself accordingly.",
        "phrases": {
            "essence": "creation through pure energetic alignment",
            "strength": "accessing deeply intuitive knowledge and manifesting through resonance",
            "challenge": "translating ethereal awareness into forms others can recognize",
            "approach": "You tune into subtle energetic currents and allow reality to reshape itself around your aligned state.",
            "growth": "developing ways to bridge your profound intuitive process with shared reality"
        }
    },
    "strongly-fluid-strongly-fluid": {
        "name": "Pure Intuitive",
        "description": "You create almost exclusively through energy, frequency, and direct knowing. Conventional manifestation approaches feel unnecessarily complex to your deeply intuitive nature. You access reality at the quantum level, where intention and manifestation are essentially one movement.",
        "phrases": {
            "essence": "creation through profound intuitive knowing",
            "strength": "directly accessing the field where intention and manifestation merge",
            "challenge": "interacting with conventional reality without diminishing your connection to source",
            "approach": "You manifest primarily through being rather than doing, allowing reality to respond to your frequency.",
            "growth": "developing bridges between your quantum awareness and the shared physical world"
        }
    },
    "strongly-fluid-fluid": {
        "name": "Quantum Guide",
        "description": "You access reality at remarkably subtle levels, where intention and manifestation are nearly simultaneous. Your highly developed intuitive abilities allow you to perceive possibilities beyond conventional awareness, bringing through insights that reshape understanding.",
        "phrases": {
            "essence": "creation through profound intuitive connection",
            "strength": "accessing subtle dimensions of reality beyond conventional perception",
            "challenge": "translating multidimensional awareness into linear communication",
            "approach": "You align with the essence of what you wish to create, allowing it to emerge through energetic resonance.",
            "growth": "developing ways to bridge your intuitive knowing with practical implementation"
        }
    },
    "strongly-fluid-balanced": {
        "name": "Visionary Translator",
        "description": "You access profound intuitive wisdom while maintaining enough center to translate it effectively. Your exceptional intuitive abilities perceive beyond conventional reality, while your balanced nature helps bridge these insights to practical application.",
        "phrases": {
            "essence": "bridging profound intuition with practical integration",
            "strength": "translating multidimensional awareness into implementable guidance",
            "challenge": "preserving the depth of intuitive knowledge during the translation process",
            "approach": "You receive intuitive information at deep levels, then work to express it in accessible ways.",
            "growth": "refining your ability to maintain intuitive connection while engaging with conventional reality"
        }
    },
    "strongly-structured-strongly-fluid": {
        "name": "Dimensional Bridge",
        "description": "You embody a remarkable synthesis of seemingly opposite approaches. Your exceptional structure provides tangible form for profound intuitive vision. You excel at creating robust systems that channel ethereal energy, bringing the invisible into powerful manifestation.",
        "phrases": {
            "essence": "bridging profound structure and deep intuition",
            "strength": "creating powerful frameworks that channel intuitive wisdom",
            "challenge": "maintaining the integrity of both structure and intuition simultaneously",
            "approach": "You receive deep intuitive guidance, then apply exceptional organizational skills to bring it into form.",
            "growth": "refining your unique ability to honor both highly structured and deeply intuitive aspects"
        }
    }
}

IDEAL_APPROACHES = {
    "structured-structured": {
        "strengths": "clarity, precision, consistency, methodical execution",
        "approaches": [
            "Map out your manifestation path with solid plans and measurable milestones.",
            "Use visualizations that are crystal-clear and time-specific.",
            "Track your momentum like data, watch what builds, what stalls.",
            "Lock in routines that nourish your focus and energy.",
            "Apply logic to dissolve and learn from tension, don't just push through it."
        ]
    },
    "structured-balanced": {
        "strengths": "strategic flexibility, practical intuition, organized adaptability",
        "approaches": [
            "Build frameworks with clear priorities that can flex without falling apart.",
            "Move between planned structure and intuitive adjustments as needed.",
            "Trust data, but leave the door open for synchronicity.",
            "Keep consistent anchors while letting inspiration surprise you.",
            "Know when to pause, reflect, and let new clarity rise."
        ]
    },
    "structured-fluid": {
        "strengths": "stable foundations, intuitive expansion, creative implementation",
        "approaches": [
            "Set clear intentions and let them stretch in unexpected directions.",
            "Work in focused bursts, then unplug to receive what's next.",
            "Ground intuitive downloads through systems that support, not suffocate.",
            "Design rituals or systems that hold space for inspiration to land.",
            "Mix planning with energetic alignment like you're mixing paint."
        ]
    },
    "balanced-structured": {
        "strengths": "adaptable systems, harmonized logic, grounded creativity",
        "approaches": [
            "Adopt structures, adaptable frameworks that flow, not tight rope, more riverbank.",
            "Anchor consistent practices while evolving them over time.",
            "Let logic and intuition co-pilot your moves.",
            "Build systems that leave room for breath and built-in flexibility.",
            "Shift between doing phases and integrating alignment-focused phases."
        ]
    },
    "balanced-balanced": {
        "strengths": "dynamic integration, responsive flow, holistic creation",
        "approaches": [
            "Shape-shift between structure and intuition based on the moment.",
            "Blend the practical and the energetic in equal measure.",
            "Let your process evolve without losing its core.",
            "Plan without control, allow without passivity.",
            "Use both data and energy as feedback, not rules."
        ]
    },
    "balanced-fluid": {
        "strengths": "intuitive adaptability, emotional attunement, expansive grounding",
        "approaches": [
            "Let inner guidance lead, while maintaining practical grounding.",
            "Build soft structures that hold space, not pressure.",
            "Translate vision into action one inspired step at a time.",
            "Use intuitive practices with consistent implementation.",
            "Alternate between expansive exploration and focused integration."
        ]
    },
    "fluid-structured": {
        "strengths": "intuitive discipline, inspired planning, grounded magic",
        "approaches": [
            "Let your inner knowing set the vision, then map the moves.",
            "Create gentle structures that support your visionary nature.",
            "Let energy speak first, then act from that clarity.",
            "Use intuitive guidance to inform strategic planning.",
            "Implement consistent practices that honor your need for creative freedom."
        ]
    },
    "fluid-balanced": {
        "strengths": "vision-driven harmony, adaptable flow, creative balance",
        "approaches": [
            "Stay wide open to inspiration while maintaining practical awareness.",
            "Use minimal structures as soft landing pads for ideas.",
            "Ground your wildness in presence, not pressure.",
            "Notice when it's time for expansive vision and when it's time for focused execution.",
            "Let intuitive bursts be followed by integration, instead of cyclical thinking."
        ]
    },
    "fluid-fluid": {
        "strengths": "energetic alignment, nonlinear magic, deep trust in the unseen",
        "approaches": [
            "Make alignment your method. Vibe first, results second.",
            "Visualize from feeling, not from steps.",
            "Create from the pulse of inspiration, not the plan.",
            "Let synchronicity lead, you don't have to force it.",
            "Bring in structure only as a channel, never as a cage."
        ]
    },
    # Add fallbacks for combinations involving strongly-*
    "strongly-structured-strongly-structured": {
        "strengths": "clarity, precision, consistency, methodical execution",
        "approaches": [
            "Map out your manifestation path with solid plans and measurable milestones.",
            "Use visualizations that are crystal-clear and time-specific.",
            "Track your momentum like data, watch what builds, what stalls.",
            "Lock in routines that nourish your focus and energy.",
            "Apply logic to dissolve and learn from tension, don't just push through it."
        ]
    },
     "strongly-structured-structured": {
        "strengths": "clarity, precision, consistency, methodical execution",
        "approaches": [
            "Map out your manifestation path with solid plans and measurable milestones.",
            "Use visualizations that are crystal-clear and time-specific.",
            "Track your momentum like data, watch what builds, what stalls.",
            "Lock in routines that nourish your focus and energy.",
            "Apply logic to dissolve and learn from tension, don't just push through it."
        ]
    },
    "strongly-structured-balanced": {
        "strengths": "strategic flexibility, practical intuition, organized adaptability",
        "approaches": [
            "Build frameworks with clear priorities that can flex without falling apart.",
            "Move between planned structure and intuitive adjustments as needed.",
            "Trust data, but leave the door open for synchronicity.",
            "Keep consistent anchors while letting inspiration surprise you.",
            "Know when to pause, reflect, and let new clarity rise."
        ]
    },
    "strongly-structured-fluid": {
        "strengths": "stable foundations, intuitive expansion, creative implementation",
        "approaches": [
            "Set clear intentions and let them stretch in unexpected directions.",
            "Work in focused bursts, then unplug to receive what's next.",
            "Ground intuitive downloads through systems that support, not suffocate.",
            "Design rituals or systems that hold space for inspiration to land.",
            "Mix planning with energetic alignment like you're mixing paint."
        ]
    },
     "strongly-structured-strongly-fluid": {
        "strengths": "stable foundations, intuitive expansion, creative implementation",
        "approaches": [
            "Set clear intentions and let them stretch in unexpected directions.",
            "Work in focused bursts, then unplug to receive what's next.",
            "Ground intuitive downloads through systems that support, not suffocate.",
            "Design rituals or systems that hold space for inspiration to land.",
            "Mix planning with energetic alignment like you're mixing paint."
        ]
    },
    "balanced-strongly-structured": {
         "strengths": "adaptable systems, harmonized logic, grounded creativity",
        "approaches": [
            "Adopt structures, adaptable frameworks that flow, not tight rope, more riverbank.",
            "Anchor consistent practices while evolving them over time.",
            "Let logic and intuition co-pilot your moves.",
            "Build systems that leave room for breath and built-in flexibility.",
            "Shift between doing phases and integrating alignment-focused phases."
        ]
    },
    "balanced-strongly-fluid": {
        "strengths": "intuitive adaptability, emotional attunement, expansive grounding",
        "approaches": [
            "Let inner guidance lead, while maintaining practical grounding.",
            "Build soft structures that hold space, not pressure.",
            "Translate vision into action one inspired step at a time.",
            "Use intuitive practices with consistent implementation.",
            "Alternate between expansive exploration and focused integration."
        ]
    },
    "fluid-strongly-structured": {
        "strengths": "intuitive discipline, inspired planning, grounded magic",
        "approaches": [
            "Let your inner knowing set the vision, then map the moves.",
            "Create gentle structures that support your visionary nature.",
            "Let energy speak first, then act from that clarity.",
            "Use intuitive guidance to inform strategic planning.",
            "Implement consistent practices that honor your need for creative freedom."
        ]
    },
    "fluid-strongly-fluid": {
        "strengths": "energetic alignment, nonlinear magic, deep trust in the unseen",
        "approaches": [
            "Make alignment your method. Vibe first, results second.",
            "Visualize from feeling, not from steps.",
            "Create from the pulse of inspiration, not the plan.",
            "Let synchronicity lead, you don't have to force it.",
            "Bring in structure only as a channel, never as a cage."
        ]
    },
    "strongly-fluid-strongly-fluid": {
        "strengths": "energetic alignment, nonlinear magic, deep trust in the unseen",
        "approaches": [
            "Make alignment your method. Vibe first, results second.",
            "Visualize from feeling, not from steps.",
            "Create from the pulse of inspiration, not the plan.",
            "Let synchronicity lead, you don't have to force it.",
            "Bring in structure only as a channel, never as a cage."
        ]
    },
    "strongly-fluid-fluid": {
        "strengths": "energetic alignment, nonlinear magic, deep trust in the unseen",
        "approaches": [
            "Make alignment your method. Vibe first, results second.",
            "Visualize from feeling, not from steps.",
            "Create from the pulse of inspiration, not the plan.",
            "Let synchronicity lead, you don't have to force it.",
            "Bring in structure only as a channel, never as a cage."
        ]
    },
    "strongly-fluid-balanced": {
        "strengths": "intuitive adaptability, emotional attunement, expansive grounding",
        "approaches": [
            "Let inner guidance lead, while maintaining practical grounding.",
            "Build soft structures that hold space, not pressure.",
            "Translate vision into action one inspired step at a time.",
            "Use intuitive practices with consistent implementation.",
            "Alternate between expansive exploration and focused integration."
        ]
    }
}

COMMON_MISALIGNMENTS = {
    "structured-structured": [
        "Getting so locked into the plan that you leave no room for surprise",
        "Waiting for the \"perfect\" setup instead of moving with what you have",
        "Ignoring your intuition when it doesn't show up in a spreadsheet",
        "Getting discouraged when outcomes don't match the timeline you imagined",
        "Focusing too much on the method and forgetting the why"
    ],
    "structured-balanced": [
        "Reaching for structure when the moment is asking for softness",
        "Overthinking intuitive nudges until they lose their spark",
        "Building systems that start helpful and end up restrictive",
        "Getting impatient with things that unfold outside your control",
        "Writing off intuitive or nonlinear methods as unreliable"
    ],
    "structured-fluid": [
        "Trying to organize inspiration before it fully arrives",
        "Overriding your gut with what seems \"smarter\" on paper",
        "Creating rigid plans that don't leave room for breath",
        "Resisting surrender because it feels like giving up control",
        "Dismissing energy work or subtle shifts as not \"real\" progress"
    ],
    "balanced-structured": [
        "Defaulting to logic even when the call is emotional or energetic",
        "Imposing unnecessary systems just to feel safe",
        "Measuring your worth by results instead of resonance",
        "Doubting your own intuition if it can't be explained",
        "Resisting ease, mistaking it for inconsistency"
    ],
    "balanced-balanced": [
        "Second-guessing yourself when there's no clear \"right\" path",
        "Trying to include every approach and diluting your power",
        "Spreading energy too thin without anchoring it somewhere",
        "Switching gears too often to let anything root",
        "Avoiding full commitment in either direction, structure or surrender"
    ],
    "balanced-fluid": [
        "Staying in the dreamspace when action would actually help",
        "Losing your grounding in the swirl of possibility",
        "Avoiding structure because it feels like restriction",
        "Floating through phases that need some intentional rooting",
        "Ignoring data or plans that could strengthen your magic"
    ],
    "fluid-structured": [
        "Building cages when you meant to build containers",
        "Talking yourself out of intuitive truths if they sound too \"out there\"",
        "Prioritizing execution over alignment and burning out",
        "Rushing timelines instead of trusting divine pacing",
        "Downplaying your own vibrational work like it's not enough"
    ],
    "fluid-balanced": [
        "Resisting even the gentle structure that could support you",
        "Scattering your focus when it wants to be held",
        "Ignoring insights that could bring your vision into reality",
        "Walking away from ideas just when they're ready to root",
        "Mistaking flexibility for directionlessness"
    ],
    "fluid-fluid": [
        "Staying in the ethers without grounding your vision into form",
        "Letting ideas swirl endlessly without choosing one to land",
        "Avoiding tangible action that could magnetize what you want",
        "Starting and stopping when consistency wants to emerge",
        "Letting clarity remain optional when it's trying to knock on your door"
    ],
     # Add fallbacks for combinations involving strongly-*
    "strongly-structured-strongly-structured": [
        "Getting so locked into the plan that you leave no room for surprise",
        "Waiting for the \"perfect\" setup instead of moving with what you have",
        "Ignoring your intuition when it doesn't show up in a spreadsheet",
        "Getting discouraged when outcomes don't match the timeline you imagined",
        "Focusing too much on the method and forgetting the why"
    ],
     "strongly-structured-structured": [
        "Getting so locked into the plan that you leave no room for surprise",
        "Waiting for the \"perfect\" setup instead of moving with what you have",
        "Ignoring your intuition when it doesn't show up in a spreadsheet",
        "Getting discouraged when outcomes don't match the timeline you imagined",
        "Focusing too much on the method and forgetting the why"
    ],
    "strongly-structured-balanced": [
        "Reaching for structure when the moment is asking for softness",
        "Overthinking intuitive nudges until they lose their spark",
        "Building systems that start helpful and end up restrictive",
        "Getting impatient with things that unfold outside your control",
        "Writing off intuitive or nonlinear methods as unreliable"
    ],
    "strongly-structured-fluid": [
        "Trying to organize inspiration before it fully arrives",
        "Overriding your gut with what seems \"smarter\" on paper",
        "Creating rigid plans that don't leave room for breath",
        "Resisting surrender because it feels like giving up control",
        "Dismissing energy work or subtle shifts as not \"real\" progress"
    ],
     "strongly-structured-strongly-fluid": [
        "Trying to organize inspiration before it fully arrives",
        "Overriding your gut with what seems \"smarter\" on paper",
        "Creating rigid plans that don't leave room for breath",
        "Resisting surrender because it feels like giving up control",
        "Dismissing energy work or subtle shifts as not \"real\" progress"
    ],
    "balanced-strongly-structured": [
        "Defaulting to logic even when the call is emotional or energetic",
        "Imposing unnecessary systems just to feel safe",
        "Measuring your worth by results instead of resonance",
        "Doubting your own intuition if it can't be explained",
        "Resisting ease, mistaking it for inconsistency"
    ],
    "balanced-strongly-fluid": [
        "Staying in the dreamspace when action would actually help",
        "Losing your grounding in the swirl of possibility",
        "Avoiding structure because it feels like restriction",
        "Floating through phases that need some intentional rooting",
        "Ignoring data or plans that could strengthen your magic"
    ],
    "fluid-strongly-structured": [
        "Building cages when you meant to build containers",
        "Talking yourself out of intuitive truths if they sound too \"out there\"",
        "Prioritizing execution over alignment and burning out",
        "Rushing timelines instead of trusting divine pacing",
        "Downplaying your own vibrational work like it's not enough"
    ],
    "fluid-strongly-fluid": [
        "Staying in the ethers without grounding your vision into form",
        "Letting ideas swirl endlessly without choosing one to land",
        "Avoiding tangible action that could magnetize what you want",
        "Starting and stopping when consistency wants to emerge",
        "Letting clarity remain optional when it's trying to knock on your door"
    ],
    "strongly-fluid-strongly-fluid": [
        "Staying in the ethers without grounding your vision into form",
        "Letting ideas swirl endlessly without choosing one to land",
        "Avoiding tangible action that could magnetize what you want",
        "Starting and stopping when consistency wants to emerge",
        "Letting clarity remain optional when it's trying to knock on your door"
    ],
    "strongly-fluid-fluid": [
        "Staying in the ethers without grounding your vision into form",
        "Letting ideas swirl endlessly without choosing one to land",
        "Avoiding tangible action that could magnetize what you want",
        "Starting and stopping when consistency wants to emerge",
        "Letting clarity remain optional when it's trying to knock on your door"
    ],
    "strongly-fluid-balanced": [
        "Staying in the dreamspace when action would actually help",
        "Losing your grounding in the swirl of possibility",
        "Avoiding structure because it feels like restriction",
        "Floating through phases that need some intentional rooting",
        "Ignoring data or plans that could strengthen your magic"
    ]
}

MASTERY_TRAIT_MAP = {
  "clarity":        { "code": "C", "label": "Clarity-driven" },
  "momentum":       { "code": "M", "label": "Momentum-supported" },
  "stability":      { "code": "SF", "label": "Stability-focused" },
  "resonance":      { "code": "S", "label": "Sensationally-attuned" },
  "freedom":        { "code": "AD", "label": "Autonomy-driven" },
  "structure":      { "code": "SI", "label": "Structure-informed" },
  "receptivity":    { "code": "RP", "label": "Receptivity-powered" },
  "adaptability":   { "code": "A", "label": "Adaptability-based" }
}

DOMINANT_VALUE_TO_TRAIT_KEY_MAP = {
  # Core Priorities
  "creative-expression": "resonance",
  "financial-abundance": "stability",
  "emotional-fulfillment": "resonance",
  "personal-autonomy": "freedom",
  "personal-freedom": "freedom",
  "deep-relationships": "resonance",
  "spiritual-connection": "receptivity",
  "craft-mastery": "structure",
  "wealth-security": "stability",
  "emotional-peace": "stability",
  "deep-connection": "resonance",
  "higher-meaning": "resonance", # Could also map to receptivity depending on nuance
  "confidence-trust": "clarity", # Could also map to structure
  "peace-ease": "stability",
  "choice-autonomy": "freedom",
  "stability-security": "stability",
  "passion-inspiration": "resonance", # Could also map to momentum
  "joy-excitement": "resonance", # Could also map to momentum

  # Alignment Needs
  "accept-cycles": "adaptability",
  "accept-structure": "structure",
  "accept-emotions": "resonance",
  "accept-gradual-clarity": "adaptability",
  "accept-intuition": "receptivity",
  "accept-flexibility": "adaptability",
  "control-outcomes": "structure",
  "control-emotions": "structure",
  "control-consistency": "structure",
  "control-clarity": "clarity",
  "control-decisions": "clarity",
  "control-intuition": "clarity", # Could argue structure too
  "let-go-control": "adaptability", # Added based on context
  "follow-inner-signal": "receptivity" # Added based on context
}

# --- Helper Functions ---

def _map_dominant_value_to_trait_key(dominant_value: str) -> Optional[str]:
    """Maps a dominant value from mastery profile to a trait key."""
    return DOMINANT_VALUE_TO_TRAIT_KEY_MAP.get(dominant_value)

def _generate_subtype(mastery_profile: Dict[str, str]) -> Optional[str]:
    """Generates a subtype string based on mastery profile traits."""
    primary_trait_key = mastery_profile.get('corePriority')
    secondary_trait_key = mastery_profile.get('alignmentNeed')

    primary = MASTERY_TRAIT_MAP.get(primary_trait_key) if primary_trait_key else None
    secondary = MASTERY_TRAIT_MAP.get(secondary_trait_key) if secondary_trait_key else None

    if not primary or not secondary:
        logger.warning(f"Could not generate subtype, missing trait data for: {mastery_profile}")
        return None

    if primary['code'] == secondary['code']:
        return f"Subtype: [{primary['code']}] (Primarily {primary['label']})"
    else:
        code = f"[{primary['code']}-{secondary['code']}]"
        label = f"({primary['label']}, {secondary['label']})"
        return f"Subtype: {code} {label}"

def _generate_typology_shifts(typology_key: str, growth_areas: List[str]) -> List[str]:
    """Generates list of suggested shifts based on typology and growth areas."""
    type_shifts = {
        "structured-structured": [
            "Make more space for your intuition to speak, even if it whispers at first.",
            "Set aside analytical thinking now and then, even 10 minutes of inner listening matters.",
            "Learn to sense when structure is serving, and when it's stalling the process."
        ],
        "structured-balanced": [
            "Notice when you're defaulting to structure out of habit, not alignment.",
            "Let yourself trust the part of you that knows when to plan and when to flow.",
            "Design containers that move with you, they don't have to stay rigid to be effective."
        ],
        "structured-fluid": [
            "Let structure support your intuition, not silence it.",
            "Create simple, open systems that hold ideas without analyzing them to death.",
            "Let your creativity stretch out before you try to shape it."
        ],
        "balanced-structured": [
            "Pay attention to when structure is helpful, and when it becomes armor.",
            "Build systems that adapt as you do, structure isn't meant to trap you.",
            "Let yourself act on nudges that don't always make logical sense."
        ],
        "balanced-balanced": [
            "Don't overthink your process. Let it be messy sometimes.",
            "You don't need a perfect plan. You already have the tools, use what's here.",
            "When it's time to choose, listen to both mind and body. Then move."
        ],
        "balanced-fluid": [
            "Honor your flow, but don't fear the structure that could support it.",
            "A little bit of grounding can help your creativity land.",
            "Make space for alignment, but also make space for follow-through."
        ],
        "fluid-structured": [
            "Building cages when you meant to build containers.", # This seems like a misalignment, maybe "Build containers, not cages."?
            "Let structure become a bridge, not a barricade.",
            "Let your structured side serve the vision, not try to control it.",
            "Use grounding rituals when your energy starts to float too far out."
        ],
        "fluid-balanced": [
            "Keep leading with your intuition, but let the practical side catch up.",
            "Create space for the vision to flow, then anchor it.",
            "Let your balanced nature turn inspiration into something you can touch."
        ],
        "fluid-fluid": [
            "Give your energy a place to land, a little structure goes a long way.",
            "Set aside time to anchor your dreams into form.",
            "Remember: consistency doesn't kill your magic, it channels it."
        ],
         # Add fallbacks for strongly-* combinations
        "strongly-structured-strongly-structured": type_shifts["structured-structured"],
        "strongly-structured-structured": type_shifts["structured-structured"],
        "strongly-structured-balanced": type_shifts["structured-balanced"],
        "strongly-structured-fluid": type_shifts["structured-fluid"],
        "strongly-structured-strongly-fluid": type_shifts["structured-fluid"], # Or maybe structured-structured? Needs review
        "balanced-strongly-structured": type_shifts["balanced-structured"],
        "balanced-strongly-fluid": type_shifts["balanced-fluid"],
        "fluid-strongly-structured": type_shifts["fluid-structured"],
        "fluid-strongly-fluid": type_shifts["fluid-fluid"],
        "strongly-fluid-strongly-fluid": type_shifts["fluid-fluid"],
        "strongly-fluid-fluid": type_shifts["fluid-fluid"],
        "strongly-fluid-balanced": type_shifts["balanced-fluid"],
    }

    shifts = list(type_shifts.get(typology_key, type_shifts["fluid-structured"])) # Defaulting

    # Add shifts based on growth areas
    if 'consistency-challenge' in growth_areas:
        shifts.append('Create a rhythm that fits you, not one that fights you. Let your consistency be flexible, alive, something that honors your cycles while still building momentum.')
    if 'clarity-challenge' in growth_areas:
        shifts.append('Blend reflection with inner listening. Let clarity come in layers, not lightning bolts. You\'re not behind for not knowing yet, you\'re in conversation.')
    if 'action-challenge' in growth_areas or 'action-gap' in growth_areas:
        shifts.append('Align your action style with your natural energy. Some seasons are built for bursts, others for steady build. Know your wave, then ride it.')
    if 'intuition-challenge' in growth_areas or 'self-trust-resistance' in growth_areas:
        shifts.append('Practice tuning in without immediately needing proof. Your knowing is valid, let it lead sometimes, even when it\'s quiet.')
    if 'emotion-challenge' in growth_areas or 'emotional-block' in growth_areas:
        shifts.append('Give your emotions space without needing to fix or flatten them. Feeling deeply doesn\'t block creation, it is part of creation.')
    if 'receiving-challenge' in growth_areas:
        shifts.append('Create rituals that help you open, soften, notice. Sometimes what you asked for is already arriving, just not in the costume you expected.')
    if 'decision-doubt' in growth_areas:
        shifts.append('Design a decision-making style that blends analysis with intuition. Let both voices speak, then choose with your whole body.')
    if 'focus-challenge' in growth_areas:
        shifts.append('Make containers for your attention that work with, not against, your nature. Structure doesn\'t have to be strict, it can be sacred.')
    if 'burnout-pattern' in growth_areas:
        shifts.append('Honor your ebb as much as your flow. Sustainable energy doesn\'t come from pushing, it comes from rhythm, rest, and return.')
    if 'commitment-hesitation' in growth_areas:
        shifts.append('Find ways to commit gently, piece by piece, instead of forcing yourself to leap. You can go all in without abandoning yourself.')
    if 'risk-resistance' in growth_areas:
         shifts.append('You\'re getting more comfortable in the unknown, letting desire lead, even when the outcome isn\'t guaranteed.')
    if 'emotional-expression-resistance' in growth_areas:
         shifts.append('You\'re allowing more of your emotional truth to surface, without needing it to be tidy or justified.')
    if 'vision-clarity-resistance' in growth_areas:
         shifts.append('You\'re learning to hold a vision without fearing the specificity, letting clarity feel like empowerment, not pressure.')
    if 'momentum-resistance' in growth_areas:
         shifts.append('You\'re discovering how to stay in motion, not from force, but from flow that builds on itself.')
    if 'control-resistance' in growth_areas:
         shifts.append('You\'re softening your grip, letting go of the need to manage every outcome, and learning to co-create with life.')


    return shifts

def _generate_acceptance_permissions(alignment_needs: List[str], typology_key: str) -> List[str]:
    """Generates list of acceptance permissions."""
    type_permissions = {
        "structured-structured": [
            "You're allowed to not know. Let uncertainty be part of the process, not something to fix.",
            "You're allowed to trust your intuition, even if it doesn't match the data.",
            "You're allowed to pivot when inspiration pulls you somewhere new."
        ],
        "structured-balanced": [
            "You're allowed to sense when it's time to loosen the plan.",
            "You're allowed to be a blend, you don't have to pick a side.",
            "You're allowed to shift your systems as you grow."
        ],
        "structured-fluid": [
            "You're allowed to want structure and still be deeply intuitive.",
            "You're allowed to design containers that bend with you.",
            "You're allowed to trust timing over timelines."
        ],
        "balanced-structured": [
            "You're allowed to change your process without calling it inconsistency.",
            "You're allowed to lean into structure when it actually serves you.",
            "You're allowed to work with both logic and feeling, they're not in conflict."
        ],
        "balanced-balanced": [
            "You're allowed to not commit to one way of being.",
            "You're allowed to integrate without choosing a single lane.",
            "You're allowed to be the bridge, the in-between is a real place."
        ],
        "balanced-fluid": [
            "You're allowed to lead with your intuition and still be supported.",
            "You're allowed to follow what lights you up without abandoning the real world.",
            "You're allowed to hold freedom and form in the same breath."
        ],
        "fluid-structured": [
            "You're allowed to trust your knowing first, logic can catch up later.",
            "You're allowed to follow intuitive hits even when they don't make 'sense.'",
            "You're allowed to move without explaining every step."
        ],
        "fluid-balanced": [
            "You're allowed to let inspiration lead and structure follow.",
            "You're allowed to hold your big vision and still build it piece by piece.",
            "You're allowed to switch gears, dream, plan, dream again."
        ],
        "fluid-fluid": [
            "You're allowed to trust your way, even if it looks nothing like theirs.",
            "You're allowed to create how you create, it doesn't have to be conventional.",
            "You're allowed to follow energy instead of plans."
        ],
         # Add fallbacks for strongly-* combinations
        "strongly-structured-strongly-structured": type_permissions["structured-structured"],
        "strongly-structured-structured": type_permissions["structured-structured"],
        "strongly-structured-balanced": type_permissions["structured-balanced"],
        "strongly-structured-fluid": type_permissions["structured-fluid"],
        "strongly-structured-strongly-fluid": type_permissions["structured-fluid"], # Needs review
        "balanced-strongly-structured": type_permissions["balanced-structured"],
        "balanced-strongly-fluid": type_permissions["balanced-fluid"],
        "fluid-strongly-structured": type_permissions["fluid-structured"],
        "fluid-strongly-fluid": type_permissions["fluid-fluid"],
        "strongly-fluid-strongly-fluid": type_permissions["fluid-fluid"],
        "strongly-fluid-fluid": type_permissions["fluid-fluid"],
        "strongly-fluid-balanced": type_permissions["balanced-fluid"],
    }

    permissions = list(type_permissions.get(typology_key, type_permissions["fluid-structured"])) # Defaulting

    # Add permissions based on alignment needs
    if 'accept-cycles' in alignment_needs:
        permissions.append('You\'re allowed to move in waves. Constant output is not a requirement for worth.')
    if 'accept-structure' in alignment_needs:
        permissions.append('You\'re allowed to build what supports you, even if it\'s more structure than others need.')
    if 'accept-emotions' in alignment_needs:
        permissions.append('You\'re allowed to feel what you feel, no fixing, just listening.')
    if 'accept-gradual-clarity' in alignment_needs:
        permissions.append('You\'re allowed to let clarity emerge slowly. There\'s no deadline on knowing.')
    if 'accept-intuition' in alignment_needs:
        permissions.append('You\'re allowed to follow a nudge without a logical reason.')
    if 'accept-flexibility' in alignment_needs:
        permissions.append('You\'re allowed to stay open, even if others need things nailed down.')
    if 'control-outcomes' in alignment_needs:
        permissions.append('You\'re allowed to release the "how" and trust what\'s coming.')
    if 'control-emotions' in alignment_needs:
        permissions.append('You\'re allowed to ride the full range of your feelings, they\'re not a flaw.')
    if 'control-consistency' in alignment_needs:
        permissions.append('You\'re allowed to define consistency in a way that fits your rhythm.')
    if 'control-clarity' in alignment_needs:
        permissions.append('You\'re allowed to explore without committing on day one.')
    if 'control-decisions' in alignment_needs:
        permissions.append('You\'re allowed to make choices from both wisdom and wonder.')
    if 'control-intuition' in alignment_needs:
        permissions.append('You\'re allowed to follow your knowing without defending it.')

    return permissions

def _generate_energy_support_tools(alignment_needs: List[str], energy_patterns: List[str], typology_key: str) -> List[str]:
    """Generates list of energy support tools."""
    type_tools = {
         "structured-structured": [
            "Intuition practices with structure, prompts, frameworks, things with clear edges",
            "Systems that flex in small ways without losing integrity",
            "Measurable ways to track inner growth, your version of data, not just numbers",
            "Daily practices like Lectio Divina or structured morning pages to commune with inner clarity",
            "Vedic or Transcendental Meditation to support commitment to inner stillness",
            "Saturn-aligned planning rituals using astrology to give your days cosmic architecture",
            "Box breathing for grounding and Ayurvedic daily routines (Dinacharya) for consistency",
            "Energetic accountability check-ins that keep you in integrity with your sacred timeline",
            "<a href='https://www.peathefeary.com/realization-toolkit-membership-information' target='_blank' class='text-amber-700 hover:underline'>The Refiner</a> tool to fine-tune what's true and release what isn't, honoring your love of measurable growth"
        ],
        "structured-balanced": [
            "Rituals that alternate between planning and play",
            "Decision-making tools that let logic and intuition collaborate",
            "Routines with soft touchpoints for creativity to slip in",
            "Habit stacking aligned with lunar phases to harmonize structure with cosmic rhythm",
            "Tarot journaling interpreted through a logic-meets-intuition lens",
            "Time Blocking infused with intuitive check-ins to stay motivated without overextending",
            "Grounding mantras, color therapy, and EFT tapping to balance drive with surrender",
            "<a href='https://www.peathefeary.com/realization-toolkit-membership-information' target='_blank' class='text-amber-700 hover:underline'>The Refiner</a> tool to fine-tune what's true and release what isn't, honoring evolution with mystical precision"
        ],
        "structured-fluid": [
            "Containers for intuition that don't box it in",
            "Practices that turn sparks of insight into something real",
            "Grounding rituals that hold your energy without muting it",
            "Vision boards inside intentional containers where inspiration can land and stay",
            "Morning energy mapping and chakra intentions",
            "40-day Kundalini kriya cycles to let energy flow inside a devotional rhythm",
            "Calendared \"download\" sessions and breath-led intuitive writing",
            "Themed movement rituals (like \"Fire Fridays\")",
            "<a href='https://www.peathefeary.com/timeline-jumping' target='_blank' class='text-amber-700 hover:underline'>The Timeline Jumping Audio</a> to give your intuitive leaps a structure without clipping your wings"
        ],
        "balanced-structured": [
            "Systems that shift with your seasons",
            "Tools that help you feel when to lean into structure or soften into flow",
            "Integration practices that hold both strategy and soul",
            "Seasonal rituals like solstice ceremonies or equinox resets",
            "Blending muscle testing or pendulum work with rational insight for decision-making",
            "Gentle and adaptive morning routines with mood-matching journal prompts",
            "Grounding walks to connect with earth",
            "Weekly reflections paired with soul-led planning for steady blooming",
            "<a href='https://www.peathefeary.com/realization-toolkit-membership-information' target='_blank' class='text-amber-700 hover:underline'>Bankroll Spiral</a> to support your unfolding by bringing grace to wealth and worth expansion"
        ],
        "balanced-balanced": [
            "Frameworks for weaving different modalities into a whole",
            "Tools that help you feel what's needed now, not what should work",
            "Regular check-ins to make space for inner recalibration",
            "Oracle cards or intuitive body scans for judgment-free check-ins",
            "Altars across modalities, combining breathwork with sound therapy",
            "Dream journaling, water rituals, and aromatherapy guided by body wisdom",
            "<a href='https://www.peathefeary.com/realization-toolkit-membership-information' target='_blank' class='text-amber-700 hover:underline'>The Realization Toolkit Membership</a> as a wellspring for your multidimensional nature"
        ],
        "balanced-fluid": [
            "Planning systems that breathe, minimal but supportive",
            "Practices that keep you tethered while you expand",
            "Frameworks for intuitive choices that still lead to grounded outcomes",
            "Softly structured manifestation rituals, especially on new moons",
            "Intuitive dance and minimalistic planning like \"three priorities a day\"",
            "Breath-guided painting and nervous-system-nourishing teas (tulsi, lemon balm)",
            "Minimally guided visualizations",
            "Planning systems that move in circles, not lines",
            "<a href='https://www.peathefeary.com/what-is-rich-terrain' target='_blank' class='text-amber-700 hover:underline'>Rich Terrain</a> to help you plant and prune in rhythm with your own seasons"
        ],
        "fluid-structured": [
            "Let vision come first, then build the scaffolding to hold it",
            "Intuition check-ins that ground you without overriding your fire",
            "Action steps that flow from energy, not obligation",
            "Scripting and altar ceremonies to name your desires",
            "Subtle scaffolding with intuitive digital tools like Notion",
            "Solar plexus work through breath, walking, and core activation",
            "Batched creative rituals and chakra-based journaling",
            "<a href='https://www.peathefeary.com/shop/p/7eleczgn0uipxp2lwdhkeczqq6l2sb-r2wcr-wd9h5-yhryn' target='_blank' class='text-amber-700 hover:underline'>Decide to Trust (print</a> or mantra) as your compass, reminding that clarity often comes after the leap"
        ],
        "fluid-balanced": [
            "Anchor points for big ideas to start taking form",
            "Loose but reliable manifestation rituals",
            "Practices that translate intuitive hits into doable steps",
            "Rituals tied to your natural energy peaks",
            "Soft manifestation and intuitive weekly check-ins",
            "Morning light exposure to help land in your day",
            "Grounding teas, intuitive sketching, vocal toning, and body-led meditations",
            "Vision maps and circular calendars that hold dreams without rushing them",
            "<a href='https://www.peathefeary.com/classesandaudios/p/timelinejumping' target='_blank' class='text-amber-700 hover:underline'>The Timeline Jumping Embodiment Guide</a> for playful portals into alignment"
        ],
        "fluid-fluid": [
            "Alignment practices that bring your energy into coherence",
            "Gentle grounding tools, just enough to help you land without pinning you down",
            "Navigation practices that help you follow the thread without losing momentum",
            "Energy attunement meditations, fascia release, and ceremonial baths",
            "Dream, moon, and mood tracking",
            "Sound healing through voice, tuning forks, and chimes",
            "Intuitive journaling and breathwork for decision-making",
            "Intuitive time-blocking for momentum without pressure",
            "<a href='https://www.peathefeary.com/spirit-math-store' target='_blank' class='text-amber-700 hover:underline'>Spirit Math</a> as Oracle for riddles, revelations, and resonance without requiring linearity"
        ],
         # Add fallbacks for strongly-* combinations
        "strongly-structured-strongly-structured": type_tools["structured-structured"],
        "strongly-structured-structured": type_tools["structured-structured"],
        "strongly-structured-balanced": type_tools["structured-balanced"],
        "strongly-structured-fluid": type_tools["structured-fluid"],
        "strongly-structured-strongly-fluid": type_tools["structured-fluid"], # Needs review
        "balanced-strongly-structured": type_tools["balanced-structured"],
        "balanced-strongly-fluid": type_tools["balanced-fluid"],
        "fluid-strongly-structured": type_tools["fluid-structured"],
        "fluid-strongly-fluid": type_tools["fluid-fluid"],
        "strongly-fluid-strongly-fluid": type_tools["fluid-fluid"],
        "strongly-fluid-fluid": type_tools["fluid-fluid"],
        "strongly-fluid-balanced": type_tools["balanced-fluid"],
    }

    tools = list(type_tools.get(typology_key, type_tools["fluid-structured"])) # Defaulting

    # Add tools based on energy patterns
    if 'clear-instructions' in energy_patterns:
        tools.append('Templates that provide just enough clarity to get you started, no pressure to follow perfectly')
    if 'intuitive-instincts' in energy_patterns:
        tools.append('Practices that deepen your relationship with your gut knowing, not just to hear it, but to trust it')
    if 'emotional-inspiration' in energy_patterns:
         tools.append('Creative rituals that connect you to feeling states before you begin creating')
    if 'balanced-rhythm' in energy_patterns:
         tools.append('Tools that help you track your energy cycles (like lunar tracking or cycle syncing)')
    if 'gradual-clarity' in energy_patterns:
         tools.append('Mind mapping or vision boarding that allows ideas to cluster and connect over time')
    if 'process-trust' in energy_patterns:
         tools.append('Surrender practices or letting-go rituals to release attachment to specific outcomes')
    if 'rigid-routines' in energy_patterns:
         tools.append('Introduce small variations into your routines to build flexibility')
    if 'ignored-intuition' in energy_patterns:
         tools.append('Daily intuitive check-ins: Ask your body/energy "What\'s needed now?"')
    if 'structured-productivity' in energy_patterns:
         tools.append('Time-blocking or project management tools that align with your structured nature')
    if 'spontaneous-productivity' in energy_patterns:
         tools.append('Keep an "inspiration capture" system handy (notebook, app) for when ideas strike')
    if 'structured-environment' in energy_patterns:
         tools.append('Designate specific, organized spaces for different types of creative work')
    if 'dynamic-environment' in energy_patterns:
         tools.append('Regularly refresh your workspace or change locations to keep energy flowing')

    # Add tools based on alignment needs (if not already covered by typology)
    # Example: If 'accept-cycles' is needed, ensure cycle-tracking tools are suggested
    if 'accept-cycles' in alignment_needs and not any('cycle' in tool.lower() for tool in tools):
         tools.append('Cycle tracking (lunar, menstrual, energetic) to understand your natural waves')
    if 'accept-emotions' in alignment_needs and not any('emotion' in tool.lower() for tool in tools):
         tools.append('Emotional release practices (like journaling, movement, or vocal toning)')

    # Limit the number of tools shown? Or prioritize? For now, return all relevant.
    return tools


# --- Main Result Generation Function ---

def generate_results_text(results_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates all descriptive text sections for the assessment results.

    Args:
        results_data: The complete results dictionary from scorer.generate_complete_results.

    Returns:
        A dictionary containing the generated text sections.
    """
    logger.info("Generating results text...")
    output = {}

    if not results_data or 'typologyPair' not in results_data:
        logger.error("Cannot generate results text: Missing essential results data.")
        return {"error": "Incomplete results data."}

    typology_pair = results_data['typologyPair']
    spectrum_placements = results_data['spectrumPlacements']
    dominant_values = results_data['dominantValues']
    mastery_influences = results_data.get('masteryInfluences', {}) # Get influences if available

    # 1. Generate Dynamic Typology Description
    try:
        dynamic_result = {}
        pair_key = typology_pair.get('key')
        pair_template = TYPOLOGY_PAIRS.get(pair_key)

        if pair_template:
            dynamic_result['typologyName'] = pair_template.get('name', 'Unknown Typology')
            dynamic_result['typologyDescription'] = pair_template.get('description', 'No description available.')
            pair_phrases = pair_template.get('phrases', {})
            dynamic_result['typologyEssence'] = pair_phrases.get('essence', '')
            dynamic_result['typologyApproach'] = pair_phrases.get('approach', '')
            dynamic_result['typologyStrength'] = pair_phrases.get('strength', '')
            dynamic_result['typologyChallenge'] = pair_phrases.get('challenge', '')
            dynamic_result['typologyGrowth'] = pair_phrases.get('growth', '')

            # Get phrases from primary/secondary spectrums
            primary_spec_id = typology_pair.get('primary', {}).get('spectrumId')
            primary_placement = typology_pair.get('primary', {}).get('placement')
            secondary_spec_id = typology_pair.get('secondary', {}).get('spectrumId')
            secondary_placement = typology_pair.get('secondary', {}).get('placement')

            primary_desc_key = f"{primary_spec_id}-{primary_placement}" if primary_spec_id and primary_placement else None
            secondary_desc_key = f"{secondary_spec_id}-{secondary_placement}" if secondary_spec_id and secondary_placement else None

            primary_phrases = TYPOLOGY_DESCRIPTIONS.get(primary_desc_key, {}).get('phrases', {})
            secondary_phrases = TYPOLOGY_DESCRIPTIONS.get(secondary_desc_key, {}).get('phrases', {})

            dynamic_result['primaryIdentity'] = primary_phrases.get('identity', '')
            dynamic_result['secondaryIdentity'] = secondary_phrases.get('identity', '')
            dynamic_result['primaryStrength'] = primary_phrases.get('strength', '')
            dynamic_result['secondaryStrength'] = secondary_phrases.get('strength', '')
            dynamic_result['primaryChallenge'] = primary_phrases.get('challenge', '')
            dynamic_result['secondaryChallenge'] = secondary_phrases.get('challenge', '')

            # Combine into summaries
            dynamic_result['identitySummary'] = f"You are {dynamic_result['primaryIdentity']} with qualities of {dynamic_result['secondaryIdentity']}. Your {dynamic_result['typologyName']} nature gives you a unique perspective on reality creation."
            dynamic_result['strengthSummary'] = f"Your primary strength lies in {dynamic_result['primaryStrength']}, complemented by your ability for {dynamic_result['secondaryStrength']}. This combination enables {dynamic_result['typologyStrength']}."
            dynamic_result['challengeSummary'] = f"Your growth edge involves {dynamic_result['primaryChallenge']} while also navigating {dynamic_result['secondaryChallenge']}. As a {dynamic_result['typologyName']}, your specific challenge is {dynamic_result['typologyChallenge']}."
            dynamic_result['approachSummary'] = f"{dynamic_result['typologyApproach']} This reflects the essence of {dynamic_result['typologyEssence']}."
            dynamic_result['growthSummary'] = f"Your path forward involves {dynamic_result['typologyGrowth']}, which will help you fully embody your unique potential as a {dynamic_result['typologyName']}."

            output['dynamic_typology'] = dynamic_result
        else:
            logger.warning(f"Typology pair template not found for key: {pair_key}")
            output['dynamic_typology'] = {"error": f"Definition not found for typology pair: {pair_key}"}

    except Exception as e:
        logger.error(f"Error generating dynamic typology result: {e}", exc_info=True)
        output['dynamic_typology'] = {"error": "Failed to generate typology description."}


    # 2. Generate Spectrum Placement Descriptions
    try:
        output['spectrum_details'] = {}
        for spectrum_id, placement in spectrum_placements.items():
            desc_key = f"{spectrum_id}-{placement}"
            desc_data = TYPOLOGY_DESCRIPTIONS.get(desc_key)
            if desc_data:
                 output['spectrum_details'][spectrum_id] = {
                     "name": desc_data.get("name"),
                     "description": desc_data.get("description"),
                     "placement": placement # Include the calculated placement
                 }
            else:
                 logger.warning(f"Description not found for spectrum placement: {desc_key}")
                 output['spectrum_details'][spectrum_id] = {
                     "name": spectrum_id.replace('-', ' ').title(), # Fallback name
                     "description": "No specific description available for this placement.",
                     "placement": placement
                 }
    except Exception as e:
        logger.error(f"Error generating spectrum details: {e}", exc_info=True)
        output['spectrum_details'] = {"error": "Failed to generate spectrum details."}


    # 3. Generate Ideal Approaches
    try:
        pair_key = typology_pair.get('key')
        approaches_data = IDEAL_APPROACHES.get(pair_key)
        if approaches_data:
            output['ideal_approaches'] = {
                "strengths": approaches_data.get("strengths"),
                "approaches": approaches_data.get("approaches", [])
            }
        else:
            logger.warning(f"Ideal approaches not found for key: {pair_key}")
            output['ideal_approaches'] = {"error": f"Ideal approaches not found for typology pair: {pair_key}"}
    except Exception as e:
        logger.error(f"Error generating ideal approaches: {e}", exc_info=True)
        output['ideal_approaches'] = {"error": "Failed to generate ideal approaches."}


    # 4. Generate Common Misalignments
    try:
        pair_key = typology_pair.get('key')
        misalignments = COMMON_MISALIGNMENTS.get(pair_key, [])
        output['common_misalignments'] = misalignments
    except Exception as e:
        logger.error(f"Error generating common misalignments: {e}", exc_info=True)
        output['common_misalignments'] = {"error": "Failed to generate common misalignments."}


    # 5. Generate Mastery Priorities & Growth Areas Text
    try:
        output['mastery_priorities'] = []
        output['growth_areas'] = []
        priority_map = { # Copied from JS for direct use here
            'creative-expression': 'You\'re here to create, to birth new things into the world that carry your fingerprint. Expression isn\'t extra, it\'s essential.',
            'financial-abundance': 'You want your life to feel full, supported, resourced, and open. Money, for you, is about spaciousness and choice.',
            'emotional-fulfillment': 'Depth matters. You\'re not built for surface. You want real connection, real feeling, and the emotional truth of things.',
            'personal-autonomy': 'You need room to move. Authority to choose. A path that\'s yours, not someone else\'s map.',
            'deep-relationships': 'Intimacy and belonging ground you. You thrive in connection that sees you clearly and lets you show up whole.',
            'spiritual-connection': 'You feel the pull of something larger, a bigger rhythm, a sacred thread. You move best when you\'re plugged into it.',
            'craft-mastery': 'You value devotion. Getting so close to your craft that it becomes an extension of your being.',
            'wealth-security': 'You want to feel safe inside your life. A sense of grounded resourcing, not just surviving, but held.',
            'emotional-peace': 'You seek inner steadiness, the kind of calm that makes space for everything without being overtaken by anything.',
            'personal-freedom': 'You\'re here to follow your own compass. What matters is that it\'s yours.',
            'deep-connection': 'You want to be seen. Felt. Known. And to offer that same presence in return.',
            'higher-meaning': 'You\'re not just making moves, you\'re making meaning. There has to be a deeper thread running through it connected to the larger whole.',
            'confidence-trust': 'You want to feel yourself as solid, that your knowing can be trusted, that your steps are enough.',
            'peace-ease': 'Ease isn\'t laziness to you, it\'s alignment. You value softness that doesn\'t collapse your power.',
            'choice-autonomy': 'Being able to say yes or no from a rooted place is sacred. You need to feel like you\'re the one driving.',
            'stability-security': 'You\'re not afraid of change, but you want your foundation to hold while it happens.',
            'passion-inspiration': 'You move through desire. Aliveness fuels your work, when you\'re inspired, everything flows better.',
            'joy-excitement': 'You\'re here for pleasure. You want your life to feel good, to make space for deliciousness, for fun, for full-bodied yum.'
        }
        growth_map = { # Copied from JS
            'consistency-challenge': 'You\'re learning how to build rhythm that doesn\'t drain you, consistency that comes from alignment, not force.',
            'clarity-challenge': 'You\'re in the process of tuning in to what you really want, beneath the noise, beyond the scripts.',
            'action-challenge': 'You\'re learning how to move with your energy, how to take steps that feel alive instead of obligatory.',
            'intuition-challenge': 'You\'re building trust in your inner knowing, letting it lead, even when the path doesn\'t look linear.',
            'emotion-challenge': 'You\'re expanding your capacity to feel without being swept away, using emotion as signal, not sabotage.',
            'receiving-challenge': 'You\'re opening to being met, to letting what you\'ve called in actually arrive and be received.',
            'decision-doubt': 'You\'re strengthening your ability to choose, to trust that your inner compass is valid and enough.',
            'action-gap': 'You can see the vision, now you\'re bridging the space between knowing and doing.',
            'focus-challenge': 'You\'re practicing how to stay with something, not through pressure, but devotion.',
            'emotional-block': 'You\'re untangling stories that once protected you, but now hold you back, and making space for something new.',
            'burnout-pattern': 'You\'re learning how to move sustainably, honoring your cycles of output, rest, and restoration.',
            'commitment-hesitation': 'You\'re playing with what it means to go all in, not from urgency, but from inner yes.',
            'self-trust-resistance': 'You\'re rebuilding the bridge between you and your knowing, letting self-trust become your default.',
            'risk-resistance': 'You\'re getting more comfortable in the unknown, letting desire lead, even when the outcome isn\'t guaranteed.',
            'emotional-expression-resistance': 'You\'re allowing more of your emotional truth to surface, without needing it to be tidy or justified.',
            'vision-clarity-resistance': 'You\'re learning to hold a vision without fearing the specificity, letting clarity feel like empowerment, not pressure.',
            'momentum-resistance': 'You\'re discovering how to stay in motion, not from force, but from flow that builds on itself.',
            'control-resistance': 'You\'re softening your grip, letting go of the need to manage every outcome, and learning to co-create with life.'
        }
        for value in dominant_values.get('corePriorities', []):
            output['mastery_priorities'].append(priority_map.get(value, f"Priority: {value}"))
        for value in dominant_values.get('growthAreas', []):
            output['growth_areas'].append(growth_map.get(value, f"Growth Area: {value}"))
    except Exception as e:
        logger.error(f"Error generating mastery/growth text: {e}", exc_info=True)
        output['mastery_priorities'] = ["Error generating priorities."]
        output['growth_areas'] = ["Error generating growth areas."]


    # 6. Generate Strategy Sections (Shifts, Permissions, Tools)
    try:
        pair_key = typology_pair.get('key')
        growth = dominant_values.get('growthAreas', [])
        alignment = dominant_values.get('alignmentNeeds', [])
        energy = dominant_values.get('energyPatterns', [])

        output['strategy_shifts'] = _generate_typology_shifts(pair_key, growth)
        output['strategy_permissions'] = _generate_acceptance_permissions(alignment, pair_key)
        output['strategy_tools'] = _generate_energy_support_tools(alignment, energy, pair_key)
    except Exception as e:
        logger.error(f"Error generating strategy sections: {e}", exc_info=True)
        output['strategy_shifts'] = ["Error generating shifts."]
        output['strategy_permissions'] = ["Error generating permissions."]
        output['strategy_tools'] = ["Error generating tools."]

    # 7. Add Subtype
    try:
        core_priority_key = _map_dominant_value_to_trait_key(dominant_values.get('corePriorities', [None])[0])
        alignment_need_key = _map_dominant_value_to_trait_key(dominant_values.get('alignmentNeeds', [None])[0])
        if core_priority_key and alignment_need_key:
             mastery_profile = {'corePriority': core_priority_key, 'alignmentNeed': alignment_need_key}
             output['subtype'] = _generate_subtype(mastery_profile)
        else:
             output['subtype'] = None
    except Exception as e:
        logger.error(f"Error generating subtype: {e}", exc_info=True)
        output['subtype'] = None


    logger.info("Finished generating results text.")
    return output

# Example Usage (for testing purposes)
if __name__ == '__main__':
    # Import scorer functions if running this file directly
    from scorer import generate_complete_results

    # Sample responses (same as in scorer.py example)
    test_typology_responses = {
        'cognitive-q1': 'left', 'cognitive-q2': 'balanced',
        'perceptual-q1': 'right', 'perceptual-q2': 'right',
        'kinetic-q1': 'left', 'kinetic-q2': 'left',
        'choice-q1': 'balanced', 'choice-q2': 'left',
        'resonance-q1': 'right', 'resonance-q2': 'balanced',
        'rhythm-q1': 'balanced', 'rhythm-q2': 'right'
    }
    test_mastery_responses = {
        'core-q1': 'creative-expression', 'core-q2': 'craft-mastery', 'core-q3': 'passion-inspiration',
        'growth-q1': 'action-challenge', 'growth-q2': 'action-gap', 'growth-q3': 'momentum-resistance',
        'alignment-q1': 'accept-cycles', 'alignment-q2': 'control-consistency',
        'energy-q1': 'intuitive-instincts', 'energy-q2': 'rigid-routines', 'energy-q3': 'spontaneous-productivity', 'energy-q4': 'dynamic-environment'
    }

    # Generate the complete results data first
    complete_results_data = generate_complete_results(test_typology_responses, test_mastery_responses)

    # Now generate the text results
    text_results = generate_results_text(complete_results_data)

    print("\n--- Generated Results Text (Sample) ---")
    import json
    print(json.dumps(text_results, indent=2))