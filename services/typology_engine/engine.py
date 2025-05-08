import yaml
from pathlib import Path
from typing import Dict, List, Tuple, Any

from .models import TypologyConfig

class TypologyEngine:
    """
    Handles loading the typology configuration and calculating assessment results.
    """
    def __init__(self, config_path: str = "assets/typology_questions.yml"):
        """
        Initializes the engine by loading and parsing the YAML configuration.

        Args:
            config_path: Path to the typology YAML configuration file.
        """
        self.config_path = Path(config_path)
        if not self.config_path.is_file():
            raise FileNotFoundError(f"Typology configuration not found at {config_path}")

        try:
            with open(self.config_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            self.config = TypologyConfig.model_validate(raw_config)
            self._validate_unique_ids() # Add validation after Pydantic parsing
            self._build_lookup_maps()
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file {config_path}: {e}")
        except Exception as e: # Catch Pydantic validation errors or our custom errors
             # Ensure custom ValueErrors are passed through, otherwise wrap others
             if isinstance(e, ValueError):
                 raise e
             else:
                 raise ValueError(f"Error validating config file {config_path}: {e}")

    def _validate_unique_ids(self):
        """Checks for duplicate IDs within the loaded configuration."""
        ids = set()
        # Spectrum IDs
        for spectrum in self.config.spectrums:
            if spectrum.id in ids:
                raise ValueError(f"Duplicate Spectrum ID found: {spectrum.id}")
            ids.add(spectrum.id)
        # Mastery Dimension IDs
        ids = set()
        for dim in self.config.mastery_dimensions:
            if dim.id in ids:
                raise ValueError(f"Duplicate Mastery Dimension ID found: {dim.id}")
            ids.add(dim.id)
        # Question IDs (across all spectrums and mastery)
        ids = set()
        all_spectrum_questions = [q for s in self.config.spectrums for q in s.questions]
        all_mastery_questions = [q for d in self.config.mastery_dimensions for q in d.questions]
        for question in all_spectrum_questions + all_mastery_questions:
             if question.id in ids:
                 raise ValueError(f"Duplicate Question ID found: {question.id}")
             ids.add(question.id)
             # Check mastery answer IDs within the question
             if hasattr(question, 'answers') and isinstance(question.answers, list):
                 answer_ids = set()
                 for answer in question.answers:
                     if answer.id in answer_ids:
                         raise ValueError(f"Duplicate Answer ID '{answer.id}' found within Question '{question.id}'")
                     answer_ids.add(answer.id)
        # Typology Profile IDs
        ids = set()
        for profile in self.config.results.typology_profiles:
            if profile.id in ids:
                raise ValueError(f"Duplicate Typology Profile ID found: {profile.id}")
            ids.add(profile.id)


    def _build_lookup_maps(self):
        """Builds dictionaries for quick lookup of questions and answers."""
        self.spectrum_questions = {
            q.id: q for spectrum in self.config.spectrums for q in spectrum.questions
        }
        self.mastery_questions = {
            q.id: q for dim in self.config.mastery_dimensions for q in dim.questions
        }
        # Add more lookups if needed, e.g., spectrum by id

    def get_questions(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Returns a simplified list of spectrum and mastery questions for presentation.
        (Does not implement actual presentation logic).
        """
        spectrum_q_list = [
            {"id": q.id, "text": q.text, "spectrum_id": s.id, "answers": q.answers.model_dump()}
            for s in self.config.spectrums
            for q in s.questions
        ]
        mastery_q_list = [
            {"id": q.id, "text": q.text, "dimension_id": d.id, "answers": [a.model_dump() for a in q.answers]}
            for d in self.config.mastery_dimensions
            for q in d.questions
        ]
        return spectrum_q_list, mastery_q_list

    def calculate_scores(self, answers: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates the final assessment scores based on user answers.

        Args:
            answers: A dictionary where keys are question IDs (e.g., 'cognitive_q1', 'core_q1')
                     and values are the selected answer values (e.g., -1, 0, 1 for spectrums,
                     or string values like 'creative_expression' for mastery).

        Returns:
            A dictionary containing the calculated scores and results.
            (Detailed structure TBD based on scoring logic).
        """
        if not answers:
            raise ValueError("Answers dictionary cannot be empty.")

        # 1. Separate spectrum and mastery answers
        spectrum_answers = {
            qid: val for qid, val in answers.items() if qid in self.spectrum_questions
        }
        mastery_answers = {
            qid: val for qid, val in answers.items() if qid in self.mastery_questions
        }

        # 2. Compute initial spectrum scores
        initial_spectrum_scores = self._compute_initial_spectrum_scores(spectrum_answers)

        # 3. Apply mastery influence
        adjusted_spectrum_scores = self._apply_mastery_influence(
            initial_spectrum_scores, mastery_answers
        )

        # 4. Rank spectrums
        ranked_spectrums = self._rank_spectrums(adjusted_spectrum_scores)

        # 5. Determine placement (e.g., left_leaning, balanced, right_leaning)
        spectrum_placements = self._determine_placements(adjusted_spectrum_scores)

        # --- Placeholder for Profile ID and Typology Matching ---
        # TODO: Define logic for generating profile ID and matching typologies
        final_profile_id = self._generate_profile_id(ranked_spectrums, spectrum_placements)
        matching_typologies = self._find_matching_typologies(ranked_spectrums, spectrum_placements)
        # --- End Placeholder ---

        return {
            "initial_spectrum_scores": initial_spectrum_scores,
            "adjusted_spectrum_scores": adjusted_spectrum_scores,
            "ranked_spectrums": ranked_spectrums,
            "spectrum_placements": spectrum_placements,
            "final_profile_id": final_profile_id,
            "top_matching_typologies": matching_typologies,
            "raw_answers": answers # Include raw answers for reference
        }

    def _compute_initial_spectrum_scores(self, spectrum_answers: Dict[str, int]) -> Dict[str, float]:
        """Calculates the sum of scores for each spectrum."""
        scores = {spectrum.id: 0.0 for spectrum in self.config.spectrums}
        counts = {spectrum.id: 0 for spectrum in self.config.spectrums}

        for spectrum in self.config.spectrums:
            for question in spectrum.questions:
                if question.id in spectrum_answers:
                    answer_value = spectrum_answers[question.id]
                    # Validate answer value is one of the expected [-1, 0, 1]
                    if answer_value not in [-1, 0, 1]:
                         raise ValueError(f"Invalid answer value '{answer_value}' for spectrum question '{question.id}'. Expected -1, 0, or 1.")
                    scores[spectrum.id] += answer_value
                    counts[spectrum.id] += 1

        # Optional: Normalize scores (e.g., average score per spectrum)
        # For now, just return the sum
        # normalized_scores = {sid: (scores[sid] / counts[sid] if counts[sid] > 0 else 0) for sid in scores}
        # return normalized_scores
        return scores # Returning sum for now as influence might expect raw sums

    def _apply_mastery_influence(
        self,
        initial_scores: Dict[str, float],
        mastery_answers: Dict[str, str]
    ) -> Dict[str, float]:
        """Adjusts spectrum scores based on selected mastery dimension answers."""
        adjusted_scores = initial_scores.copy()
        influence_rules = self.config.scoring_rules.mastery_spectrum_influence

        for question_id, answer_value in mastery_answers.items():
            # Find which mastery dimension this question belongs to
            dimension_id = None
            for dim in self.config.mastery_dimensions:
                if any(q.id == question_id for q in dim.questions):
                    dimension_id = dim.id
                    break

            if dimension_id and dimension_id in influence_rules:
                dimension_influence = influence_rules.get(dimension_id, {})
                if answer_value in dimension_influence:
                    spectrum_influences = dimension_influence[answer_value]
                    for spectrum_id, influence_value in spectrum_influences.items():
                        if spectrum_id in adjusted_scores:
                            adjusted_scores[spectrum_id] += influence_value
                        else:
                            print(f"Warning: Spectrum '{spectrum_id}' mentioned in mastery influence for '{dimension_id}/{answer_value}' not found in calculated scores.") # Or raise error

        # Clamp scores if needed (e.g., prevent going beyond theoretical max/min)
        # max_possible = len(spectrum.questions) # Max positive score
        # min_possible = -len(spectrum.questions) # Max negative score
        # adjusted_scores[spectrum_id] = max(min_possible, min(max_possible, adjusted_scores[spectrum_id]))
        # Clamping logic depends on whether scores were normalized earlier. Skipping for now.

        return adjusted_scores

    def _rank_spectrums(self, adjusted_scores: Dict[str, float]) -> List[Tuple[str, float]]:
        """Ranks spectrums based on priority order and adjusted scores."""
        # Create a dictionary mapping spectrum_id to its priority index
        priority_map = {
            spectrum_id: index
            for index, spectrum_id in enumerate(self.config.scoring_rules.spectrum_priority_order)
        }

        # Sort the adjusted scores based first on priority, then potentially by score magnitude (absolute value) or raw score.
        # The exact sorting logic after priority isn't specified, using absolute score magnitude as a tie-breaker.
        ranked_list = sorted(
            adjusted_scores.items(),
            key=lambda item: (
                priority_map.get(item[0], float('inf')), # Primary sort: Priority (lower index first)
                -abs(item[1]) # Secondary sort: Absolute score magnitude (higher magnitude first)
            )
        )
        return ranked_list

    def _determine_placements(self, adjusted_scores: Dict[str, float]) -> Dict[str, str]:
        """Determines the placement label (e.g., left, balanced, right) for each spectrum."""
        # This requires defining thresholds for 'strong_left', 'left_leaning', etc.
        # These thresholds are not explicitly defined in the YAML.
        # Making assumptions based on typical 3-question spectrums (range -3 to +3).
        # TODO: Refine thresholds based on actual score ranges or add to config.
        placements = {}
        placement_map = self.config.scoring_rules.placement_mapping

        for spectrum in self.config.spectrums:
            score = adjusted_scores.get(spectrum.id, 0.0)
            num_questions = len(spectrum.questions)
            max_score = float(num_questions) # e.g., 3
            min_score = -float(num_questions) # e.g., -3

            # Example Thresholds (assuming 3 questions per spectrum, range -3 to +3)
            strong_threshold = max_score * 0.67 # e.g., 2.0
            leaning_threshold = max_score * 0.33 # e.g., 1.0

            placement_key = "balanced" # Default
            if score <= -strong_threshold:
                placement_key = "strong_left"
            elif score < -leaning_threshold:
                 placement_key = "left_leaning" # Or just "left"? YAML has both. Using leaning.
            elif score >= strong_threshold:
                placement_key = "strong_right"
            elif score > leaning_threshold:
                 placement_key = "right_leaning" # Or just "right"? YAML has both. Using leaning.
            # Scores between -leaning_threshold and +leaning_threshold remain 'balanced'

            # Map the internal key (e.g., 'strong_left') to the final label (e.g., 'strongly_structured')
            placements[spectrum.id] = placement_map.get(placement_key, "unknown_placement")

        return placements


    # --- Profile ID Generation and Typology Matching ---
    def _generate_profile_id(self, ranked_spectrums: List[Tuple[str, float]], placements: Dict[str, str]) -> str:
        """
        Generates a profile ID based on the top 3 ranked spectrums and their placements.
        This provides a concise summary identifier for the profile.
        """
        if not ranked_spectrums:
            return "UNKNOWN_PROFILE"
        top_3 = [spec_id for spec_id, score in ranked_spectrums[:3]]
        # Use the first letter of the mapped placement identifier (e.g., 'strongly_structured' -> 'S')
        placement_codes = "".join(placements.get(sid, 'X')[0].upper() for sid in top_3) # e.g., SSB (StronglyStructured, Structured, Balanced)
        top_3_ids_short = "_".join(sid.split('_')[0][:3].upper() for sid in top_3) # e.g., COG_KIN_CHO
        return f"{top_3_ids_short}_{placement_codes}" # Example: COG_KIN_CHO_SSB

    def _find_matching_typologies(
        self,
        ranked_spectrums: List[Tuple[str, float]],
        placements: Dict[str, str],
        top_n: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Finds the top N matching typology profiles based on spectrum placements.

        Note: This uses a simplified matching logic based on inferred ideal placements
              for typologies, as explicit rules are not in the YAML config.
              Weights matches based on spectrum priority.

        Args:
            ranked_spectrums: List of (spectrum_id, score) tuples, ordered by priority.
            placements: Dictionary mapping spectrum_id to placement label (e.g., 'structured').
            top_n: The number of top matching profiles to return.

        Returns:
            A list of the top N matching typology profile dictionaries.
        """
        if not self.config.results.typology_profiles:
            return []

        # --- Inferred Ideal Placements (Example - Should ideally be in config) ---
        # Mapping typology ID prefixes/keywords to expected placements on key spectrums.
        # This is a major simplification and requires domain expertise.
        ideal_patterns = {
            "architect": {"cognitive_alignment": ["structured", "strongly_structured"], "kinetic_drive": ["structured", "strongly_structured"], "manifestation_rhythm": ["structured", "strongly_structured"]},
            "visionary": {"cognitive_alignment": ["fluid", "strongly_fluid"], "perceptual_focus": ["fluid", "strongly_fluid"], "resonance_field": ["fluid", "strongly_fluid"]},
            "integrator": {"cognitive_alignment": ["balanced"], "kinetic_drive": ["balanced"], "manifestation_rhythm": ["balanced"]}, # Example
            "harmonizer": {"resonance_field": ["balanced", "fluid"], "perceptual_focus": ["balanced", "fluid"]}, # Example
            "intuitive": {"cognitive_alignment": ["fluid", "strongly_fluid"], "choice_navigation": ["fluid", "strongly_fluid"]},
            "quantum": {"cognitive_alignment": ["fluid", "strongly_fluid"], "perceptual_focus": ["fluid", "strongly_fluid"], "resonance_field": ["fluid", "strongly_fluid"]},
            # Add more patterns based on typology analysis...
        }
        # --- End Inferred Placements ---

        priority_weights = {
            spec_id: len(self.config.scoring_rules.spectrum_priority_order) - i
            for i, spec_id in enumerate(self.config.scoring_rules.spectrum_priority_order)
        }

        profile_scores = []
        for profile in self.config.results.typology_profiles:
            current_score = 0
            matched_pattern = None

            # Find the best matching ideal pattern based on profile ID/name keywords
            for keyword, pattern in ideal_patterns.items():
                 # Use lower case for robust matching
                if keyword in profile.id.lower() or keyword in profile.typology_name.lower():
                    matched_pattern = pattern
                    break # Use the first matched pattern

            if matched_pattern:
                for spectrum_id, ideal_placements in matched_pattern.items():
                    user_placement = placements.get(spectrum_id)
                    if user_placement:
                        weight = priority_weights.get(spectrum_id, 1) # Default weight 1 if not in priority list
                        if user_placement in ideal_placements:
                            current_score += 2 * weight # Strong match
                        # Optional: Add points for adjacent matches (e.g., 'structured' vs 'strongly_structured')
                        # elif _is_adjacent(user_placement, ideal_placements):
                        #     current_score += 1 * weight # Weaker match
            else:
                 # Handle profiles that don't match any simple keyword pattern (e.g., assign base score or skip)
                 pass

            profile_scores.append({"profile": profile.model_dump(), "score": current_score})

        # Sort profiles by score in descending order
        sorted_profiles = sorted(profile_scores, key=lambda x: x["score"], reverse=True)

        # Return the top N profiles
        return [p["profile"] for p in sorted_profiles[:top_n]]


# Example Usage (for testing purposes)
if __name__ == "__main__":
    engine = TypologyEngine()
    print(f"Loaded config version: {engine.config.version}")

    # Get questions (example of how they might be retrieved for a UI)
    # spec_qs, mast_qs = engine.get_questions()
    # print("\nSpectrum Questions Sample:")
    # print(spec_qs[0])
    # print("\nMastery Questions Sample:")
    # print(mast_qs[0])


    # Simulate user answers (replace with actual answers)
    # Need one answer for each question ID
    simulated_answers = {}
    for s in engine.config.spectrums:
        for q in s.questions:
            # Simulate choosing 'balanced' or 'right' mostly
            simulated_answers[q.id] = 0 if q.id.endswith('q1') else 1
    for d in engine.config.mastery_dimensions:
        for q in d.questions:
            # Simulate choosing the first answer option
            simulated_answers[q.id] = q.answers[0].value

    print("\nSimulated Answers (Partial):")
    print({k: v for i, (k, v) in enumerate(simulated_answers.items()) if i < 5})


    try:
        results = engine.calculate_scores(simulated_answers)
        print("\nCalculated Results:")
        import json
        print(json.dumps(results, indent=2))
    except ValueError as e:
        print(f"\nError calculating scores: {e}")