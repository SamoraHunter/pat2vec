import os
import copy
import pickle
import random
from typing import Any, Dict, List, Optional
import numpy as np

# from pat2vec.util.get_dummy_data_cohort_searcher import random_state

random_state = 42

# Set random seed
np.random.seed(random_state)
random.seed(random_state)

# Store documents with their annotations for realistic simulation
_document_store: Dict[str, List[Dict[str, Any]]] = {}


def random_sample(pickled_dict: Dict[str, Any], sample_size: int) -> Dict[str, Any]:
    """Selects a random sample of entities from a pickled dictionary.

    Args:
        pickled_dict: The dictionary loaded from a pickle file, expected
            to have an 'entities' key.
        sample_size: The number of entities to sample.

    Returns:
        A new dictionary containing the sampled entities.
    """
    random.seed(random_state)
    keys = list(pickled_dict["entities"].keys())
    sample_keys = random.sample(keys, min(sample_size, len(keys)))
    sample = {"entities": {key: pickled_dict["entities"][key] for key in sample_keys}}
    return sample


def get_or_create_annotations_for_text(
    text: str,
    sample_annotations_data: Dict[str, Any],
    document_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Create annotations with correct positions within the given text.

    This function simulates realistic MedCAT annotation by:
    1. Extracting entities from sample data
    2. Finding their mentions in the actual document text (case-insensitive)
    3. Recording accurate start/end positions
    4. Including meta-annotations (Presence, Time, Subject)

    Args:
        text: The document text to annotate.
        sample_annotations_data: The loaded pickle file with sample entities.
        document_id: Optional document ID for storage in document store.

    Returns:
        A dictionary with entities containing accurate positions matching the text,
        including text samples around each entity.
    """
    random.seed(random_state)

    # If we already have annotations for this document, return them
    if document_id is not None and document_id in _document_store:
        return _document_store[document_id]

    # Get entities from sample data
    entities_dict = sample_annotations_data.get("entities", {})
    if not entities_dict:
        return {"entities": {}}

    annotated_entities = {}

    # Handle empty or very short text - generate synthetic annotations with placeholder positions
    # Gold standard CUIs commonly used for IPW demonstration
    # These are SNOMED codes that should be available in the sample annotations
    gold_standard_cuis = {
        "38341003",  # Hypertensive disorder, systemic arterial (disorder)
        "268910001",  # Patient's condition improved (finding)
        "62315008",  # Diarrhea (finding)
        "55822004",  # Hyperlipidemia (disorder)
        "49727002",  # Cough (finding)
        "274640006",  # Fever with chills (finding)
    }

    if not text or len(text.strip()) < 10:
        num_entities_to_sample = min(5, len(entities_dict))
        entity_keys = list(entities_dict.keys())

        # First, try to include gold standard entities
        gold_standard_available = [
            key
            for key in entity_keys
            if entities_dict[key].get("cui") in gold_standard_cuis
        ]

        if gold_standard_available:
            selected_gold = random.sample(
                gold_standard_available, min(2, len(gold_standard_available))
            )
            remaining_keys = [k for k in entity_keys if k not in selected_gold]
            sampled_keys = selected_gold[:num_entities_to_sample]

            # Fill with random keys if we need more
            if len(sampled_keys) < num_entities_to_sample and remaining_keys:
                extra_needed = num_entities_to_sample - len(sampled_keys)
                sampled_keys.extend(
                    random.sample(
                        remaining_keys, min(extra_needed, len(remaining_keys))
                    )
                )
        else:
            sampled_keys = random.sample(entity_keys, num_entities_to_sample)

        for i, key in enumerate(sampled_keys):
            original_entity = entities_dict[key]
            source_value = original_entity.get("source_value", "")

            if not source_value:
                continue

            # Create synthetic positions based on hash (reproducible for same entity/text combo)
            base_pos = abs(hash(str(key) + str(document_id))) % max(
                50, len(text) * 2 if text else 100
            )

            start_pos = base_pos
            end_pos = start_pos + len(source_value)

            # Extract text sample (300 chars window)
            window = 300
            virtual_start = max(0, start_pos - window)
            virtual_end = (
                min(len(text), end_pos + window) if text else start_pos + window * 2
            )

            if text and len(text) > virtual_start:
                text_sample = text[virtual_start:virtual_end]
            else:
                # Generate synthetic text sample based on entity
                text_sample = f"[Synthetic text around '{source_value}']"

            new_entity = copy.deepcopy(original_entity)
            new_entity["start"] = start_pos
            new_entity["end"] = end_pos

            if "id" in new_entity:
                new_entity["id"] = f"{key}_{i}_{document_id}_synth"

            new_entity["text_sample"] = text_sample
            new_entity["original_document_text"] = text

            annotated_entities[f"entity_{i}"] = new_entity

        result = {"entities": annotated_entities}

        # Store for this document if we have an ID
        if document_id is not None:
            _document_store[document_id] = result

        return result

    # Sample a subset of entities to annotate
    num_entities_to_sample = min(5, len(entities_dict))

    # Gold standard CUIs commonly used for IPW demonstration
    # These are SNOMED codes that should be available in the sample annotations
    gold_standard_cuis = {
        "38341003",  # Hypertensive disorder, systemic arterial (disorder)
        "268910001",  # Patient's condition improved (finding)
        "62315008",  # Diarrhea (finding)
        "55822004",  # Hyperlipidemia (disorder)
        "49727002",  # Cough (finding)
        "274640006",  # Fever with chills (finding)
    }

    if num_entities_to_sample > 0:
        entity_keys = list(entities_dict.keys())

        # First, try to include gold standard entities
        gold_standard_available = [
            key
            for key in entity_keys
            if entities_dict[key].get("cui") in gold_standard_cuis
        ]

        # Select remaining keys randomly (or use gold standard if available)
        if gold_standard_available:
            # Ensure at least one gold standard entity is included
            selected_gold = random.sample(
                gold_standard_available, min(2, len(gold_standard_available))
            )
            remaining_keys = [k for k in entity_keys if k not in selected_gold]
            sampled_keys = selected_gold[:num_entities_to_sample]

            # Fill with random keys if we need more
            if len(sampled_keys) < num_entities_to_sample and remaining_keys:
                extra_needed = num_entities_to_sample - len(sampled_keys)
                sampled_keys.extend(
                    random.sample(
                        remaining_keys, min(extra_needed, len(remaining_keys))
                    )
                )
        else:
            # No gold standard entities available, use random sampling
            sampled_keys = random.sample(entity_keys, num_entities_to_sample)

        found_any = False

        for i, key in enumerate(sampled_keys):
            original_entity = entities_dict[key]
            source_value = original_entity.get("source_value", "")

            if not source_value:
                continue

            # Find the mention in text (case-insensitive)
            text_lower = text.lower()
            mention_lower = source_value.lower()

            start_pos = text_lower.find(mention_lower)

            if start_pos != -1:
                found_any = True
                # Calculate end position based on actual document length
                end_pos = start_pos + len(source_value)

                # Extract text sample (300 chars window)
                window = 300
                virtual_start = max(0, start_pos - window)
                virtual_end = min(len(text), end_pos + window)
                text_sample = text[virtual_start:virtual_end] if text else ""

                # Create entity with accurate positions
                new_entity = copy.deepcopy(original_entity)
                new_entity["start"] = start_pos
                new_entity["end"] = end_pos

                # Ensure we have a unique ID for this annotation instance
                if "id" in new_entity:
                    new_entity["id"] = f"{key}_{i}_{document_id}"

                # Add text sample to entity (for downstream processing)
                new_entity["text_sample"] = text_sample
                new_entity["original_document_text"] = text

                annotated_entities[f"entity_{i}"] = new_entity

        # If no entities were found in the document, fall back to synthetic annotations
        if not found_any and num_entities_to_sample > 0:
            # Generate synthetic annotations with approximate positions
            for i, key in enumerate(sampled_keys):
                original_entity = entities_dict[key]
                source_value = original_entity.get("source_value", "")

                if not source_value:
                    continue

                # Create a synthetic position (assuming ~10 chars per word on average)
                # Use hash of document text to get reproducible positions
                key_str = str(key)
                base_pos = abs(hash(key_str + str(document_id))) % max(
                    100, len(text) if text else 500
                )

                start_pos = base_pos
                end_pos = start_pos + len(source_value)

                # Extract text sample (300 chars window)
                window = 300
                virtual_start = max(0, start_pos - window)
                virtual_end = (
                    min(len(text), end_pos + window) if text else start_pos + window * 2
                )
                text_sample = ""

                # If we have actual text and reasonable positions, extract from it
                if text and len(text) > virtual_start:
                    text_sample = text[
                        max(0, virtual_start) : min(len(text), virtual_end)
                    ]
                elif not text or end_pos > len(text):
                    # Generate synthetic text sample based on entity
                    text_sample = f"[Synthetic text around '{source_value}']"

                # Create entity with synthetic positions
                new_entity = copy.deepcopy(original_entity)
                new_entity["start"] = start_pos
                new_entity["end"] = end_pos

                # Ensure we have a unique ID for this annotation instance
                if "id" in new_entity:
                    new_entity["id"] = f"{key}_{i}_{document_id}_synth"

                # Add text sample to entity (for downstream processing)
                new_entity["text_sample"] = text_sample
                new_entity["original_document_text"] = text

                annotated_entities[f"entity_{i}"] = new_entity

    result = {"entities": annotated_entities}

    # Store for this document if we have an ID
    if document_id is not None:
        _document_store[document_id] = result

    return result


def dummy_medcat_annotation_generator(
    text: Optional[str] = None,
    document_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Loads a sample MedCAT annotation dictionary and returns annotations.

    This function reads a predefined pickle file containing sample annotations.
    If text is provided, it generates annotations with accurate positions
    matching the document content. Otherwise, it returns randomly sampled
    entities from the sample data (backward compatible).

    Args:
        text: Optional document text to annotate. When provided, annotations
            will include accurate start/end positions within this text.
        document_id: Optional document ID for caching/storing annotations.

    Returns:
        A dictionary containing entities with annotation details. If text is
        provided, entities will have accurate positions matching the document.
    """
    pickle_file = os.path.join("test_files", "sample_annotations.pickle")
    # Load the dictionary from the pickle file
    with open(pickle_file, "rb") as f:
        sample_annotations = pickle.load(f)

    if text is not None:
        # Generate annotations linked to actual document text
        dummy_annotations = get_or_create_annotations_for_text(
            text=text,
            sample_annotations_data=sample_annotations,
            document_id=document_id,
        )
    else:
        # Backward compatible: just return random sample
        dummy_annotations = random_sample(sample_annotations, random.randint(1, 50))

    return dummy_annotations


class dummy_CAT(object):
    """A dummy MedCAT class for testing purposes.

    This class mimics the behavior of the MedCAT `CAT` object by providing
    methods that return randomly generated dummy annotations, allowing for
    testing of annotation pipelines without needing a real MedCAT model.
    """

    class DummyFilters(dict):
        """Dummy filters object that behaves like a dict with attribute access."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.cuis = set()

    class DummyLinkingConfig(object):
        """Dummy linking configuration."""

        def __init__(self):
            self.filters = dummy_CAT.DummyFilters()
            self.filter_before_disamb = False

    class DummyConfig(object):
        """Dummy config object."""

        def __init__(self):
            self.linking = dummy_CAT.DummyLinkingConfig()

    class DummyCDB(object):
        """Dummy CDB (Concept Database) object."""

        def __init__(self):
            self.config = dummy_CAT.DummyConfig()

    def __init__(self, with_filters: bool = False):
        """Initialize dummy CAT object.

        Args:
            with_filters: If True, initialize with some dummy filters for testing
                         filter removal logic. Defaults to False.
        """
        self.config = self.DummyConfig()
        self.cdb = self.DummyCDB()

        if with_filters:
            # Add some dummy filters for testing
            self.config.linking.filters = self.DummyFilters(
                {"cuis": {"C0001234", "C0005678"}, "type_ids": {"T047", "T048"}}
            )
            self.config.linking.filter_before_disamb = True
            self.cdb.config.linking.filters["cuis"] = {"C9999999"}

    def get_entities(self, text: str) -> Dict[str, Any]:
        """Returns MedCAT annotations linked to the provided text.

        Args:
            text: The text to annotate. Annotations will have accurate start/end
                positions matching the document content.

        Returns:
            A dictionary containing entities with accurate positions in the text.
        """
        # Generate a simple document ID based on hash of text (for caching)
        doc_id = hash(text) % 10000 if text else None
        return dummy_medcat_annotation_generator(text=text, document_id=doc_id)

    def get_entities_multi_texts(
        self, texts: List[str], n_process: int = 1, batch_size: int = 100, **kwargs
    ) -> List[Dict[str, Any]]:
        """Returns a list of annotations linked to each text.

        For each text in the input list, it generates annotations with accurate
        positions matching the document content.

        Args:
            texts: The list of texts to annotate. Each text will have annotations
                with accurate start/end positions within that text.
            n_process: Number of processes to use (ignored).
            batch_size: Batch size to use (ignored).

        Returns:
            A list of dictionaries, where each dictionary contains entities with
            accurate positions matching the corresponding text.
        """
        result = []

        for i in range(0, len(texts)):
            doc_id = hash(texts[i]) % 10000 if texts[i] else None
            result.append(
                dummy_medcat_annotation_generator(text=texts[i], document_id=doc_id)
            )

        # raise error if there are texts but no results
        if len(texts) > 0 and len(result) == 0:
            raise ValueError(
                "No results returned from dummy_medcat_annotation_generator"
            )
        return result


def clear_document_store() -> None:
    """Clears the document store for fresh annotation generation.

    This function clears the internal document cache that stores annotations
    linked to specific document texts. Call this when you want to ensure
    annotations are regenerated rather than retrieved from cache.
    """
    global _document_store
    _document_store = {}


def augment_dummy_annotations_file(target_count: int = 500) -> None:
    """Increases the number of dummy annotations in the pickle file.

    This function reads the sample annotations file, duplicates existing
    entries with new IDs until the target count is reached, and saves the
    result back to the file.

    Args:
        target_count: The desired minimum number of annotations in the file.
    """
    pickle_file = os.path.join("test_files", "sample_annotations.pickle")

    # Handle path resolution if not running from root
    if not os.path.exists(pickle_file):
        pickle_file = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "test_files",
            "sample_annotations.pickle",
        )

    if not os.path.exists(pickle_file):
        print(f"Error: Could not find {pickle_file}")
        return

    with open(pickle_file, "rb") as f:
        data = pickle.load(f)

    entities = data.get("entities", {})
    keys = list(entities.keys())
    current_count = len(entities)

    if current_count < target_count and keys:
        # Duplicate existing entities to reach target count
        while len(entities) < target_count:
            for key in keys:
                if len(entities) >= target_count:
                    break
                new_key = f"{key}_copy_{len(entities)}"
                new_entity = copy.deepcopy(entities[key])
                new_entity["id"] = len(entities)  # Update ID if relevant

                # Randomly perturb accuracy and similarity to create variance
                new_entity["acc"] = random.uniform(0.1, 1.0)
                new_entity["context_similarity"] = random.uniform(0.1, 1.0)

                # Shift positions to avoid exact overlaps in tests
                if "start" in new_entity and "end" in new_entity:
                    shift = random.randint(1, 1000)
                    new_entity["start"] += shift
                    new_entity["end"] += shift

                # Randomize meta-annotations to test filtering logic (e.g. negated presence)
                if "meta_anns" in new_entity:
                    if "Presence" in new_entity["meta_anns"]:
                        new_entity["meta_anns"]["Presence"]["value"] = random.choice(
                            ["True", "False"]
                        )
                    if "Time" in new_entity["meta_anns"]:
                        new_entity["meta_anns"]["Time"]["value"] = random.choice(
                            ["Recent", "Past"]
                        )
                    if "Subject" in new_entity["meta_anns"]:
                        new_entity["meta_anns"]["Subject"]["value"] = random.choice(
                            ["Patient", "Other"]
                        )

                entities[new_key] = new_entity

        with open(pickle_file, "wb") as f:
            pickle.dump(data, f)
        print(f"Expanded annotation pool from {current_count} to {len(entities)}.")
