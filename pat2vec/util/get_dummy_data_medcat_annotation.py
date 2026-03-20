import os
import copy
import pickle
import random
from typing import Any, Dict, List
import numpy as np

# from pat2vec.util.get_dummy_data_cohort_searcher import random_state

random_state = 42

# Set random seed
np.random.seed(random_state)
random.seed(random_state)


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


def dummy_medcat_annotation_generator() -> Dict[str, Any]:
    """Loads a sample MedCAT annotation dictionary and returns a random subset.

    This function reads a predefined pickle file containing sample annotations
    and uses `random_sample` to return a random number of entities (between 0
    and 10).

    Returns:
        A dictionary containing a random subset of entities from the sample
        annotations.
    """
    pickle_file = os.path.join("test_files", "sample_annotations.pickle")
    # Load the dictionary from the pickle file
    with open(pickle_file, "rb") as f:
        sample_annotations = pickle.load(f)

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
        """Returns a random subset of sample MedCAT annotations for a single text.

        Args:
            text: The text to annotate (input is ignored, used for signature
                compatibility).

        Returns:
            A dictionary containing a random subset of entities.
        """
        return dummy_medcat_annotation_generator()

    def get_entities_multi_texts(
        self, texts: List[str], n_process: int = 1, batch_size: int = 100, **kwargs
    ) -> List[Dict[str, Any]]:
        """Returns a list of random annotations for a list of texts.

        For each text in the input list, it generates a separate random subset
        of sample MedCAT annotations.

        Args:
            texts: The list of texts to annotate. The content is ignored, but
                the length determines the number of dummy annotations returned.
            n_process: Number of processes to use (ignored).
            batch_size: Batch size to use (ignored).

        Returns:
            A list of dictionaries, where each dictionary contains a random
            subset of entities.
        """
        result = []

        for i in range(0, len(texts)):
            result.append(dummy_medcat_annotation_generator())

        # raise error if there are texts but no results
        if len(texts) > 0 and len(result) == 0:
            raise ValueError(
                "No results returned from dummy_medcat_annotation_generator"
            )
        return result


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
