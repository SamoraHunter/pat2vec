import os
import pickle
import random
from typing import Any, Dict, List

# from pat2vec.util.get_dummy_data_cohort_searcher import random_state
from IPython.display import display

random_state = 42
import numpy as np

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

    dummy_annotations = random_sample(sample_annotations, random.randint(0, 10))

    return dummy_annotations


class dummy_CAT(object):
    """A dummy MedCAT class for testing purposes.

    This class mimics the behavior of the MedCAT `CAT` object by providing
    methods that return randomly generated dummy annotations, allowing for
    testing of annotation pipelines without needing a real MedCAT model.
    """

    def __init__(self):
        pass

    def get_entities(self, text: str) -> Dict[str, Any]:
        """Returns a random subset of sample MedCAT annotations for a single text.

        Args:
            text: The text to annotate (input is ignored, used for signature
                compatibility).

        Returns:
            A dictionary containing a random subset of entities.
        """
        return dummy_medcat_annotation_generator()

    def get_entities_multi_texts(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Returns a list of random annotations for a list of texts.

        For each text in the input list, it generates a separate random subset
        of sample MedCAT annotations.

        Args:
            texts: The list of texts to annotate. The content is ignored, but
                the length determines the number of dummy annotations returned.

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
