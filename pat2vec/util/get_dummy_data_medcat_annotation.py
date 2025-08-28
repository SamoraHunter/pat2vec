import os
import pickle
import random

# from pat2vec.util.get_dummy_data_cohort_searcher import random_state
from IPython.display import display

random_state = 42
import numpy as np

# Set random seed
np.random.seed(random_state)
random.seed(random_state)


def random_sample(pickled_dict, sample_size):
    random.seed(random_state)
    keys = list(pickled_dict["entities"].keys())
    sample_keys = random.sample(keys, min(sample_size, len(keys)))
    sample = {"entities": {key: pickled_dict["entities"][key] for key in sample_keys}}
    return sample


def dummy_medcat_annotation_generator():
    """
    Loads a sample MedCAT annotation dictionary from a pickle file and returns a random subset of its entities.

    Parameters:
        None

    Returns:
        dict: A dictionary containing a random subset of the entities from the sample annotations.
    """
    pickle_file = os.path.join("test_files", "sample_annotations.pickle")
    # Load the dictionary from the pickle file
    with open(pickle_file, "rb") as f:
        sample_annotations = pickle.load(f)

    dummy_annotations = random_sample(sample_annotations, random.randint(0, 10))

    return dummy_annotations


class dummy_CAT(object):

    def __init__(self):
        pass

    def get_entities(self, text):
        """
        Given a text, this function returns a random subset of sample MedCAT annotations.

        Parameters:
            text (str): The text to annotate.

        Returns:
            dict: A dictionary containing a random subset of the entities from the sample annotations.
        """
        return dummy_medcat_annotation_generator()

    def get_entities_multi_texts(self, texts):
        """
        Given a list of texts, this function returns a list of dictionaries containing a random subset of sample MedCAT annotations for each text in the list.

        Parameters:
            texts (list): The list of texts to annotate.

        Returns:
            list: A list of dictionaries containing a random subset of the entities from the sample annotations for each text in the list.
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
