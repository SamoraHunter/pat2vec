"""Patient timeline generators for EPR documents and clinical notes.

This module contains functions for generating synthetic patient timeline data,
including GPT-based, Faker-based, and CSV-based generation methods.
"""

import logging
import os
import random
import string
from datetime import datetime, timedelta
from typing import cast, Optional

import pandas as pd
from faker import Faker

from transformers import pipeline

random_state = 42
Faker.seed(random_state)
logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)


def generate_patient_timeline(client_idcode: str) -> str:
    """Generates a random patient timeline using a GPT-2 model.

    Creates a short, semi-realistic clinical note timeline for a patient,
    including demographic information and a series of timestamped entries.

    Args:
        client_idcode: The client ID for the patient.

    Returns:
        A string containing the patient's dummy timeline.
    """
    logging.getLogger("transformers").setLevel(logging.WARNING)
    generator = pipeline("text-generation", model="gpt2")

    probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
    num_entries = random.choices(range(1, 6), probabilities)[0]

    starting_age = random.randint(18, 99)

    patient_info = {
        "client_idcode": client_idcode,
        "Age": starting_age,
        "Gender": random.choice(["Male", "Female"]),
        "DOB": datetime.utcnow() - timedelta(days=365 * starting_age),
    }

    timeline = []
    current_time = datetime.utcfromtimestamp(
        random.randint(789331200, int(datetime.now().timestamp()))
    )

    for i in range(num_entries):
        entry_timestamp = current_time + timedelta(days=random.randint(1, 30))
        entry_text = generator(
            "Patient presented with:", max_length=50, do_sample=True
        )[0]["generated_text"]

        patient_info["Age"] += (entry_timestamp - current_time).days / 365
        entry_summary = f"Entered on - {entry_timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC:\n{entry_text}\n"
        timeline.append(entry_summary)
        current_time = entry_timestamp

    patient_demographics = f"Patient Demographics:\nClient ID: {patient_info['client_idcode']}\nAge: {patient_info['Age']:.1f}\nGender: {patient_info['Gender']}\nDOB: {patient_info['DOB'].strftime('%Y-%m-%d')}"
    timeline.insert(0, f"{patient_demographics}\n\nClinical Note Timeline:\n")
    patient_timeline = "\n".join(timeline)
    return patient_timeline


# Export generate_uuid_list and create_random_date_from_globals for backward compatibility


def generate_patient_timeline_faker(client_idcode: str) -> str:
    """Generates a fake patient timeline using the Faker library.

    Creates a short, semi-realistic clinical note timeline for a patient,
    including demographic information and a series of timestamped entries
    with fake sentences.

    Args:
        client_idcode: The client ID for the patient.

    Returns:
        A string containing the patient's dummy timeline.
    """
    probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
    num_entries = random.choices(range(1, 6), probabilities)[0]

    starting_age = random.randint(18, 99)

    patient_info = {
        "client_idcode": client_idcode,
        "Age": starting_age,
        "Gender": random.choice(["Male", "Female"]),
        "DOB": datetime.utcnow() - timedelta(days=365 * starting_age),
    }

    timeline = []
    current_time = datetime.utcfromtimestamp(
        random.randint(789331200, int(datetime.now().timestamp()))
    )

    for i in range(num_entries):
        entry_timestamp = current_time + timedelta(days=random.randint(1, 30))
        entry_text = faker.sentence(nb_words=15)

        patient_info["Age"] += (entry_timestamp - current_time).days / 365
        entry_summary = f"Entered on - {entry_timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC:\n{entry_text}\n"
        timeline.append(entry_summary)
        current_time = entry_timestamp

    patient_demographics = f"Patient Demographics:\nclient_idcode: {patient_info['client_idcode']}\nAge: {patient_info['Age']:.1f}\nGender: {patient_info['Gender']}\nDOB: {patient_info['DOB'].strftime('%Y-%m-%d')}"
    timeline.insert(0, f"{patient_demographics}\n\nClinical Note Timeline:\n")
    patient_timeline = "\n".join(timeline)
    return patient_timeline


def get_patient_timeline_dummy(
    client_idcode: str,
    output_path: str = os.path.join("test_files", "dummy_timeline.csv"),
) -> Optional[str]:
    """Retrieves a random patient timeline from a pre-generated CSV file.

    Args:
        client_idcode: The client ID to search for (currently unused).
        output_path: The path to the CSV file containing dummy timelines.

    Returns:
        The text of a random patient timeline, or None if the file is not found
        or is invalid.
    """
    try:
        df: pd.DataFrame = pd.read_csv(output_path)
    except FileNotFoundError:
        logger.error(f"FileNotFoundError: {output_path} doesn't exist!")
        return None

    if df.empty:
        logger.warning("DataFrame is empty!")
        return None

    if "client_idcode" not in df.columns:
        logger.error("'client_idcode' column doesn't exist in the DataFrame!")
        return None

    if "body_analysed" not in df.columns:
        logger.error("'body_analysed' column doesn't exist in the DataFrame!")
        return None

    sample: pd.DataFrame = df.sample(1, random_state=random_state)

    if len(sample) == 0:
        logger.warning("Sample is empty!")
        return None

    try:
        return cast(str, sample.iloc[0]["body_analysed"])
    except KeyError:
        logger.error("KeyError: 'body_analysed' column doesn't exist in the DataFrame!")
        return None


def run_generate_patient_timeline_and_append(
    n: int = 10, output_path: str = os.path.join("test_files", "dummy_timeline.csv")
) -> None:
    """Generates and appends dummy patient timelines to a CSV file.

    This function creates `n` dummy patient timelines and appends them to a
    specified CSV file. If the file doesn't exist, it will be created.

    Args:
        n: The number of patient timelines to generate. Defaults to 10.
        output_path: The path to the output CSV file. Defaults to
            "test_files/dummy_timeline.csv".
    """
    try:
        if os.path.exists(output_path):
            df = pd.read_csv(output_path)
        else:
            df = pd.DataFrame(columns=["client_idcode", "body_analysed"])
    except FileNotFoundError:
        logger.error(f"FileNotFoundError: {output_path} doesn't exist!")
        return

    for _ in range(n):
        client_idcode = "".join(random.choices(string.digits, k=9))

        try:
            patient_timeline_text = generate_patient_timeline(client_idcode)
        except Exception as e:
            logger.error(f"Exception: {e}")
            return

        try:
            new_row = pd.DataFrame(
                [
                    {
                        "client_idcode": client_idcode,
                        "body_analysed": patient_timeline_text,
                    }
                ]
            )
            df = pd.concat([df, new_row], ignore_index=True)
        except Exception as e:
            logger.error(f"Exception: {e}")
            return

    try:
        df.to_csv(
            output_path, mode="a", header=not os.path.exists(output_path), index=False
        )
    except Exception as e:
        logger.error(f"Exception: {e}")
        return


# Global instances
random_state = 42

random.seed(random_state)
faker = Faker()
faker.seed_instance(random_state)

# Export pipeline for backward compatibility
__all__ = ["pipeline"]
