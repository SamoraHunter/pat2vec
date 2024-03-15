import logging
import random
import re
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker
from transformers import pipeline

from pat2vec.util.dummy_data_files.dummy_lists import (
    blood_test_names,
    diagnostic_names,
    drug_names,
    ethnicity_list,
)

fake = Faker()

# pending implementation of other data sources/ orders index etc..


def generate_epr_documents_data(
    num_rows,
    entered_list,
    global_start_year,
    global_start_month,
    global_end_year,
    global_end_month,
):
    """
    Generate dummy data for the 'epr_documents' index.

    Parameters:
    - num_rows (int): Number of rows to generate.
    - entered_list (list): List of entered values.
    - global_start_year (int): Start year for the global date range.
    - global_start_month (int): Start month for the global date range.
    - global_end_year (int): End year for the global date range.
    - global_end_month (int): End month for the global date range.

    Returns:
    - pd.DataFrame: Generated DataFrame with specified columns.
    """
    current_pat_client_id_code = random.choice(entered_list)

    data = {
        "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
        "document_guid": [f"doc_{i}" for i in range(num_rows)],
        "document_description": [f"description_{i}" for i in range(num_rows)],
        # 'body_analysed': [fake.paragraph() for _ in range(num_rows)],
        "body_analysed": [
            generate_patient_timeline(current_pat_client_id_code)
            for _ in range(num_rows)
        ],
        "updatetime": [
            datetime(
                random.randint(global_start_year, global_end_year),
                random.randint(global_start_month, global_end_month),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
            )
            for _ in range(num_rows)
        ],
        "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
    }

    df = pd.DataFrame(data)
    return df


def generate_epr_documents_personal_data(
    num_rows,
    entered_list,
    global_start_year,
    global_start_month,
    global_end_year,
    global_end_month,
):
    """
    Generate dummy data for the 'epr_documents' index with linked personal information.

    Parameters:
    - num_rows (int): Number of rows to generate.
    - entered_list (list): List of entered values.
    - global_start_year (int): Start year for the global date range.
    - global_start_month (int): Start month for the global date range.
    - global_end_year (int): End year for the global date range.
    - global_end_month (int): End month for the global date range.

    Returns:
    - pd.DataFrame: Generated DataFrame with linked personal information.
    """
    current_pat_client_id_code = random.choice(entered_list)

    ethnicity = fake.random_element(ethnicity_list)

    data = {
        "client_idcode": [current_pat_client_id_code] * num_rows,
        "client_firstname": [fake.first_name() for _ in range(num_rows)],
        "client_lastname": [fake.last_name() for _ in range(num_rows)],
        "client_dob": [
            fake.date_of_birth(minimum_age=18, maximum_age=90).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            for _ in range(num_rows)
        ],
        "client_gendercode": [
            random.choice(["male", "female"]) for _ in range(num_rows)
        ],
        "client_racecode": [ethnicity for _ in range(num_rows)],
        "client_deceaseddtm": [
            fake.date_time_this_decade() if random.choice([True, False]) else None
            for _ in range(num_rows)
        ],
        "updatetime": [
            datetime(
                random.randint(global_start_year, global_end_year),
                random.randint(global_start_month, global_end_month),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
                tzinfo=timezone.utc,
            ).strftime("%Y-%m-%d")
            for _ in range(num_rows)
        ],
    }

    df = pd.DataFrame(data)
    return df


def generate_diagnostic_orders_data(
    num_rows,
    entered_list,
    global_start_year,
    global_start_month,
    global_end_year,
    global_end_month,
):
    """
    Generate dummy data for the 'diagnostic_orders' index.

    Parameters:
    - num_rows (int): Number of rows to generate.
    - entered_list (list): List of entered values.
    - global_start_year (int): Start year for the global date range.
    - global_start_month (int): Start month for the global date range.
    - global_end_year (int): End year for the global date range.
    - global_end_month (int): End month for the global date range.

    Returns:
    - pd.DataFrame: Generated DataFrame with specified columns.
    """

    data = {
        "order_guid": [f"order_{i}" for i in range(num_rows)],
        "client_idcode": [random.choice(entered_list) for _ in range(num_rows)],
        "order_name": [fake.random_element(diagnostic_names) for _ in range(num_rows)],
        "order_summaryline": [fake.sentence() for _ in range(num_rows)],
        "order_holdreasontext": [fake.sentence() for _ in range(num_rows)],
        "order_entered": [
            datetime(
                random.randint(global_start_year, global_end_year),
                random.randint(global_start_month, global_end_month),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
            )
            for _ in range(num_rows)
        ],
        "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
        "_id": ["{i}" for i in range(num_rows)],
        "_index": ["{np.nan}" for _ in range(num_rows)],
        "_score": ["{np.nan}" for _ in range(num_rows)],
    }
    # print("generate_diagnostic_orders_data")
    df = pd.DataFrame(data)
    return df


def generate_drug_orders_data(
    num_rows,
    entered_list,
    global_start_year,
    global_start_month,
    global_end_year,
    global_end_month,
):
    """
    Generate dummy data for the 'drug_orders' index.

    Parameters:
    - num_rows (int): Number of rows to generate.
    - entered_list (list): List of entered values.
    - global_start_year (int): Start year for the global date range.
    - global_start_month (int): Start month for the global date range.
    - global_end_year (int): End year for the global date range.
    - global_end_month (int): End month for the global date range.

    Returns:
    - pd.DataFrame: Generated DataFrame with specified columns.
    """
    # print("generate_drug_orders_data")
    data = {
        "order_guid": [f"order_{i}" for i in range(num_rows)],
        "client_idcode": [random.choice(entered_list) for _ in range(num_rows)],
        # New value for drug_name
        "order_name": [fake.random_element(drug_names) for _ in range(num_rows)],
        # New value for drug_description
        "order_summaryline": [fake.sentence() for _ in range(num_rows)],
        # New value for dosage
        "order_holdreasontext": [fake.sentence() for _ in range(num_rows)],
        "order_entered": [
            datetime(
                random.randint(global_start_year, global_end_year),
                random.randint(global_start_month, global_end_month),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
            )
            for _ in range(num_rows)
        ],
        "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
        "_id": ["{i}" for i in range(num_rows)],
        "_index": ["{np.nan}" for i in range(num_rows)],
        "_score": ["{np.nan}" for i in range(num_rows)],
    }

    df = pd.DataFrame(data)
    return df


def generate_observations_MRC_text_data(
    num_rows,
    entered_list,
    global_start_year,
    global_start_month,
    global_end_year,
    global_end_month,
):
    """
    Generate dummy data for the 'observations' index.

    Parameters:
    - num_rows (int): Number of rows to generate.
    - entered_list (list): List of entered values.
    - global_start_year (int): Start year for the global date range.
    - global_start_month (int): Start month for the global date range.
    - global_end_year (int): End year for the global date range.
    - global_end_month (int): End month for the global date range.

    Returns:
    - pd.DataFrame: Generated DataFrame with specified columns.
    """

    current_pat_client_id_code = random.choice(entered_list)

    data = {
        "observation_guid": [f"obs_{i}" for i in range(num_rows)],
        "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
        "obscatalogmasteritem_displayname": "AoMRC_ClinicalSummary_FT",
        "observation_valuetext_analysed": [
            generate_patient_timeline(current_pat_client_id_code)
            for _ in range(num_rows)
        ],
        # 'observation_valuetext_analysed': [fake.paragraph() for _ in range(num_rows)],
        "observationdocument_recordeddtm": [
            datetime(
                random.randint(global_start_year, global_end_year),
                random.randint(global_start_month, global_end_month),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
            )
            for _ in range(num_rows)
        ],
        "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
        "_id": ["{i}" for i in range(num_rows)],
        "_index": ["{np.nan}" for i in range(num_rows)],
        "_score": ["{np.nan}" for i in range(num_rows)],
    }

    df = pd.DataFrame(data)
    return df


def generate_observations_data(
    num_rows,
    entered_list,
    global_start_year,
    global_start_month,
    global_end_year,
    global_end_month,
    search_term,
):
    """
    Generate dummy data for the 'observations' index.

    Parameters:
    - num_rows (int): Number of rows to generate.
    - entered_list (list): List of entered values.
    - global_start_year (int): Start year for the global date range.
    - global_start_month (int): Start month for the global date range.
    - global_end_year (int): End year for the global date range.
    - global_end_month (int): End month for the global date range.

    Returns:
    - pd.DataFrame: Generated DataFrame with specified columns.
    """

    current_pat_client_id_code = random.choice(entered_list)

    data = {
        "observation_guid": [f"obs_{i}" for i in range(num_rows)],
        "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
        "obscatalogmasteritem_displayname": [search_term],
        "observation_valuetext_analysed": [
            random.uniform(0, 100) for _ in range(num_rows)
        ],
        # 'observation_valuetext_analysed': [fake.paragraph() for _ in range(num_rows)],
        "observationdocument_recordeddtm": [
            datetime(
                random.randint(global_start_year, global_end_year),
                random.randint(global_start_month, global_end_month),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
            )
            for _ in range(num_rows)
        ],
        "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
        "_id": ["{i}" for i in range(num_rows)],
        "_index": ["{np.nan}" for i in range(num_rows)],
        "_score": ["{np.nan}" for i in range(num_rows)],
    }

    df = pd.DataFrame(data)
    return df


def generate_basic_observations_data(
    num_rows,
    entered_list,
    global_start_year,
    global_start_month,
    global_end_year,
    global_end_month,
):
    """
    Generate dummy data for the 'basic_observations' index.

    Parameters:
    - num_rows (int): Number of rows to generate.
    - entered_list (list): List of entered values.
    - global_start_year (int): Start year for the global date range.
    - global_start_month (int): Start month for the global date range.
    - global_end_year (int): End year for the global date range.
    - global_end_month (int): End month for the global date range.

    Returns:
    - pd.DataFrame: Generated DataFrame with specified columns.
    """
    # print("generate_basic_observations_data")
    current_pat_client_id_code = random.choice(entered_list)

    data = {
        "client_idcode": [current_pat_client_id_code] * num_rows,
        "basicobs_itemname_analysed": [
            fake.random_element(blood_test_names) for _ in range(num_rows)
        ],
        "basicobs_value_numeric": [random.uniform(1, 100) for _ in range(num_rows)],
        "basicobs_entered": [
            datetime(
                random.randint(global_start_year, global_end_year),
                random.randint(global_start_month, global_end_month),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
                tzinfo=timezone.utc,
            ).strftime("%Y-%m-%dT%H:%M:%S")
            for _ in range(num_rows)
        ],
        "clientvisit_serviceguid": [f"service_{i}" for i in range(num_rows)],
        "_id": ["{i}" for i in range(num_rows)],
        "_index": ["{np.nan}" for i in range(num_rows)],
        "_score": ["{np.nan}" for i in range(num_rows)],
        "order_guid": ["{np.nan}" for i in range(num_rows)],
        "order_name": ["{np.nan}" for i in range(num_rows)],
        "order_summaryline": ["{np.nan}" for i in range(num_rows)],
        "order_holdreasontext": ["{np.nan}" for i in range(num_rows)],
        "order_entered": ["{np.nan}" for i in range(num_rows)],
        "clientvisit_visitidcode": ["{np.nan}" for i in range(num_rows)],
    }

    df = pd.DataFrame(data)
    print(type(df))
    return df


def extract_date_range(string):
    pattern = r"(\d+)-(\d+)-(\d+) TO (\d+)-(\d+)-(\d+)"
    match = re.search(pattern, string)
    if match:
        global_start_year = int(match.group(1))
        global_start_month = int(match.group(2))
        global_start_day = int(match.group(3))
        global_end_year = int(match.group(4))
        global_end_month = int(match.group(5))
        global_end_day = int(match.group(6))
        return (
            global_start_year,
            global_start_month,
            global_start_day,
            global_end_year,
            global_end_month,
            global_end_day,
        )
    else:
        return None


def cohort_searcher_with_terms_and_search_dummy(
    index_name, fields_list, term_name, entered_list, search_string
):
    """
    Generate dummy data based on the provided index and search parameters. This function is a dummy for a real cogStack deployment.

    Parameters:
    - index_name (str): Name of the index.
    - fields_list (list): List of fields for the DataFrame columns.
    - term_name (str): Term name for search.
    - entered_list (list): List of entered values.
    - search_string (str): Search string for additional filtering.
    - verbose (bool): Verbosity flag to enable/disable print statements.

    Returns:
    - pd.DataFrame: Generated DataFrame based on the specified conditions.
    """

    verbose = True

    (
        global_start_year,
        global_start_month,
        global_start_day,
        global_end_year,
        global_end_month,
        global_end_day,
    ) = extract_date_range(search_string)

    if verbose:
        print("cohort_searcher_with_terms_and_search_dummy:", search_string)

    if "client_firstname" in fields_list:
        if verbose:
            print("Generating data for 'client_firstname'")
        num_rows = random.randint(0, 10)
        df = generate_epr_documents_personal_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
        )
        return df

    elif "basicobs_value_numeric" in search_string:
        if verbose:
            print("Generating data for 'basicobs_value_numeric'")
        num_rows = random.randint(0, 10)
        df = generate_basic_observations_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
        )
        return df

    elif index_name == "epr_documents":
        if verbose:
            print("Generating data for 'epr_documents'")

        probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]  # Adjust as needed
        num_rows = random.choices(range(1, 6), probabilities)[0]
        df = generate_epr_documents_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
        )
        return df

    elif (
        index_name == "observations"
        and search_string.find("AoMRC_ClinicalSummary_FT") != -1
    ):
        if verbose:
            print("Generating mrc text data for 'observations'")

        probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]  # Adjust as needed
        num_rows = random.choices(range(1, 6), probabilities)[0]
        df = generate_observations_MRC_text_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
        )
        return df

    elif index_name == "observations":
        if verbose:
            print("Generating data for 'observations'")

        probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]  # Adjust as needed
        num_rows = random.choices(range(1, 6), probabilities)[0]

        search_term = str(
            extract_search_term_obscatalogmasteritem_displayname(search_string)
        )

        df = generate_observations_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            search_term,
        )
        return df

    elif index_name == "order" and "medication" in search_string:
        if verbose:
            print("Generating data for 'orders' with medication")
        num_rows = random.randint(0, 10)
        df = generate_drug_orders_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
        )
        return df

    elif index_name == "order" and "diagnostic" in search_string:
        if verbose:
            print("Generating data for 'orders' with diagnostic")
        num_rows = random.randint(0, 10)
        df = generate_diagnostic_orders_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
        )
        return df

    else:
        print(
            "Index name is not 'epr_documents', 'observations', 'basic_observations', 'orders'. Returning an empty DataFrame."
        )
        return pd.DataFrame(
            columns=[
                "updatetime",
                "_index",
                "_id",
                "_score",
                "observationdocument_recordeddtm",
                "observation_valuetext_analysed",
            ]
            + fields_list
        )


# # Example usage for epr_documents with personal information:
# epr_documents_personal_df = cohort_searcher_with_terms_and_search_dummy(
#     index_name="epr_documents",
#     fields_list=["client_idcode", "client_firstname", "client_lastname", "client_dob", "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"],
#     term_name="client_idcode.keyword",
#     entered_list=['D3232DUM23'],  # Add more client IDs as needed
#     global_start_year=2022,
#     global_start_month=1,
#     global_end_year=2023,
#     global_end_month=12,
#     search_string=f'updatetime:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
# )

# display(epr_documents_personal_df)


def generate_patient_timeline(client_idcode):

    # Set the logging level to suppress INFO messages
    logging.getLogger("transformers").setLevel(logging.WARNING)
    generator = pipeline("text-generation", model="gpt2")

    probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]  # Adjust as needed

    # Perform a weighted random selection based on the defined probabilities
    num_entries = random.choices(range(1, 6), probabilities)[0]

    starting_age = random.randint(18, 99)
    # Initialize patient demographic information
    patient_info = {
        "client_idcode": client_idcode,
        "Age": starting_age,
        "Gender": random.choice(["Male", "Female"]),
        "DOB": datetime.utcnow() - timedelta(days=365 * starting_age),
    }

    # Generate clinical note summaries
    timeline = []

    # Generate a random timestamp between 1995 and the current time
    current_time = datetime.utcfromtimestamp(
        random.randint(789331200, int(datetime.now().timestamp()))
    )

    for i in range(num_entries):
        entry_timestamp = current_time + timedelta(days=random.randint(1, 30))
        entry_text = generator(
            "Patient presented with:", max_length=50, do_sample=True
        )[0]["generated_text"]

        # Update patient information
        patient_info["Age"] += (entry_timestamp - current_time).days / 365

        # Format entry
        entry_summary = f"Entered on - {entry_timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC:\n{entry_text}\n"
        timeline.append(entry_summary)

        current_time = entry_timestamp

    # Construct the final timeline
    patient_demographics = f"Patient Demographics:\client_idcode: {patient_info['client_idcode']}\client_idcode: {patient_info['Age']:.1f}\nGender: {patient_info['Gender']}\nDOB: {patient_info['DOB'].strftime('%Y-%m-%d')}"
    timeline.insert(0, f"{patient_demographics}\n\nClinical Note Timeline:\n")
    patient_timeline = "\n".join(timeline)

    return patient_timeline


def extract_search_term_obscatalogmasteritem_displayname(search_string):
    # Using regular expression to find the part after 'obscatalogmasteritem_displayname:'
    match = re.search(r"obscatalogmasteritem_displayname:\((.*?)\)", search_string)
    if match:
        # Get the matched group and remove punctuation
        search_term = match.group(1).replace('"', "").replace("'", "").strip()
        # Ignore anything after 'AND' or 'OR'
        search_term = search_term.split("AND", 1)[0].split("OR", 1)[0].strip()
        return search_term
    else:
        return search_string
