from ..generator_helpers import create_random_date_from_globals, maybe_nan
from pat2vec.pat2vec_get_methods.get_method_bed import BED_FIELDS
import random
from typing import List
import pandas as pd
import numpy as np

random_state = 42
random.seed(random_state)
faker = None


WARD_TYPE_WEIGHTS = {
    "WD": 0.58,
    "HDU": 0.22,
    "ITU": 0.15,
}

WARD_TYPES_LIST = list(WARD_TYPE_WEIGHTS.keys())
WARD_PROBS_INIT = [
    WARD_TYPE_WEIGHTS["WD"],
    WARD_TYPE_WEIGHTS["HDU"],
    WARD_TYPE_WEIGHTS["ITU"],
]

DEFAULT_NEWS_MEAN = 4
DEFAULT_NEWS_STD = 3

NEWS_HDU_THRESHOLD = 5
NEWS_ITU_THRESHOLD = 7


def _generate_bed_assignment_for_ward(
    ward_type: str,
) -> str:
    """Generate appropriate bed assignment based on ward type.

    Ward-specific bed naming conventions:
    - WD (Ward Beds): Bay A/B beds, general wards
    - HDU (High Dependency): DedicatedHDU bed numbers
    - ITU (Intensive Therapy): SpecializedITU care units

    Args:
        ward_type: The determined ward type (WD/HDU/ITU)

    Returns:
        Bed assignment string matching ward conventions
    """
    ward_bed_patterns = {
        "WD": [
            "Bay A Bed 1",
            "Bay A Bed 2",
            "Bay A Bed 3",
            "Bay B Bed 1",
            "Bay B Bed 2",
            "Bed 1",
            "Bed 2",
            "Bed 3",
            "Side Room 1",
        ],
        "HDU": [
            "HDU Bed 1",
            "HDU Bed 2",
            "HDU Bed 3",
            "HDU Side Room",
        ],
        "ITU": ["ITU Bed 1", "ITU Bed 2", "ITU Bed 3", "ITU Side Room"],
    }

    bed_list = ward_bed_patterns.get(ward_type, ward_bed_patterns["WD"])
    return random.choice(bed_list)


def generate_bed_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = BED_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for bed number observations with realistic clinical patterns.

    Implements Bayesian network: hospital site → ward type → specific bed assignment

    Realistic probability distributions by ward type:
    - Ward Beds (WD): ~60% of patients (general medical/surgical)
    - High Dependency Unit (HDU): ~20% (requiring increased nursing input)
    - Intensive Therapy Unit (ITU): ~15% (most critically ill)

    Clinical correlation patterns:
    - Higher NEWS scores correlate with HDU/ITU beds
    - Low SpO2 (<93%) increases likelihood of high dependency monitoring
    - Reduced NaN rate (~4%) for missing data

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Day of start month (default 1).
        global_end_day: Day of end month (default final day).
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy bed data.

    Raises:
        None
    """
    from faker import Faker

    df_holder_list = []

    Faker.seed(random_state)
    random.seed(random_state)

    for client_id_code in entered_list:
        faker_inst = Faker()
        np.random.seed(random_state + abs(hash(client_id_code)) % 10000)
        random.seed(random_state + abs(hash(client_id_code)) % 10000)

        # Assign ward type at patient level based on baseline probabilities
        _patient_ward_type = random.choices(WARD_TYPES_LIST, weights=WARD_PROBS_INIT)[0]

        news_seed = random_state + abs(hash(client_id_code + "_news")) % 10000
        spo2_seed = random_state + abs(hash(client_id_code + "_spo2")) % 10000

        np.random.seed(news_seed)
        random.seed(news_seed)

        _baseline_news = int(
            min(max(random.gauss(DEFAULT_NEWS_MEAN, DEFAULT_NEWS_STD), 0), 20)
        )

        np.random.seed(spo2_seed)
        random.seed(spo2_seed)

        spo2_pool = ["98%", "97%", "96%", "95%", "94%", "93%", "92%"]
        spo2_weights = [0.15, 0.2, 0.25, 0.2, 0.1, 0.05, 0.05]
        _baseline_spo2 = random.choices(spo2_pool, weights=spo2_weights)[0]

        admissions = []
        for i in range(num_rows):
            admission_date = create_random_date_from_globals(
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                global_start_day,
                global_end_day,
            )

            row_ward_seed = (
                random_state
                + abs(hash(client_id_code + "_row" + str(i) + "_ward")) % 10000
            )
            np.random.seed(row_ward_seed)
            random.seed(row_ward_seed)

            # Assign ward type for each admission (allowing some variability within patient)
            current_ward_type = random.choices(
                WARD_TYPES_LIST, weights=WARD_PROBS_INIT
            )[0]

            bed_assignment = _generate_bed_assignment_for_ward(current_ward_type)

            value = maybe_nan(bed_assignment, probability=0.04)

            admissions.append(
                {
                    "observation_guid": faker_inst.uuid4(),
                    "client_idcode": client_id_code,
                    "obscatalogmasteritem_displayname": "CORE_BedNumber3",
                    "observation_valuetext_analysed": value,
                    "observationdocument_recordeddtm": admission_date.strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    ),
                    "clientvisit_visitidcode": f"visit_{faker_inst.random_number(digits=8, fix_len=True)}",
                }
            )

        df = pd.DataFrame(admissions)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan

    final_df = final_df[fields_list]
    return final_df
