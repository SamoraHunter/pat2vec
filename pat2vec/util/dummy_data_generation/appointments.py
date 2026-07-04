"""Appointment generator for pims_apps index."""

import random
from typing import List

import pandas as pd
from faker import Faker

from .generator_helpers import create_random_date_from_globals

random_state = 42
faker = Faker()
faker.seed_instance(random_state)
random.seed(random_state)


def generate_appointments_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = [
        "Popular",
        "AppointmentType",
        "AttendanceReference",
        "ClinicCode",
        "ClinicDesc",
        "Consultant",
        "DateModified",
        "DNA",
        "HospitalID",
        "PatNHSNo",
        "Specialty",
        "_id",
        "_index",
        "_score",
        "AppointmentDateTime",
        "Attended",
        "CancDesc",
        "CancRefNo",
        "ConsultantCode",
        "DateCreated",
        "Ethnicity",
        "Gender",
        "NHSNoStatusCode",
        "NotSpec",
        "PatDateOfBirth",
        "PatForename",
        "PatPostCode",
        "PatSurname",
        "PiMsPatRefNo",
        "Primarykeyfieldname",
        "Primarykeyfieldvalue",
        "SessionCode",
        "SpecialtyCode",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'pims_apps' index."""
    df_holder_list = []

    for i in range(0, len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        data = {
            "Popular": [faker.random_number(digits=3) for _ in range(num_rows)],
            "AppointmentType": [
                faker.random_element(["Type A", "Type B", "Type C"])
                for _ in range(num_rows)
            ],
            "AttendanceReference": [
                faker.random_number(digits=6) for _ in range(num_rows)
            ],
            "ClinicCode": [str(faker.random_number(digits=4)) for _ in range(num_rows)],
            "ClinicDesc": [faker.word() for _ in range(num_rows)],
            "Consultant": [faker.name() for _ in range(num_rows)],
            "DateModified": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "DNA": [faker.random_element([0, 1]) for _ in range(num_rows)],
            "HospitalID": [current_pat_client_id_code for _ in range(num_rows)],
            "PatNHSNo": [str(faker.random_number(digits=10)) for _ in range(num_rows)],
            "Specialty": [
                faker.random_element(["Specialty A", "Specialty B", "Specialty C"])
                for _ in range(num_rows)
            ],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
            "AppointmentDateTime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "Attended": [faker.random_element([0, 1]) for _ in range(num_rows)],
            "CancDesc": [faker.sentence() for _ in range(num_rows)],
            "CancRefNo": [faker.random_number(digits=8) for _ in range(num_rows)],
            "ConsultantCode": [
                str(faker.random_number(digits=4)) for _ in range(num_rows)
            ],
            "DateCreated": [
                faker.date_time_this_year().strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "Ethnicity": [
                faker.random_element(["Ethnicity A", "Ethnicity B", "Ethnicity C"])
                for _ in range(num_rows)
            ],
            "Gender": [
                faker.random_element(["Male", "Female"]) for _ in range(num_rows)
            ],
            "NHSNoStatusCode": [
                str(faker.random_number(digits=2)) for _ in range(num_rows)
            ],
            "NotSpec": [faker.random_element([0, 1]) for _ in range(num_rows)],
            "PatDateOfBirth": [faker.date_of_birth() for _ in range(num_rows)],
            "PatForename": [faker.first_name() for _ in range(num_rows)],
            "PatPostCode": [faker.postcode() for _ in range(num_rows)],
            "PatSurname": [faker.last_name() for _ in range(num_rows)],
            "PiMsPatRefNo": [faker.random_number(digits=6) for _ in range(num_rows)],
            "Primarykeyfieldname": [faker.word() for _ in range(num_rows)],
            "Primarykeyfieldvalue": [
                str(faker.random_number(digits=4)) for _ in range(num_rows)
            ],
            "SessionCode": [
                str(faker.random_number(digits=3)) for _ in range(num_rows)
            ],
            "SpecialtyCode": [
                str(faker.random_number(digits=4)) for _ in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list, ignore_index=True)

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)
    return df
