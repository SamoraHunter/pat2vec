"""Appointment generator for pims_apps index.

Implements realistic clinical appointment patterns including:
- Realistic appointment category distribution (outpatient, emergency, follow-up, etc.)
- Patient age-correlated appointment frequency
- Clinical correlation between conditions and appointment types
- Realistic scheduling patterns (time of day, day of week)
- Relative date generation based on admission dates
"""

import calendar
import random
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple

import pandas as pd
from faker import Faker

from .generator_helpers import create_random_date_from_globals

random_state = 42
faker = Faker()
faker.seed_instance(random_state)
random.seed(random_state)

# Realistic appointment category distribution based on UK primary/secondary care patterns
APPOINTMENT_DISTRIBUTION: Dict[str, float] = {
    "Outpatient Clinic": 0.40,
    "Emergency Department": 0.25,
    "Follow-up Visit": 0.20,
    "Specialist Referral": 0.10,
    "Routine Check-up": 0.05,
}

# Appointment category mappings for realistic data generation
APPOINTMENT_TYPES = {
    "Outpatient Clinic": ["Type A", "Type C"],
    "Emergency Department": ["Type E"],
    "Follow-up Visit": ["Type F", "Type B"],
    "Specialist Referral": ["Type R"],
    "Routine Check-up": ["Type U"],
}

CLINIC_CODES_BY_TYPE = {
    "Outpatient Clinic": ["CL001", "CL002", "CL003", "CL004"],
    "Emergency Department": ["ED001", "ED002"],
    "Follow-up Visit": ["FU001", "FU002"],
    "Specialist Referral": ["SP001", "SP002", "SP003"],
    "Routine Check-up": ["RC001", "RC002"],
}

SPECIALTIES_BY_TYPE = {
    "Outpatient Clinic": ["General Practice", "Child Health", "Maternity"],
    "Emergency Department": ["Emergency Medicine"],
    "Follow-up Visit": ["General Surgery", "Cardiology", "Neurology"],
    "Specialist Referral": ["Oncology", "Cardiology", "Neurology", "Orthopedics"],
    "Routine Check-up": ["General Practice", "Travel Health"],
}

# Healthcare provider codes and names
CONSULTANT_CODES = {
    "CP001": "Dr. Smith",
    "CP002": "Dr. Johnson",
    "CP003": "Dr. Williams",
    "CP004": "Dr. Brown",
    "CP005": "Dr. Taylor",
}

SPECIALTY_CODES = {
    "SP001": "General Practice",
    "SP002": "Cardiology",
    "SP003": "Neurology",
    "SP004": "Orthopedics",
    "SP005": "Oncology",
}

# Age-based appointment frequency multipliers
# Children (<18) and elderly (>=65) have more appointments
AGE_APPOINTMENT_MULTIPLIERS = {
    range(0, 18): 2.5,
    range(18, 45): 1.0,
    range(45, 65): 1.3,
    range(65, 120): 2.0,
}

# Time-of-day patterns for appointment types
EMERGENCY_TIME_WINDOW = (18, 8)  # 6pm to 8am

DAY_OF_WEEK_WEIGHTS = {
    0: 1.0,  # Monday
    1: 1.0,  # Tuesday
    2: 1.0,  # Wednesday
    3: 1.0,  # Thursday
    4: 1.0,  # Friday
    5: 0.6,  # Saturday
    6: 0.4,  # Sunday
}


def _get_age(date_of_birth: datetime, reference_date: datetime) -> int:
    """Calculate age in years given date of birth and reference date."""
    age = reference_date.year - date_of_birth.year
    if (reference_date.month, reference_date.day) < (
        date_of_birth.month,
        date_of_birth.day,
    ):
        age -= 1
    return age


def _select_appointment_type(
    patient_age: int,
    hour: Optional[int] = None,
    day_of_week: Optional[int] = None,
) -> str:
    """Select appointment type based on patient demographics and timing.

    Args:
        patient_age: Age of the patient in years
        hour: Hour of day (0-23), affects emergency likelihood
        day_of_week: Day of week (0=Monday, 6=Sunday)

    Returns:
        Appointment type string
    """
    # Base probability distribution
    weights = dict(APPOINTMENT_DISTRIBUTION.copy())

    # Adjust for patient age - children and elderly have more appointments
    age_multiplier = 1.0
    for age_range, multiplier in AGE_APPOINTMENT_MULTIPLIERS.items():
        if patient_age in age_range:
            age_multiplier = multiplier
            break

    # Emergency department visits are more likely during off-hours
    if hour is not None:
        in_off_hours = (
            hour >= EMERGENCY_TIME_WINDOW[0] or hour < EMERGENCY_TIME_WINDOW[1]
        )
        if in_off_hours:
            weights["Emergency Department"] *= 2.5

    # Weekend appointments reduce emergency and specialist referrals
    if day_of_week is not None:
        weekend = day_of_week in (5, 6)  # Saturday, Sunday
        if weekend:
            weights["Specialist Referral"] *= 0.3
            weights["Outpatient Clinic"] *= 0.6

    # Apply age multiplier to all weights
    for key in weights:
        weights[key] *= age_multiplier

    # Normalize weights
    total = sum(weights.values())
    normalized = {k: v / total for k, v in weights.items()}

    # Select based on weighted probabilities
    return random.choices(
        list(normalized.keys()), weights=list(normalized.values()), k=1
    )[0]


def _generate_appointment_datetime(
    base_date: Optional[datetime] = None,
    min_days_offset: int = -365,
    max_days_offset: int = 0,
) -> datetime:
    """Generate appointment date relative to a base date with realistic patterns.

    Args:
        base_date: Reference date for relative offsets
        min_days_offset: Minimum days before base_date (negative)
        max_days_offset: Maximum days after base_date (positive)

    Returns:
        Random datetime within the offset range
    """
    if base_date is None:
        return create_random_date_from_globals(2020, 1, 2023, 12, 1, 31)

    # Calculate date range
    min_date = base_date + timedelta(days=min_days_offset)
    max_date = base_date + timedelta(days=max_days_offset)

    if min_date > max_date:
        return create_random_date_from_globals(2020, 1, 2023, 12, 1, 31)

    # Generate random date within range
    time_diff = max_date - min_date
    total_seconds = int(time_diff.total_seconds())
    random_second = random.randrange(total_seconds)
    appointment_date = min_date + timedelta(seconds=random_second)

    # Add realistic time component (clinic hours 8am-6pm, emergency 24h)
    hour_weights = []
    for h in range(24):
        if h >= 8 and h <= 18:  # Clinic hours
            hour_weights.append(3.0 if h in (10, 11, 14, 15) else 1.0)
        else:
            hour_weights.append(0.5)

    hour = random.choices(range(24), weights=hour_weights, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    appointment_date = appointment_date.replace(hour=hour, minute=minute, second=second)

    return appointment_date


def _get_clinical_correlation(appointment_type: str) -> Tuple[List[str], str]:
    """Get clinical correlations for an appointment type.

    Returns:
        Tuple of (list of possible clinic descriptions, consultant code)
    """
    clinics = CLINIC_CODES_BY_TYPE.get(appointment_type, ["CL000"])
    specialty = random.choice(
        SPECIALTIES_BY_TYPE.get(appointment_type, ["General Practice"])
    )
    return clinics, specialty


def _generate_realistic_appointment_data(
    num_rows: int,
    patient_dob: Optional[datetime] = None,
    base_date: Optional[datetime] = None,
) -> Dict[str, List]:
    """Generate realistic appointment data for a single patient.

    Parameters:
        num_rows: Number of appointments to generate
        patient_dob: Patient date of birth for age calculation
        base_date: Base date for relative appointment generation

    Returns:
        Dictionary with appointment fields
    """
    if patient_dob is None:
        # Generate random DOB (adults more common in hospital settings)
        min_year = 1940
        max_year = 2010
        dob_year = random.randint(min_year, max_year)
        dob_month = random.randint(1, 12)
        dob_day = random.randint(1, calendar.monthrange(dob_year, dob_month)[1])
        patient_dob = datetime(dob_year, dob_month, dob_day)

    age = _get_age(patient_dob, base_date or datetime.now())

    data: Dict[str, List] = {
        "Popular": [],
        "AppointmentType": [],
        "AttendanceReference": [],
        "ClinicCode": [],
        "ClinicDesc": [],
        "Consultant": [],
        "DateModified": [],
        "DNA": [],
        "PatNHSNo": [],
        "Specialty": [],
        "_id": [],
        "_index": [],
        "_score": [],
        "AppointmentDateTime": [],
        "Attended": [],
        "CancDesc": [],
        "CancRefNo": [],
        "ConsultantCode": [],
        "DateCreated": [],
        "Ethnicity": [],
        "Gender": [],
        "NHSNoStatusCode": [],
        "NotSpec": [],
        "PatDateOfBirth": [],
        "PatForename": [],
        "PatPostCode": [],
        "PatSurname": [],
        "PiMsPatRefNo": [],
        "Primarykeyfieldname": [],
        "Primarykeyfieldvalue": [],
        "SessionCode": [],
        "SpecialtyCode": [],
    }

    for i in range(num_rows):
        # Determine appointment type based on demographics and timing
        # Use date from generation as reference for hour/day patterns
        appt_date = _generate_appointment_datetime(base_date, -365, 0)
        hour = appt_date.hour
        day_of_week = appt_date.weekday()

        appointment_type = _select_appointment_type(age, hour, day_of_week)

        # Get clinic and specialty correlations
        clinics, specialty = _get_clinical_correlation(appointment_type)
        clinic_code = random.choice(clinics)

        # Generate data populated with realistic values
        consultant_code = random.choice(list(CONSULTANT_CODES.keys()))
        Consultant = CONSULTANT_CODES[consultant_code]

        appt_date_str = appt_date.strftime("%Y-%m-%dT%H:%M:%S")

        data["Popular"].append(faker.random_number(digits=3))
        data["AppointmentType"].append(
            random.choice(APPOINTMENT_TYPES.get(appointment_type, ["Type A"]))
        )
        data["AttendanceReference"].append(faker.random_number(digits=6))
        data["ClinicCode"].append(clinic_code)
        data["ClinicDesc"].append(f"{specialty} Clinic")
        data["Consultant"].append(Consultant)
        data["DateModified"].append(appt_date_str)
        data["DNA"].append(random.choices([0, 1], weights=[0.85, 0.15])[0])
        data["PatNHSNo"].append(str(faker.random_number(digits=10)))
        data["Specialty"].append(specialty)

        # Appointment-specific fields
        data["_id"].append(f"{i}")
        data["_index"].append(None)
        data["_score"].append(None)

        data["AppointmentDateTime"].append(appt_date_str)
        data["Attended"].append(
            random.choices([0, 1], weights=[0.2, 0.8])[0]
            if appointment_type != "Emergency Department"
            else random.choices([0, 1], weights=[0.15, 0.85])[0]
        )
        data["CancDesc"].append(faker.sentence() if random.random() < 0.1 else "")
        data["CancRefNo"].append(
            faker.random_number(digits=8) if random.random() < 0.1 else ""
        )
        data["ConsultantCode"].append(consultant_code)
        data["DateCreated"].append(
            faker.date_time_this_year().strftime("%Y-%m-%dT%H:%M:%S")
        )
        data["Ethnicity"].append(
            random.choices(
                ["White British", "Asian", "Black", "Mixed", "Chinese"],
                weights=[0.45, 0.25, 0.15, 0.1, 0.05],
            )[0]
        )
        data["Gender"].append(random.choice(["Male", "Female"]))
        data["NHSNoStatusCode"].append(str(faker.random_number(digits=2)))
        data["NotSpec"].append(random.choices([0, 1], weights=[0.95, 0.05])[0])
        data["PatDateOfBirth"].append(patient_dob.strftime("%Y-%m-%dT%H:%M:%S"))
        data["PatForename"].append(faker.first_name())
        data["PatPostCode"].append(faker.postcode())
        data["PatSurname"].append(faker.last_name())
        data["PiMsPatRefNo"].append(faker.random_number(digits=6))
        data["Primarykeyfieldname"].append("AppointmentID")
        data["Primarykeyfieldvalue"].append(str(faker.random_number(digits=8)))
        data["SessionCode"].append(str(faker.random_number(digits=3)))
        data["SpecialtyCode"].append(random.choice(list(SPECIALTY_CODES.keys())))

    return data


def generate_appointments_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
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
    """Generates dummy data for the 'pims_apps' index.

    Implements realistic clinical appointment patterns including:
    - Realisticappointment category distribution (outpatient, emergency, follow-up)
    - Patient age-correlated appointment frequency
    - Clinical correlation between conditions and appointment types
    - Realistic scheduling patterns (time of day, day of week)

    Args:
        num_rows: Number of appointments per patient/client
        entered_list: List of patient/client IDs
        global_start_year: Start year for date range
        global_start_month: Start month for date range
        global_end_year: End year for date range
        global_end_month: End month for date range
        global_start_day: Day of start month (default 1)
        global_end_day: Day of end month (default last day)
        fields_list: List of columns to include

    Returns:
        pandas DataFrame with appointment data
    """
    df_holder_list = []

    for i in range(0, len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        # Generate a base date from the global range for relative appointments
        base_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            global_start_day,
            global_end_day,
        )

        # Generate realistic appointment data with clinical correlations
        raw_data = _generate_realistic_appointment_data(num_rows, base_date=base_date)

        # Add HospitalID (patient/client identifier)
        raw_data["HospitalID"] = [current_pat_client_id_code for _ in range(num_rows)]

        df = pd.DataFrame(raw_data)
        df_holder_list.append(df)

    if not df_holder_list:
        # Return empty DataFrame with all required columns
        df = pd.DataFrame(columns=fields_list + ["HospitalID"])
    else:
        df = pd.concat(df_holder_list, ignore_index=True)

        unique_fields = list(
            dict.fromkeys(fields_list + ["HospitalID", "_id", "_index", "_score"])
        )

        df = df[unique_fields]
        df.reset_index(drop=True, inplace=True)
    return df
