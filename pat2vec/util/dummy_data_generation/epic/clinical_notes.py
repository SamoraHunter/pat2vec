import calendar
import random
from datetime import datetime, timedelta
from typing import List, Optional

import numpy as np
import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
Faker.seed(random_state)
np.random.seed(random_state)
random.seed(random_state)
faker = Faker()


SPECIALTY_DISTRIBUTION = {
    "General Medicine": 0.30,
    "Cardiology": 0.15,
    "Surgery": 0.12,
    "Neurology": 0.10,
    "Orthopedics": 0.08,
    "Psychiatry": 0.07,
    "Other": 0.18,
}

NOTE_TYPE_DISTRIBUTION = {
    "Progress Note": 0.45,
    "Consultation Note": 0.25,
    "Discharge Summary": 0.15,
    "Emergency Note": 0.10,
    "Admission Note": 0.05,
}

MEDICAL_CONDITIONS = {
    "General Medicine": [
        "hypertension",
        "type 2 diabetes",
        "hyperlipidemia",
        "upper respiratory infection",
        "acute bronchitis",
        "back pain",
        "headache",
        "dizziness",
        "arthritis",
        "anemia",
    ],
    "Cardiology": [
        "chest pain",
        "arrhythmia",
        "heart failure",
        "coronary artery disease",
        "acute myocardial infarction",
        "palpitations",
        "syncope",
        "valvular heart disease",
    ],
    "Surgery": [
        "pre-operative evaluation",
        "post-operative complication",
        "wound infection",
        "abdominal pain",
        "appendicitis",
        "galbladder disease",
        "hernia repair",
    ],
    "Neurology": [
        "stroke",
        "seizure",
        "headache",
        "dizziness",
        "nerve pain",
        "multiple sclerosis",
        "parkinsonism",
        "peripheral neuropathy",
    ],
    "Orthopedics": [
        "fracture",
        "joint pain",
        "muscle strain",
        "ligament tear",
        "spinal stenosis",
        "osteoarthritis",
        "rotator cuff injury",
    ],
    "Psychiatry": [
        "depression",
        "anxiety disorder",
        "bipolar disorder",
        "psychosis",
        "insomnia",
        "panic attacks",
        "adjustment disorder",
    ],
    "Other": [
        "allergy",
        "asthma",
        "chronic kidney disease",
        "COPD",
        "thyroid disorder",
    ],
}

CLINICAL_TEMPLATES_PROGRESS = {
    "General Medicine": [
        "Patient presents with stable chronic conditions. Hypertension managed with medication. "
        "Type 2 diabetes under control with diet and metformin. Patient reports mild back pain "
        "but mobility remains good. Recommend continue current medications and follow up in 3 months.",
        "Chronic condition follow-up. Patient reports blood pressure control at home. "
        "Lipid levels improved. No new symptoms. Advise maintain exercise program and annual flu shot.",
    ],
    "Cardiology": [
        "Patient reports intermittent chest pain, described as pressure-like, lasting 5-10 minutes. "
        "Pain occurs with exertion and relieved by rest. ECG shows non-specific ST changes. "
        "Scheduled for stress echo. Patient advised to avoid strenuous activity until further testing.",
        "Heart failure follow-up. Patient reports improved dyspnea on exertion. Weight stable at 75 kg. "
        "Current meds: lisinopril, carvedilol, furosemide. No orthopnea or PND. Recommend continue "
        "current regimen and monitor sodium intake.",
    ],
    "Surgery": [
        "Post-operative day 3 following appendectomy. Patient tolerating oral diet without nausea. "
        "Wound site clean with minimal drainage. Vital signs stable. Discharge planning initiated. "
        "Patient to follow up with surgical clinic in 1 week.",
        "Pre-operative assessment for elective knee arthroscopy. Patient has mild osteoarthritis "
        "documented on recent MRI. Cardiac clearance obtained. Patient advised to stop NSAIDs "
        "5 days pre-op and arrange post-op care.",
    ],
    "Neurology": [
        "Patient presents with recurrent headache, pattern consistent with migraine without aura. "
        "Last episode lasted 48 hours. Medication: sumatriptan provided partial relief. Patient "
        "reports frequency has decreased with topiramate. Counselled on trigger avoidance.",
        "Follow-up for stroke patient. Left-sided weakness improved with therapy. Patient now ambulates "
        "with cane. Speech therapist reports progress. Continue aspirin, statin, and rehab exercises.",
    ],
    "Orthopedics": [
        "Patient presents with acute knee injury following fall. Physical exam reveals swelling and "
        "tenderness medially. X-ray negative for fracture. Diagnosis: medial collateral strain. "
        "RICE protocol prescribed. Follow up in 2 weeks.",
        "Post-operative day 7 following ACL reconstruction. Surgical site healing well without infection. "
        "Patient began physical therapy yesterday, progressing well. Weight-bearing as tolerated. "
        "Continue crutches with partial weight bearing.",
    ],
    "Psychiatry": [
        "Patient reports worsening depressive symptoms over past 2 weeks. Sleep disturbed, anhedonia, "
        "low energy. Started on sertraline 50mg daily. Patient agrees to weekly therapy. Follow up in 1 week.",
        "Anxiety assessment. Patient reports panic attacks occurring 2-3 times per week, triggered by work stress. "
        "Previously on alprazolam, transitioned to escitalopram. Cognitive behavioral therapy recommended. "
        "Patient to practice relaxation techniques.",
    ],
}

CLINICAL_TEMPLATES_CONSULTATION = {
    "General Medicine": [
        "Consultation requested for management of hypertension. Patient has had multiple elevated readings "
        "at home. Started on amlodipine 5mg daily. Recommend home monitoring twice daily. Return in 2 weeks.",
    ],
    "Cardiology": [
        "Patient presents with palpitations and dizziness. Holter monitor reveals occasional PVCs and "
        "nonsustained VT. Patient reports symptoms correlate with device readings. Recommended continue "
        "metoprolol and schedule echocardiogram.",
    ],
    "Surgery": [
        "Pre-operative evaluation for total hip replacement. Patient has moderate osteoarthritis. "
        "Cardiac risk assessment completed. Anesthesia consult favorable. Patient optimized with weight loss "
        "and smoking cessation. Surgery scheduled in 2 weeks.",
        "Post-operative evaluation for cholecystectomy. Patient recovered well from procedure. No bile leak. "
        "Issues identified. Tolerating diet, discharge planned for next day. Follow up with primary care.",
    ],
    "Neurology": [
        "Consultation requested for acute onset right-sided weakness and aphasia. MRI confirms left cerebral "
        "artery territory infarct. Patient presents with expressive aphasia and mild receptive impairment. "
        "Thrombectomy not eligible due to time window. Started on antiplatelet therapy and rehab evaluation.",
    ],
    "Orthopedics": [
        "Patient reports chronic low back pain radiating to bilateral lower extremities. MRI shows L4-L5 "
        "disc herniation with nerve root impingement. Patient has failed conservative management. "
        "Recommended epidural steroid injection for pain relief.",
    ],
    "Psychiatry": [
        "Evaluation requested for depression and anxiety symptoms. Patient reports persistent sad mood, "
        "decreased interest, and sleep disturbance x 6 months. Diagnoses: Major Depressive Disorder, "
        "Generalized Anxiety Disorder. Recommends SSRI therapy and psychotherapy referral.",
    ],
}

CLINICAL_TEMPLATES_DISCHARGE_SUMMARY = {
    "General Medicine": [
        "Discharge Summary: Hospital course uneventful. Patient diagnosed with pneumonia, treated with "
        "antibiotics. Discharged home in stable condition. Follow up with PCP in 1 week. Continue "
        "antibiotics for full course.",
    ],
    "Cardiology": [
        "Discharge Summary following admission for acute myocardial infarction. Cardiac catheterization "
        "revealed 70% stenosis of LCX. Stent placed successfully. Patient discharged on aspirin, clopidogrel, "
        "beta-blocker, and statin. Cardiology follow up in 2 weeks.",
    ],
    "Surgery": [
        "Discharge Summary: Patient underwent laparoscopic cholecystectomy for symptomatic cholelithiasis. "
        "Procedure uncomplicated. Tolerating regular diet. Discharged home on post-operative day 2. "
        "Follow up with surgery clinic in 10 days.",
    ],
    "Neurology": [
        "Discharge Summary: Patient admitted for bacterial meningitis. Started on antibiotics and dexamethasone. "
        "CSF culture pending but clinical improvement noted. Continue IV antibiotics. Neurology follow up in 1 week "
        "for outpatient testing.",
    ],
    "Orthopedics": [
        "Discharge Summary following admission for hip fracture with surgical repair. Intraoperative "
        "complications none. Patient mobilized with physical therapy. Discharged to skilled nursing facility "
        "for rehab. Ortho follow up in 2 weeks.",
    ],
    "Psychiatry": [
        "Discharge Summary: Patient admitted for psychotic episode. Medications stabilized on risperidone. "
        "Patient now cooperative and oriented. Safe for discharge with outpatient psychiatry follow up in 3 days.",
    ],
}

CLINICAL_TEMPLATES_EMERGENCY = {
    "General Medicine": [
        "Emergency Department note: Patient arrived via ambulance with acute respiratory distress. "
        "Oxygen saturation 88% on room air. Chest x-ray shows infiltrate consistent with pneumonia. "
        "Started on antibiotics, steroids, and oxygen therapy. Admitted to hospital.",
    ],
    "Cardiology": [
        "Emergency Note: Patient presents with severe substernal chest pain radiating to left arm. "
        "ECG demonstrates ST elevation in inferior leads. Cardiac enzymes elevated. Activated STEMI "
        "protocol. Transferred to cath lab for primary PCI.",
    ],
    "Neurology": [
        "Emergency Department evaluation for acute onset right facial droop and left-sided weakness. "
        "Symptoms started approximately 2 hours prior to arrival. NIHSS score 8. CT head without contrast "
        "non-revealing hemorrhage. Stroke alert activated, patient eligible for thrombectomy.",
    ],
}

CLINICAL_TEMPLATES_ADMISSION_NOTE = {
    "General Medicine": [
        "Admission Note: Patient presents with acute exacerbation of asthma. PEF 50% predicted. Started on "
        "nebulizers and oral steroids. Monitor for improvement. Rule out pneumonia with chest x-ray.",
    ],
    "Cardiology": [
        "Admission Note: Patient admitted for unstable angina. Chest pain occurs at rest. ECG shows ST depression. "
        "Cardiac enzymes pending. Held anticoagulation, started on heparin. Cardiology consultation obtained.",
    ],
}

AGE_MODIFIERS = {
    (0, 18): {
        "General Medicine": [
            "otitis",
            "strep throat",
            "epiphyseal injury",
            "asthma exacerbation",
        ],
        "Cardiology": ["congenital heart defect", "arrhythmia", "murmur"],
        "Surgery": ["appendicitis", "trauma", "cryptorchidism"],
        "Neurology": ["febrile seizure", "headache", "concussion"],
        "Orthopedics": ["fracture", "growth plate injury", "scoliosis"],
        "Psychiatry": ["ADHD", "autism spectrum", "behavioral issues"],
    },
    (18, 65): {
        "General Medicine": ["hypertension", "diabetes", "back pain", "migraine"],
        "Cardiology": ["chest pain", "arrhythmia", "heart failure"],
        "Surgery": ["appendicitis", "hernia", "gallstones"],
        "Neurology": ["migraine", "seizure", "stroke"],
        "Orthopedics": ["fracture", "sports injury", "carpal tunnel"],
        "Psychiatry": ["depression", "anxiety", "substance use"],
    },
    (65, 120): {
        "General Medicine": [
            "hypertension",
            "type 2 diabetes",
            "arthritis",
            "osteoporosis",
        ],
        "Cardiology": ["heart failure", "arrhythmia", "CAD", "syncope"],
        "Surgery": ["pre-op eval", "post-op complication", "fall injury"],
        "Neurology": ["stroke", "dementia", "parkinsonism", "fall"],
        "Orthopedics": ["fracture", "osteoarthritis", "spinal stenosis"],
        "Psychiatry": ["depression", "anxiety", "cognitive decline"],
    },
}

NOTE_PREFIXES = {
    "Progress Note": ["Daily", "Routine", "Evening", "Morning", "Night"],
    "Consultation Note": ["Specialty", "Request", "Follow-up"],
    "Discharge Summary": ["Discharge", "Transition"],
    "Emergency Note": ["Emergency", "Urgent", "Rapid Assessment"],
    "Admission Note": ["Admission", "Initial Evaluation", "Working Diagnosis"],
}


def generate_age_appropriate_conditions(specialty: str, patient_age: int) -> List[str]:
    """Generates age-appropriate medical conditions based on specialty and age bracket."""
    for age_range, conditions in AGE_MODIFIERS.items():
        if age_range[0] <= patient_age < age_range[1]:
            return conditions.get(specialty, ["general condition"])
    return MEDICAL_CONDITIONS[specialty]


def select_specialty_by_distribution() -> str:
    """Selects a medical specialty based on realistic distribution."""
    specialties = list(SPECIALTY_DISTRIBUTION.keys())
    weights = list(SPECIALTY_DISTRIBUTION.values())
    return random.choices(specialties, weights=weights, k=1)[0]


def select_note_type_by_distribution() -> str:
    """Selects a note type based on realistic documentation distribution."""
    note_types = list(NOTE_TYPE_DISTRIBUTION.keys())
    weights = list(NOTE_TYPE_DISTRIBUTION.values())
    return random.choices(note_types, weights=weights, k=1)[0]


def get_note_template(specialty: str, note_type: str, patient_age: int) -> str:
    """Gets an appropriate clinical note template based on specialty, note type, and age."""
    condition = generate_age_appropriate_conditions(specialty, patient_age)

    templates_map = {
        "Progress Note": CLINICAL_TEMPLATES_PROGRESS,
        "Consultation Note": CLINICAL_TEMPLATES_CONSULTATION,
        "Discharge Summary": CLINICAL_TEMPLATES_DISCHARGE_SUMMARY,
        "Emergency Note": CLINICAL_TEMPLATES_EMERGENCY,
        "Admission Note": CLINICAL_TEMPLATES_ADMISSION_NOTE,
    }

    template_collection = templates_map.get(note_type, {})
    templates = template_collection.get(specialty, [])

    if not templates:
        return generate_generic_note(specialty, note_type)

    base_template = random.choice(templates)
    condition_text = (
        random.choice(condition) if isinstance(condition, list) else str(condition)
    )
    return replace_condition_in_template(base_template, condition_text)


def replace_condition_in_template(template: str, condition: str) -> str:
    """Replaces placeholder conditions in a template with realistic medical terms."""
    placeholders = [
        "general condition",
        "chronic condition",
        "medical condition",
        "health issue",
        "clinical symptom",
    ]

    result = template
    for placeholder in placeholders:
        if placeholder in result.lower():
            result = result.replace(placeholder, condition)
            break

    return result


def generate_generic_note(specialty: str, note_type: str) -> str:
    """Generates a generic clinical note when specific templates are unavailable."""
    templates = {
        "Progress Note": f"Patient status stable. {specialty} follow-up noted. No significant changes.",
        "Consultation Note": f"{specialty} consultation requested. Evaluation completed with recommendations.",
        "Discharge Summary": f"Patient discharged following treatment at {specialty}. Follow-up recommended.",
        "Emergency Note": f"Urgent evaluation at {specialty}. Patient stabilized and disposition arranged.",
        "Admission Note": f"Initial admission assessment at {specialty}. Working diagnosis established.",
    }
    return templates.get(note_type, "Evaluation completed.")


def determine_document_date(
    admission_date: Optional[datetime],
    note_type: str,
    global_start_dt: datetime,
    global_end_dt: datetime,
) -> datetime:
    """Determines realistic document creation date based on note type and context."""
    if note_type == "Emergency Note":
        return create_random_date_from_globals(
            global_start_dt.year,
            global_start_dt.month,
            global_end_dt.year,
            global_end_dt.month,
            global_start_day=global_start_dt.day,
            global_end_day=global_end_dt.day,
        )

    if note_type == "Discharge Summary" and admission_date:
        discharge_offset = random.randint(1, 7)
        discharge_date = admission_date + timedelta(days=discharge_offset)
        max_day = calendar.monthrange(discharge_date.year, discharge_date.month)[1]
        return create_random_date_from_globals(
            discharge_date.year,
            discharge_date.month,
            global_end_dt.year,
            global_end_dt.month,
            discharge_date.day,
            min(global_end_dt.day, max_day),
        )

    if note_type == "Consultation Note":
        consult_day = random.randint(1, 5)
        consult_date = (
            admission_date + timedelta(days=consult_day) if admission_date else None
        )
        if consult_date:
            max_day = calendar.monthrange(consult_date.year, consult_date.month)[1]
            return create_random_date_from_globals(
                consult_date.year,
                consult_date.month,
                global_end_dt.year,
                global_end_dt.month,
                consult_date.day,
                min(global_end_dt.day, max_day),
            )

    return create_random_date_from_globals(
        global_start_dt.year,
        global_start_dt.month,
        global_end_dt.year,
        global_end_dt.month,
    )


def generate_document_name(specialty: str, note_type: str, patient_age: int) -> str:
    """Generates realistic document names based on specialty and note type."""
    prefixes = NOTE_PREFIXES.get(note_type, ["Note"])

    if note_type == "Progress Note":
        return f"{random.choice(prefixes)} {specialty} Progress Note"
    elif note_type == "Consultation Note":
        return f"Consultation - {specialty}"
    elif note_type == "Discharge Summary":
        return f"{specialty} Discharge Summary"
    elif note_type == "Emergency Note":
        return f"Emergency Department Evaluation - {specialty}"
    elif note_type == "Admission Note":
        return f"{random.choice(prefixes)} {specialty} Admission Note"

    return f"{note_type} - {specialty}"


def get_patient_timeline_dummy(
    client_id_code: str, patient_age: Optional[int] = None
) -> str:
    """Gets a realistic dummy patient timeline with clinical note content."""
    specialty = select_specialty_by_distribution()
    note_type = select_note_type_by_distribution()

    if patient_age is None:
        patient_age = random.randint(25, 75)

    content = get_note_template(specialty, note_type, patient_age)
    document_name = generate_document_name(specialty, note_type, patient_age)

    return (
        f"CLINICAL NOTE: {document_name}\n\nCONTENT: {content}\n\n"
        f"SPECIALTY: {specialty}\nNOTE_TYPE: {note_type}\nAGE: {patient_age}"
    )


def generate_epic_clinical_notes_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
    use_GPT: bool = False,
    patient_age_override: Optional[int] = None,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Content",
        "document_Name",
        "document_EncounterEpicCsn",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_clinical_notes' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        use_GPT: If True, uses a text generation model for clinical notes content.
        patient_age_override: Optional specific age to use for all patients (optional range 0-120).
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy clinical note data.
    Raises:
        None
    """
    df_holder_list = []

    patient_age = patient_age_override
    if patient_age is None:
        patient_age = random.randint(18, 85)

    global_start_dt = datetime(
        global_start_year, global_start_month, global_start_day, 0, 0, 0
    )
    global_end_dt = datetime(
        global_end_year,
        global_end_month,
        calendar.monthrange(global_end_year, global_end_month)[1],
        23,
        59,
        59,
    )

    for client_id_code in entered_list:
        note_data = []

        specialty = select_specialty_by_distribution()
        patient_per_note_age = random.randint(18, 85)

        for _ in range(num_rows):
            note_type = select_note_type_by_distribution()

            content = get_note_template(specialty, note_type, patient_per_note_age)
            document_name = generate_document_name(
                specialty, note_type, patient_per_note_age
            )

            doc_date = determine_document_date(
                None, note_type, global_start_dt, global_end_dt
            )

            note_data.append(
                {
                    "document_PatientDurableKey": client_id_code,
                    "document_CreatedWhen": doc_date.strftime("%Y-%m-%dT%H:%M:%S"),
                    "document_Content": content,
                    "document_Name": document_name,
                    "document_EncounterEpicCsn": faker.random_number(digits=10),
                    "id": faker.uuid4(),
                }
            )

        df_holder_list.append(pd.DataFrame(note_data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    target_col = "document_Content"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)
    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_medical_history_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Diagnosis",
        "document_DiagnosisConcepts",
        "document_Name",
        "document_Comment",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_medical_history' index.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy medical history data.
    Raises:
        None
    """
    df_holder_list = []
    for client_id_code in entered_list:
        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_Diagnosis": [faker.word() for _ in range(num_rows)],
            "document_DiagnosisConcepts": [faker.word() for _ in range(num_rows)],
            "document_Name": [faker.sentence(nb_words=2) for _ in range(num_rows)],
            "document_Comment": [faker.sentence() for _ in range(num_rows)],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    target_col = "document_Comment"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)
    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_UpdatedWhen",
        "document_Name",
        "document_Content",
        "document_OrderClass",
        "document_OrderDate",
        "document_OrderStatus",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_orders' index.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy orders data.
    Raises:
        None
    """
    df_holder_list = []
    for client_id_code in entered_list:
        order_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            global_start_day,
            global_end_day,
        )
        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_UpdatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_Name": [faker.word() for _ in range(num_rows)],
            "document_Content": [faker.sentence() for _ in range(num_rows)],
            "document_OrderClass": [
                random.choice(["Medication", "Lab", "Imaging"]) for _ in range(num_rows)
            ],
            "document_OrderDate": [
                int(order_date.timestamp() * 1000) for _ in range(num_rows)
            ],
            "document_OrderStatus": [
                random.choice(["Completed", "Pending", "Cancelled"])
                for _ in range(num_rows)
            ],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    target_col = "document_Content"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)
    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_patient_timeline(client_id_code: str) -> Optional[str]:
    """Generates a patient timeline using GPT."""
    return None
