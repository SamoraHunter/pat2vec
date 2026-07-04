# Import observation generators from observations subpackage

# Import epic generators

# Import epic medical history (may need separate handling if not in subpackage)
try:
    from .epic.orders import generate_epic_medical_history_data
except ImportError:

    def generate_epic_medical_history_data(
        num_rows,
        entered_list,
        global_start_year,
        global_start_month,
        global_end_year=2023,
        global_end_month=12,
        fields_list=None,
    ):
        # Placeholder implementation - define locally if not available
        import random
        import uuid
        from .generator_helpers import create_random_date_from_globals

        if fields_list is None:
            fields_list = [
                "document_PatientDurableKey",
                "document_CreatedWhen",
                "document_Diagnosis",
                "document_Name",
                "id",
            ]

        df_holder = []
        for client_id_code in entered_list:
            data = {
                "document_PatientDurableKey": [client_id_code] * num_rows,
                "document_CreatedWhen": [
                    create_random_date_from_globals(
                        global_start_year,
                        global_start_month,
                        global_end_year,
                        global_end_month,
                    ).strftime("%Y-%m-%dT%H:%M:%S")
                    for _ in range(num_rows)
                ],
                "document_Diagnosis": [
                    f"Condition_{random.randint(1, 10)}" for _ in range(num_rows)
                ],
                "document_Name": [
                    f"Note_{random.randint(1, 5)}" for _ in range(num_rows)
                ],
                "id": [str(uuid.uuid4())[:8] for _ in range(num_rows)],
            }
            df_holder.append(__import__("pandas").DataFrame(data))

        if not df_holder:
            return __import__("pandas").DataFrame(columns=fields_list)

        df = __import__("pandas").concat(df_holder, ignore_index=True)
        unique_fields = list(dict.fromkeys(fields_list + ["id"]))
        for f in unique_fields:
            if f not in df.columns:
                df[f] = None
        return df[unique_fields]
