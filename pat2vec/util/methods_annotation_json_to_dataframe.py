import numpy as np
import pandas as pd


def json_to_dataframe(
    json_data,
    doc,
    current_pat_client_id_code,
    full_doc=False,
    window=300,
    text_column="body_analysed",
    time_column="updatetime",
    guid_column="document_guid",
):
    # Extract data from the JSON
    # doc to be passed as pandas series
    # observation_guid
    if any(json_data.values()):
        done = False

        df_parts = []

        keys = list(json_data["entities"].keys())

        columns = [
            "client_idcode",
            time_column,
            "pretty_name",
            "cui",
            "type_ids",
            "types",
            "source_value",
            "detected_name",
            "acc",
            "context_similarity",
            "start",
            "end",
            "icd10",
            "ontologies",
            "snomed",
            "id",
            "Time_Value",
            "Time_Confidence",
            "Presence_Value",
            "Presence_Confidence",
            "Subject_Value",
            "Subject_Confidence",
            "text_sample",
            "full_doc",
            guid_column,
        ]

        empty_df = pd.DataFrame(data=None, columns=columns)

        for i in range(0, len(keys)):

            entities_data = json_data["entities"][keys[i]]
            pretty_name = entities_data["pretty_name"]
            cui = entities_data["cui"]
            type_ids = entities_data["type_ids"]
            types = entities_data["types"]
            source_value = entities_data["source_value"]
            detected_name = entities_data["detected_name"]
            acc = entities_data["acc"]
            context_similarity = entities_data["context_similarity"]
            start = entities_data["start"]
            end = entities_data["end"]
            icd10 = entities_data["icd10"]
            ontologies = entities_data["ontologies"]
            snomed = entities_data["snomed"]
            id = entities_data["id"]
            meta_anns = entities_data["meta_anns"]

            # Parse meta annotations
            parsed_meta_anns = parse_meta_anns(meta_anns)

            mapped_annot_doc_entity = doc[text_column]

            document_len = len(mapped_annot_doc_entity)

            document_len = len(mapped_annot_doc_entity)

            virtual_start = max(0, start - window)

            virtual_end = min(document_len, end + window)

            text_sample = mapped_annot_doc_entity[virtual_start:virtual_end]

            updatetime = doc[time_column]

            document_guid_value = doc[guid_column]

            full_doc_value = np.nan

            if full_doc and not done:
                full_doc_value = mapped_annot_doc_entity
                done = True
            else:
                full_doc_value = np.nan

            # Define DataFrame columns and create the DataFrame

            data = [
                [
                    current_pat_client_id_code,
                    updatetime,
                    pretty_name,
                    cui,
                    type_ids,
                    types,
                    source_value,
                    detected_name,
                    acc,
                    context_similarity,
                    start,
                    end,
                    icd10,
                    ontologies,
                    snomed,
                    id,
                    parsed_meta_anns["Time_Value"],
                    parsed_meta_anns["Time_Confidence"],
                    parsed_meta_anns["Presence_Value"],
                    parsed_meta_anns["Presence_Confidence"],
                    parsed_meta_anns["Subject_Value"],
                    parsed_meta_anns["Subject_Confidence"],
                    text_sample,
                    full_doc_value,
                    document_guid_value,
                ]
            ]

            df = pd.DataFrame(data, columns=columns)

            df_parts.append(df)

        try:

            super_df = pd.concat(df_parts)
            super_df.reset_index(inplace=True)
            return super_df

        except Exception as e:
            print(e)
            print("json_date", json_data)
            print(type(json_data))
            print(len(json_data))
            raise e

    else:
        columns = [
            "client_idcode",
            time_column,
            "pretty_name",
            "cui",
            "type_ids",
            "types",
            "source_value",
            "detected_name",
            "acc",
            "context_similarity",
            "start",
            "end",
            "icd10",
            "ontologies",
            "snomed",
            "id",
            "Time_Value",
            "Time_Confidence",
            "Presence_Value",
            "Presence_Confidence",
            "Subject_Value",
            "Subject_Confidence",
            "text_sample",
            "full_doc",
            guid_column,
        ]

        empty_df = pd.DataFrame(data=None, columns=columns)
        return empty_df


def parse_meta_anns(meta_anns):
    """
    Parses the meta annotations for each document in the batch
    Returns a dictionary of meta annotations for each document
    """
    time_value = meta_anns.get("Time", {}).get("value")
    time_confidence = meta_anns.get("Time", {}).get("confidence")

    presence_value = meta_anns.get("Presence", {}).get("value")
    presence_confidence = meta_anns.get("Presence", {}).get("confidence")

    subject_value = meta_anns.get("Subject/Experiencer", {}).get("value")
    subject_confidence = meta_anns.get("Subject/Experiencer", {}).get("confidence")

    # If 'Subject/Experiencer' is not found, fallback to 'Subject'
    if subject_value is None:
        subject_value = meta_anns.get("Subject", {}).get("value")
        subject_confidence = meta_anns.get("Subject", {}).get("confidence")

    return {
        "Time_Value": time_value,
        "Time_Confidence": time_confidence,
        "Presence_Value": presence_value,
        "Presence_Confidence": presence_confidence,
        "Subject_Value": subject_value,
        "Subject_Confidence": subject_confidence,
    }
