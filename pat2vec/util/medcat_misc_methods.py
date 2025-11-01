import json
import pandas as pd
import logging
import numpy as np
from tqdm import tqdm
from typing import Optional
import textwrap
from IPython.display import clear_output
from typing import Any, Dict, List, Union
from ast import literal_eval
import matplotlib.pyplot as plt
import seaborn as sns
import os

logger = logging.getLogger(__name__)


def medcat_trainer_export_to_df(file_path: str) -> pd.DataFrame:
    """
    Converts a MedCATTrainer export JSON file to a pandas DataFrame.

    Args:
        file_path: Path to the JSON file containing MedCATTrainer export data.

    Returns:
        A DataFrame containing the extracted data, with each row representing
        a single annotation.
    """
    # Load the JSON data
    with open(file_path, "r") as f:
        data = json.load(f)

    # Initialize lists to store extracted data
    annotations_data = []

    # Iterate through projects, documents, and annotations
    for project in data["projects"]:
        project_name = project["name"]
        project_id = project["id"]

        for document in project["documents"]:
            document_id = document["id"]
            document_name = document["name"]
            text = document["text"]

            for annotation in document["annotations"]:
                annotation_id = annotation["id"]
                user = annotation["user"]
                cui = annotation["cui"]
                value = annotation["value"]
                start = annotation["start"]
                end = annotation["end"]
                validated = annotation["validated"]
                correct = annotation["correct"]
                deleted = annotation["deleted"]
                alternative = annotation["alternative"]
                killed = annotation["killed"]
                irrelevant = annotation["irrelevant"]
                create_time = annotation["create_time"]
                last_modified = annotation["last_modified"]
                comment = annotation["comment"]
                manually_created = annotation["manually_created"]

                # Extract meta annotations
                meta_anns = annotation.get("meta_anns", {})
                subject_experiencer = meta_anns.get("Subject/Experiencer", {}).get(
                    "value"
                )
                presence = meta_anns.get("Presence", {}).get("value")
                time = meta_anns.get("Time", {}).get("value")

                # Append extracted data to the list
                annotations_data.append(
                    {
                        "project_name": project_name,
                        "project_id": project_id,
                        "document_id": document_id,
                        "document_name": document_name,
                        "text": text,
                        "annotation_id": annotation_id,
                        "user": user,
                        "cui": cui,
                        "value": value,
                        "start": start,
                        "end": end,
                        "validated": validated,
                        "correct": correct,
                        "deleted": deleted,
                        "alternative": alternative,
                        "killed": killed,
                        "irrelevant": irrelevant,
                        "create_time": create_time,
                        "last_modified": last_modified,
                        "comment": comment,
                        "manually_created": manually_created,
                        "subject_experiencer": subject_experiencer,
                        "presence": presence,
                        "time": time,
                    }
                )

    # Create a DataFrame from the extracted data
    df = pd.DataFrame(annotations_data)
    return df


# Example usage:
# # df = medcat_trainer_export_to_df('merge4.json')
# # logger.info(df)
#


def extract_labels_from_medcat_annotation_export(
    df: pd.DataFrame,
    human_labels: pd.DataFrame,
    window: int = 300,
    output_file: Union[str, None] = None,
) -> pd.DataFrame:
    """
    Extracts and validates labels from a MedCAT annotation export.

    This function compares annotations from a MedCAT trainer export (`df`)
    with a set of human-labeled data (`human_labels`). It matches them based
    on the text content and source value, then validates the annotation based
    on its meta-annotations (Subject, Presence, Time). The result is stored
    in an 'extracted_label' column in the `human_labels` DataFrame.

    Args:
        df: The trainer output in DataFrame form (from `medcat_trainer_export_to_df`).
        human_labels: The DataFrame containing human-labeled text samples.
        window: The window size for extracting text samples for comparison.
        output_file: An optional file path to save the processed DataFrame.

    Returns:
        The processed `human_labels` DataFrame with the 'extracted_label' column.
    """

    human_labels["extracted_label"] = np.nan

    for j in tqdm(range(0, len(df))):
        main_text = df.iloc[j]["text"]
        main_value = df.iloc[j]["value"]
        mapped_annot_doc_entity = main_text
        start = df.iloc[j]["start"]
        end = df.iloc[j]["end"]
        document_len = len(mapped_annot_doc_entity)
        virtual_start = max(0, start - window)
        virtual_end = min(document_len, end + window)
        main_text_sample = mapped_annot_doc_entity[virtual_start:virtual_end]

        for i in range(0, len(human_labels)):
            label_text = human_labels.iloc[i]["text_sample"]
            label_value = human_labels.iloc[i]["source_value"]

            if label_text in main_text:
                if main_text_sample == label_text:
                    if main_value == label_value:
                        extracted_bool = (
                            (df.iloc[j]["subject_experiencer"] == "Patient")
                            & (df.iloc[j]["presence"] == "True")
                            & (
                                (df.iloc[j]["time"] == "Recent")
                                | (df.iloc[j]["time"] == "Past")
                            )
                        )
                        human_labels.at[i, "extracted_label"] = int(extracted_bool)

    if output_file is not None:
        human_labels.to_csv(output_file, index=False)

    return human_labels


def recreate_json(df: pd.DataFrame, output_file: Optional[str] = None) -> str:
    """
    Converts an exported MedCAT trainer DataFrame back to a training JSON.

    This function takes a DataFrame (as produced by `medcat_trainer_export_to_df`)
    and reconstructs the original JSON structure required for training a
    MedCAT model.

    Args:
        df: DataFrame containing exported data from a MedCAT trainer project.
        output_file: Optional file path to save the generated JSON.

    Returns:
        A JSON string representing the MedCAT training data.
    """
    projects = []

    # Group by project and document
    grouped = df.groupby(
        ["project_name", "project_id", "document_id", "document_name", "text"]
    )

    for (
        project_name,
        project_id,
        document_id,
        document_name,
        text,
    ), group_data in grouped:
        documents = []

        for _, annotation_data in group_data.iterrows():
            annotation = {
                "id": annotation_data["annotation_id"],
                "user": annotation_data["user"],
                "cui": annotation_data["cui"],
                "value": annotation_data["value"],
                "start": annotation_data["start"],
                "end": annotation_data["end"],
                "validated": annotation_data["validated"],
                "correct": annotation_data["correct"],
                "deleted": annotation_data["deleted"],
                "alternative": annotation_data["alternative"],
                "killed": annotation_data["killed"],
                "irrelevant": annotation_data["irrelevant"],
                "create_time": annotation_data["create_time"],
                "last_modified": annotation_data["last_modified"],
                "comment": annotation_data["comment"],
                "manually_created": annotation_data["manually_created"],
                "meta_anns": {
                    "Subject/Experiencer": {
                        "value": annotation_data["subject_experiencer"]
                    },
                    "Presence": {"value": annotation_data["presence"]},
                    "Time": {"value": annotation_data["time"]},
                },
            }
            documents.append(annotation)

        project = {
            "name": project_name,
            "id": int(project_id),
            "documents": [
                {
                    "id": int(document_id),
                    "name": document_name,
                    "text": text,
                    "annotations": documents,
                    "relations": [],
                }
            ],
        }
        projects.append(project)

    reconstructed_json = {"projects": projects}
    json_str = json.dumps(reconstructed_json, indent=4)

    # Optionally write to file if output_file is provided
    if output_file:
        with open(output_file, "w") as f:
            f.write(json_str)

    return json_str


# Example usage:
# # # Call the function with your DataFrame and output filename
# # output_filename = 'recreated_annotations.json'
# # recreated_json = recreate_json(df, output_file=output_filename)
#
# # # Print the recreated JSON
# # logger.info(recreated_json)


def manually_label_annotation_df(
    df: pd.DataFrame,
    file_path: str = "human_labels.csv",
    confirmatory: bool = False,
    verbose: bool = False,
    filter_codes_list: List[List[str]] = [],
) -> None:
    """
    Interactively labels an annotation DataFrame.

    This function loops over an annotation DataFrame, displays annotations for
    unique client ID codes, and prompts the user for a label (1 for correct,
    0 for incorrect). The process continues until all annotations for a
    client (matching `filter_codes_list`) are confirmed. The labels are
    saved to a CSV file.

    Args:
        df: The DataFrame to annotate.
        file_path: The file path to store the human labels.
        confirmatory: If True, skips clients who already have a confirmed
            correct annotation for the given filter codes.
        verbose: If True, prints verbose output.
        filter_codes_list: A list of CUI code lists. A client is considered
            "done" when they have a correct annotation for each list of codes.
    """
    counter = 0
    if os.path.exists(file_path):
        if verbose:
            logger.info("Reading human labels from file...")
        human_labels = pd.read_csv(file_path)
    else:
        if verbose:
            logger.info("Creating new human labels DataFrame...")
        human_labels = pd.DataFrame(columns=["human_label"])

    if "human_label" not in df.columns:
        df["human_label"] = human_labels["human_label"]

    for index, row in tqdm(df.iterrows()):
        if confirmatory:
            if pd.notnull(row["client_idcode"]):  # type: ignore
                existing_labels = [
                    df[
                        (df["client_idcode"] == row["client_idcode"])  # type: ignore
                        & (df["human_label"] == 1)
                        & (df["cui"].isin(filter_codes))
                    ]
                    for filter_codes in filter_codes_list
                ]

                if not all(label.empty for label in existing_labels):
                    continue  # Skip rows if there is at least one label from each of the supplied lists

        if pd.isnull(row["human_label"]):  # type: ignore
            remaining_labels_info = [
                (
                    len(
                        df[
                            (df["client_idcode"] == row["client_idcode"])
                            & (df["human_label"] != 1)
                            & (df["cui"].isin(filter_codes))
                        ]
                    ),
                    len(
                        df[
                            (df["client_idcode"] == row["client_idcode"])
                            & (df["cui"].isin(filter_codes))
                        ]
                    ),
                )
                for filter_codes in filter_codes_list
            ]

            if verbose:
                logger.info("Printing text sample for user input...")
            text_sample = row["text_sample"]
            source_value = row["source_value"]
            highlighted_text = text_sample.replace(
                source_value, f"\033[1m{source_value}\033[0m"
            )  # Bold highlight
            highlighted_text = text_sample.replace(
                source_value, f"\033[4;1m{source_value}\033[0m"
            )  # Underline and bold highlight

            # Wrap the text to fit within standard scroll window width
            wrapped_text = textwrap.fill(highlighted_text, width=80)
            logger.info(wrapped_text)

            clear_output(wait=True)  # Clear Jupyter notebook display
            label = input(
                f"Labelling {row['client_idcode']} Press enter for 1 or enter 0 for 0.: "
            )
            if label == "":
                label = 1
            elif label == "quit" or label == "end":
                raise ValueError("User ended the labeling process.")
            elif label != "":
                label = 0

            df.at[index, "human_label"] = label

            if counter % 10 == 0:
                df.to_csv(file_path, index=False)  # Write to file immediately
                logger.info("Saved human labels progress to file.")
            counter += 1

            logger.info(
                f"Remaining unlabeled rows: {df[df['human_label'].isna()].shape[0]}, Labeled rows: {df[df['human_label'].notna()].shape[0]}"
            )
            logger.info(
                f"Remaining unlabeled clients: {df[df['human_label'].isna()]['client_idcode'].nunique()}, Labeled clients: {df[df['human_label'].notna()]['client_idcode'].nunique()}"
            )
            for i, filter_codes in enumerate(filter_codes_list):
                logger.info(
                    f"Remaining labels for filter {i + 1} as a total of codes: {remaining_labels_info[i][0]}/{remaining_labels_info[i][1]}"
                )

    if verbose:
        logger.info("Writing final human labels to file...")

    df.to_csv(file_path, index=False)
    logger.info("Saved human labels to file.")
    counter += 1


def parse_medcat_trainer_project_json(json_path: str) -> pd.DataFrame:
    """Parses a MedCAT trainer project JSON into a structured DataFrame.

    This function reads a JSON file exported from a MedCAT trainer project.
    It handles various formats (nested JSON, list of JSON strings) and
    normalizes the data into a flat DataFrame where each row represents a
    single annotation with its associated document and project metadata.

    Args:
        json_path: Path to the JSON file from a MedCAT trainer export.

    Returns:
        A DataFrame containing parsed and structured data, including project
        and document details, annotations, and their meta-annotations.

    Notes:
        - Handles nested JSON structures and safely converts JSON strings.
        - Explodes 'cuis' and 'documents' columns to create detailed rows.
        - Extracts meta-annotation details into separate columns.
    """

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Fix 1: Ensure data is parsed if it's a string or a list of JSON strings
    if isinstance(data, str):
        data = json.loads(data)
    elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], str):
        data = [json.loads(item) for item in data]

    # Fix 2: Handle nested structure
    if isinstance(data, dict) and "projects" in data:
        data = data["projects"]

    # Step 1: Create initial DataFrame
    df_projects = pd.DataFrame(data)

    # Step 2: Explode 'cuis' if it exists
    if "cuis" in df_projects.columns:
        df_exploded = df_projects.explode("cuis").rename(columns={"cuis": "cui"})
    else:
        df_exploded = df_projects.copy()

    # Step 3: Safely convert 'documents' column
    def safe_convert(x):
        if isinstance(x, str):
            try:
                return json.loads(x)
            except json.JSONDecodeError:
                try:
                    return literal_eval(x)
                except:
                    return []
        elif isinstance(x, list):
            return x
        return []

    df_exploded["documents"] = df_exploded["documents"].apply(safe_convert)

    # Step 4: Explode documents
    doc_df = df_exploded.explode("documents").reset_index(drop=True)

    # Step 5: Extract document-level data
    def extract_document_data(row):
        doc = row["documents"]
        if not isinstance(doc, dict):
            return None

        result = {
            "project_id": row["id"],
            "project_name": row["name"],
            "cui": row.get("cui"),
            "doc_id": doc.get("id"),
            "doc_name": doc.get("name"),
            "doc_text": doc.get("text"),
            "doc_last_modified": doc.get("last_modified"),
        }

        if "annotations" in doc:
            result["annotations"] = doc["annotations"]

        return result

    doc_data = pd.DataFrame(
        doc_df.apply(extract_document_data, axis=1).tolist()
    ).dropna()

    # Step 6: Explode annotations
    final_data = []
    for _, row in doc_data.iterrows():
        base_data = (
            row.drop("annotations").to_dict() if "annotations" in row else row.to_dict()
        )

        if "annotations" in row and isinstance(row["annotations"], list):
            for ann in row["annotations"]:
                if isinstance(ann, dict):
                    ann_record = base_data.copy()
                    ann_record.update(
                        {
                            "ann_id": ann.get("id"),
                            "ann_cui": ann.get("cui"),
                            "ann_value": ann.get("value"),
                            "ann_start": ann.get("start"),
                            "ann_end": ann.get("end"),
                            "ann_validated": ann.get("validated"),
                        }
                    )

                    if "meta_anns" in ann:
                        for meta_name, meta_data in ann["meta_anns"].items():
                            clean_name = meta_name.replace("/", "_").replace(" ", "_")
                            ann_record.update(
                                {
                                    f"meta_{clean_name}_value": meta_data.get("value"),
                                    f"meta_{clean_name}_acc": meta_data.get("acc"),
                                    f"meta_{clean_name}_validated": meta_data.get(
                                        "validated"
                                    ),
                                }
                            )

                    final_data.append(ann_record)
        else:
            final_data.append(base_data)

    # Final DataFrame
    df_final = pd.DataFrame(final_data)

    logger.info(f"Final DataFrame shape: {df_final.shape}")
    logger.info("Columns available:")
    logger.info(df_final.columns.tolist())


def create_ner_results_dataframe(
    fps, fns, tps, cui_prec, cui_rec, cui_f1, cui_counts, cat=None
):
    """
    Creates a Pandas DataFrame from NER evaluation dictionaries.

    Args:
        fps (dict): Dictionary of false positives with CUI as keys.
        fns (dict): Dictionary of false negatives with CUI as keys.
        tps (dict): Dictionary of true positives with CUI as keys.
        cui_prec (dict): Dictionary of CUI-based precision with CUI as keys.
        cui_rec (dict): Dictionary of CUI-based recall with CUI as keys.
        cui_f1 (dict): Dictionary of CUI-based F1-score with CUI as keys.
        cui_counts (dict): Dictionary of CUI counts with CUI as keys.
        if cat object passed, will add preferred name

    Returns:
        pandas.DataFrame: DataFrame with CUI as index and columns for
                          fps, fns, tps, cui_prec, cui_rec, cui_f1, cui_counts and optionally a cat medcat object.
    """
    all_cuis = (
        set(fps.keys())
        | set(fns.keys())
        | set(tps.keys())
        | set(cui_prec.keys())
        | set(cui_rec.keys())
        | set(cui_f1.keys())
        | set(cui_counts.keys())
    )

    df = pd.DataFrame(index=list(all_cuis))
    df["fps"] = pd.Series(fps)
    df["fns"] = pd.Series(fns)
    df["tps"] = pd.Series(tps)
    df["cui_prec"] = pd.Series(cui_prec)
    df["cui_rec"] = pd.Series(cui_rec)
    df["cui_f1"] = pd.Series(cui_f1)
    df["cui_counts"] = pd.Series(cui_counts)

    if cat:
        df["cui_name"] = (
            pd.Series(df.index)
            .apply(lambda cui: cat.cdb.cui2preferred_name.get(cui))
            .values
        )

    return df


def plot_ner_results(results_df: pd.DataFrame) -> None:
    """
    Generates plots to visualize NER (Named Entity Recognition) evaluation results.

    This function creates a series of plots to help analyze the performance
    of an NER model, including F1-scores, precision-recall, error analysis,
    and the relationship between concept frequency and performance.

    Args:
        results_df: A DataFrame containing NER evaluation metrics, which must
            include 'cui_name', 'cui_f1', 'cui_prec', 'cui_rec', 'fps', 'fns',
            'tps', and 'cui_counts'.
    """
    if "cui_name" not in results_df.columns:
        logger.error("Error: 'cui_name' column is required in the DataFrame.")
        return

    results_df = results_df.sort_values(by="cui_f1")

    # 1. Bar Plot: F1-Score by CUI Name
    plt.figure(figsize=(12, 6))
    sns.barplot(x="cui_name", y="cui_f1", data=results_df)
    plt.title("F1-Score by CUI Name")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("F1-Score")
    plt.xlabel("CUI Name")
    plt.tight_layout()
    plt.show()

    # 2. Scatter Plot: Precision vs. Recall
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x="cui_prec", y="cui_rec", data=results_df, hue="cui_name")
    plt.title("Precision vs. Recall")
    plt.xlabel("Precision")
    plt.ylabel("Recall")
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.show()

    # 3. Bar Plot: Error Analysis (fps, fns, tps)
    results_df_melted = results_df[["cui_name", "fps", "fns", "tps"]].melt(
        id_vars="cui_name", var_name="error_type", value_name="count"
    )
    plt.figure(figsize=(10, 6))
    sns.barplot(x="cui_name", y="count", hue="error_type", data=results_df_melted)
    plt.title("Error Analysis (TP, FP, FN) by CUI Name")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Count")
    plt.xlabel("CUI Name")
    plt.tight_layout()
    plt.show()

    # 4. Scatter Plot: CUI Counts vs. F1-Score
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x="cui_counts", y="cui_f1", data=results_df, hue="cui_name")
    plt.title("CUI Counts vs. F1-Score")
    plt.xlabel("CUI Counts")
    plt.ylabel("F1-Score")
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.show()

    # --- Additional Plots ---

    # 5. Pair Plot of Performance Metrics
    performance_metrics = results_df[["cui_prec", "cui_rec", "cui_f1"]]
    plt.figure(figsize=(8, 8))
    sns.pairplot(performance_metrics)
    plt.suptitle("Pair Plot of Precision, Recall, and F1-Score", y=1.02)
    plt.tight_layout()
    plt.show()

    # 6. Distribution of F1-Scores
    plt.figure(figsize=(8, 6))
    sns.histplot(results_df["cui_f1"], kde=True)
    plt.title("Distribution of F1-Scores")
    plt.xlabel("F1-Score")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.show()

    # 7. Scatter Plot: F1-Score vs. CUI Counts (Size indicates another metric)
    plt.figure(figsize=(10, 7))
    sns.scatterplot(
        x="cui_counts",
        y="cui_f1",
        data=results_df,
        hue="cui_name",
        size="tps",
        alpha=0.7,
    )
    plt.title("F1-Score vs. CUI Counts (Size by True Positives)")
    plt.xlabel("CUI Counts")
    plt.ylabel("F1-Score")
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.show()
