import os
import traceback
import logging
from typing import List, Optional, Union
import pandas as pd
from IPython.display import clear_output
from tqdm import tqdm


logger = logging.getLogger(__name__)

def compare_ipw_annotation_rows(
    dataframes: List[pd.DataFrame], columns_to_print: Optional[List[str]] = None
) -> None:
    """Compares and prints differing rows from multiple annotation DataFrames.

    This function identifies rows with the same 'client_idcode' across a list
    of DataFrames. If the 'text_sample' for that client differs between any
    of the DataFrames, it prints the specified columns for each version of
    the row, allowing for a side-by-side comparison.

    This is useful for evaluating the effect of filtering steps, for example,
    comparing an annotation DataFrame before and after applying a meta-annotation
    filter.

    Args:
        dataframes: A list of pandas DataFrames to compare. Each DataFrame
            should have a `name` attribute for clear output.
        columns_to_print: A list of column names to print when differences
            are found. If None, a default set of annotation-related columns
            is used.
    """
    if columns_to_print is None:
        # Default columns to print
        columns_to_print = [
            "updatetime",
            "pretty_name",
            "cui",
            "types",
            "source_value",
            "detected_name",
            "acc",
            "context_similarity",
            "Time_Value",
            "Time_Confidence",
            "Presence_Value",
            "Presence_Confidence",
            "Subject_Value",
            "Subject_Confidence",
        ]

    # Iterate over unique client_idcode values
    unique_client_ids = set()
    for df in dataframes:
        unique_client_ids = unique_client_ids.union(set(df["client_idcode"].unique()))

    for client_id in unique_client_ids:
        # Initialize a list to store rows for each dataframe
        rows = [df[df["client_idcode"] == client_id].iloc[0] for df in dataframes]

        # Check if the 'text_sample' column is not the same across all dataframes
        if not all(rows[0]["text_sample"] == row["text_sample"] for row in rows):
            clear_output(wait=True)  # Clear the output in Jupyter Notebook

            # Print 'text_sample' column from each dataframe
            for i, df in enumerate(dataframes):
                logger.info(f"{df.name}['text_sample']: {rows[i]['text_sample']}")

            # Print specified columns
            for column in columns_to_print:
                logger.info(f"{column}:")
                for i, df in enumerate(dataframes):
                    logger.info(f"{df.name}: {rows[i][column]}")
                logger.info("\n")

            # Wait for user input to proceed
            input("Press Enter to continue...")


class CsvProfiler:
    """A class to encapsulate functionality for profiling CSV files."""

    @staticmethod
    def create_profile_reports(
        epr_batchs_fp: str,
        prefix: Optional[str] = None,
        cols: Optional[List[str]] = None,
        icd10_opc4s: bool = False,
    ) -> None:
        """Generates profiling reports for CSV files in a directory.

        This method iterates through all CSV files in a specified directory,
        generates a ydata-profiling report for each, and saves it as an HTML
        file in a 'profile_reports' subdirectory.

        Args:
            epr_batchs_fp: Path to the directory containing the CSV files.
            prefix: An optional prefix to add to the generated report filenames.
            cols: A specific list of columns to include in the profile. If None,
                a default set of columns is used.
            icd10_opc4s: If True, filters the DataFrame to only include rows
                where the 'targetId' column is not empty before generating
                the report. Defaults to False.
        """
        from ydata_profiling import ProfileReport

        # Default columns to be used if none are provided
        default_cols = [
            "client_idcode",
            "pretty_name",
            "cui",
            "type_ids",
            "types",
            "acc",
            "context_similarity",
            "icd10",
            "ontologies",
            "snomed",
            "Time_Value",
            "Time_Confidence",
            "Presence_Value",
            "Presence_Confidence",
            "Subject_Value",
            "Subject_Confidence",
            "conceptId",
            "targetId",
            "updatetime",
        ]

        # Use the provided column list or the default list
        cols_to_use = cols or default_cols

        # Create the output directory for profile reports if it doesn't already exist
        profile_reports_dir = "profile_reports"
        os.makedirs(profile_reports_dir, exist_ok=True)

        for csv_file in tqdm(
            os.listdir(epr_batchs_fp), desc="Generating Profile Reports"
        ):
            file_path = os.path.join(epr_batchs_fp, csv_file)

            if not os.path.isfile(file_path):
                continue

            try:
                current_cols = list(cols_to_use)
                csv_columns = pd.read_csv(file_path, nrows=0).columns

                if "updatetime" not in csv_columns:
                    if "updatetime" in current_cols:
                        current_cols.remove("updatetime")
                    if (
                        "observationdocument_recordeddtm" not in current_cols
                        and "observationdocument_recordeddtm" in csv_columns
                    ):
                        current_cols.append("observationdocument_recordeddtm")

                if "targetId" not in csv_columns and "targetId" in current_cols:
                    current_cols.remove("targetId")

                final_cols = [col for col in current_cols if col in csv_columns]

                if not icd10_opc4s:
                    df = pd.read_csv(file_path, usecols=final_cols).sample(
                        n=100, random_state=1
                    )
                else:
                    df = pd.read_csv(file_path, usecols=final_cols)
                    if "targetId" in df.columns:
                        df.dropna(subset=["targetId"], inplace=True)

                # IMPORTANT: ProfileReport is now called as a class attribute
                profile = ProfileReport(
                    df,
                    title=f"Profiling Report for {csv_file}"
                    + (f" ({prefix})" if prefix else ""),
                    explorative=True,
                )

                report_prefix = f"{prefix}_" if prefix else ""
                report_name = (
                    f"{report_prefix}{os.path.splitext(csv_file)[0]}_profile.html"
                )
                report_path = os.path.join(profile_reports_dir, report_name)
                profile.to_file(report_path)

                logger.info(f"✅ Profile report for {csv_file} created at: {report_path}")

            except Exception as e:
                logger.error(f"❌ Error processing {csv_file}: {type(e).__name__} - {e}")
                traceback.print_exc()


if __name__ == "__main__":
    # This block demonstrates how to use the CsvProfiler class.
    logger.info("Setting up a dummy directory with CSV files for demonstration...")
    dummy_dir = "epr_batches_dummy"
    os.makedirs(dummy_dir, exist_ok=True)

    data1 = {
        "client_idcode": range(5),
        "pretty_name": ["Fever", "Headache", "Cough", "Sore Throat", "Fatigue"],
        "cui": [f"C00{i}" for i in range(5)],
        "targetId": [None, "ICD10:R51", None, "ICD10:R05", None],
        "updatetime": pd.to_datetime(
            ["2023-01-10", "2023-01-11", "2023-01-12", "2023-01-13", "2023-01-14"]
        ),
    }
    pd.DataFrame(data1).to_csv(os.path.join(dummy_dir, "batch_01.csv"), index=False)

    data2 = {
        "client_idcode": range(5, 10),
        "pretty_name": [
            "Nausea",
            "Vomiting",
            "Diarrhea",
            "Abdominal Pain",
            "Dizziness",
        ],
        "cui": [f"C00{i}" for i in range(5, 10)],
        "observationdocument_recordeddtm": pd.to_datetime(
            ["2024-02-10", "2024-02-11", "2024-02-12", "2024-02-13", "2024-02-14"]
        ),
    }
    pd.DataFrame(data2).to_csv(os.path.join(dummy_dir, "batch_02.csv"), index=False)

    logger.info("Dummy files created.")
    logger.info("-" * 30)

    logger.info("\nRunning example...")
    CsvProfiler.create_profile_reports(
        epr_batchs_fp=dummy_dir, prefix="class_import_profile"
    )

    logger.info("\nDemonstration complete. Check the 'profile_reports' directory for output.")
