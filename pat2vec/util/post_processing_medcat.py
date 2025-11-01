import pandas as pd
from rapidfuzz import fuzz
import random
from typing import List, Dict, Any

import logging

logger = logging.getLogger(__name__)


def sample_by_terms(
    df: pd.DataFrame,
    column: str,
    term_groups: List[List[str]],
    min_samples_per_term: int,
    total_sample_size: int,
    threshold: int = 75,
) -> pd.DataFrame:
    """Randomly samples rows from a DataFrame based on fuzzy matching of terms.

    This function is useful for creating a balanced sample set for tasks like
    MedCAT model training, where the source data may have an uneven
    distribution of concepts. It performs stratified sampling based on term
    groups.

    The sampling process is as follows:

    1.  It ensures a `min_samples_per_term` for each group of terms.
    2.  It then proportionally samples from the remaining available documents
        to reach the `total_sample_size`.

    Args:
        df: The DataFrame to sample from.
        column: The column in `df` to search for term matches.
        term_groups: A list of term groups, where each inner list contains
            synonymous or related terms.
        min_samples_per_term: The minimum number of samples to retrieve for
            each term group.
        total_sample_size: The desired total number of samples in the final
            DataFrame.
        threshold: The fuzzy matching score (0-100) required to consider a
            term as a match.

    Returns:
        A new DataFrame containing the sampled rows. An additional
        'matched_term' column is added for debugging, showing which specific
        term from a group matched the row.
    """
    # Flatten term groups for creating match keys and initialize result
    term_matches = {tuple(group): [] for group in term_groups}
    matched_terms = {}

    # Match rows to term groups
    for idx, text in df[column].dropna().items():
        for group in term_groups:
            for term in group:
                if fuzz.partial_ratio(term.lower(), text.lower()) >= threshold:
                    term_matches[tuple(group)].append(idx)
                    matched_terms[idx] = term
                    break

    # Ensure minimum samples per term group
    sampled_indices = set()
    warnings = []
    for group, indices in term_matches.items():
        if len(indices) >= min_samples_per_term:
            sampled_indices.update(random.sample(indices, min_samples_per_term))
        else:
            sampled_indices.update(indices)
            if len(indices) < min_samples_per_term:
                warnings.append(
                    f"Could not meet minimum samples ({min_samples_per_term}) for term group {group}. Found {len(indices)} matches."
                )

    # Calculate remaining quota for proportional sampling
    remaining_quota = total_sample_size - len(sampled_indices)
    if remaining_quota < 0:
        warnings.append(
            f"Total sample size ({total_sample_size}) is less than required minimum samples ({len(sampled_indices)}). Adjusting to {len(sampled_indices)}."
        )
        remaining_quota = 0

    # Proportional sampling from remaining rows
    total_hits = sum(len(indices) for indices in term_matches.values())
    remaining_samples = []
    if total_hits > 0 and remaining_quota > 0:
        for group, indices in term_matches.items():
            unselected_indices = list(set(indices) - sampled_indices)
            proportion = len(indices) / total_hits
            num_to_sample = int(remaining_quota * proportion)
            remaining_samples.extend(
                random.sample(
                    unselected_indices, min(num_to_sample, len(unselected_indices))
                )
            )

    # Combine sampled indices
    final_sampled_indices = list(sampled_indices) + remaining_samples[:remaining_quota]
    final_sampled_df = df.loc[final_sampled_indices].copy()

    # Add matched terms column for debugging
    final_sampled_df["matched_term"] = final_sampled_df.index.map(matched_terms)

    # Print warnings
    for warning in warnings:
        logger.warning(warning)

    return final_sampled_df


# Example usage
# if __name__ == "__main__":


#     # Define search terms grouped by similarity
#     term_groups = [
#     ["Hepatic Artery"],
#     ["Hepatic artery thrombosis"],
#     ["Stenosis of hepatic artery"],
#     ["Embolism and thrombosis of hepatic artery"],
#     ["embolism"],
#     ["hepatic artery"],
#     ["Left hepatic veins",
#     "Right hepatic veins",
#     "Middle hepatic veins",
#     "Structure of hepatic vein"],
#     ["Patent",
#     "Patent vessels"],
#     ["Common bile duct"],
#     ["Ischaemia"],
#     ["Collections"],
#     ["Structure of parenchyma of liver",
#     "Homogenous pattern"],
#     ['Bilary']
# ]


#     # Sample with the function
#     result = sample_by_terms(
#         df=dfss, #dfss is a dataframe containing text in body_analysed
#         column="body_analysed",
#         term_groups=term_groups,
#         min_samples_per_term=10,
#         total_sample_size=100,
#         threshold=75
#     )

#     # Display sampled rows
#     display(result)


def coerce_document_df_to_medcat_trainer_input(
    df: pd.DataFrame,
    text_column_value: str = "body_analysed",
    name_value: str = "_id",
) -> pd.DataFrame:
    """Prepares a DataFrame for MedCAT trainer input format.

    This function transforms a given DataFrame into the specific two-column
    format required by the MedCAT trainer: a 'name' column for unique document
    identifiers and a 'text' column for the document content.

    It performs the following steps:

    1.  Renames the specified `name_value` and `text_column_value` columns to
        'name' and 'text', respectively.
    2.  Ensures that all values in the 'name' column are unique. If duplicates
        are found, they are made unique by appending a suffix (e.g., `_1`, `_2`).
    3.  Returns a new DataFrame containing only the 'name' and 'text' columns.

    Args:
        df: The input DataFrame.
        text_column_value: The name of the column containing the document text.
        name_value: The name of the column to be used as the document identifier.

    Returns:
        A new DataFrame with 'name' and 'text' columns, ready for MedCAT
        trainer.

    Raises:
        KeyError: If `name_value` or `text_column_value` are not found in the
            DataFrame's columns.
    """
    # Clean column names to avoid issues with whitespace
    df.columns = df.columns.str.strip()

    # Check for the existence of required columns
    if name_value not in df.columns or text_column_value not in df.columns:
        raise KeyError(
            f"Expected columns '{name_value}' or '{text_column_value}' are missing from the DataFrame"
        )

    logger.debug(f"Columns before renaming: {df.columns.tolist()}")

    # Rename columns
    rename_mapping = {name_value: "name", text_column_value: "text"}
    df.rename(columns=rename_mapping, inplace=True)

    # Check if renaming succeeded
    if "name" not in df.columns or "text" not in df.columns:
        raise KeyError(
            "Renaming failed: 'name' or 'text' column is missing after rename"
        )

    # Ensure unique values in the 'name' column
    def make_unique(series):
        seen = {}
        result = []
        for value in series:
            if value not in seen:
                seen[value] = 0
            else:
                # print warning about duplicate values in the 'name' column
                logger.warning(
                    f"Duplicate value '{value}' found in 'name' column. Renaming to '{value}_{seen[value]}'"
                )

                seen[value] += 1
            unique_value = f"{value}_{seen[value]}" if seen[value] > 0 else value

            result.append(unique_value)
        return result

    df["name"] = make_unique(df["name"])

    # Select the renamed columns and return a copy
    df = df[["name", "text"]].copy()

    logger.debug(f"Columns after processing: {df.columns.tolist()}")

    return df


# coerce_document_df_to_medcat_trainer_input(df=result, text_column_value='body_analysed', name_value='_id').to_csv('ultrasounds_sample_strat.csv', index=False)
