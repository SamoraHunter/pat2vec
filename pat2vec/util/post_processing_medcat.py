import pandas as pd
from rapidfuzz import fuzz
import random


def sample_by_terms(
    df, column, term_groups, min_samples_per_term, total_sample_size, threshold=75
):
    """
    Randomly samples rows from a DataFrame based on fuzzy matching of terms.
    Useful for sampling documents for a medCat project with uneven distribution across terms.

    Parameters:
    df (pd.DataFrame): The DataFrame to sample from.
    column (str): The column to search for matches.
    term_groups (list of list of str): Groups of terms where each group is treated as a single entity.
    min_samples_per_term (int): Minimum number of samples to take per term group.
    total_sample_size (int): Desired total number of samples.
    threshold (int): Fuzzy matching threshold (0-100).

    Returns:
    pd.DataFrame: A DataFrame containing the sampled rows with an added column for matched terms.
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
                    f"Warning: Could not meet minimum samples ({min_samples_per_term}) for term group {group}. Found {len(indices)} matches."
                )

    # Calculate remaining quota for proportional sampling
    remaining_quota = total_sample_size - len(sampled_indices)
    if remaining_quota < 0:
        warnings.append(
            f"Warning: Total sample size ({total_sample_size}) is less than required minimum samples ({len(sampled_indices)}). Adjusting to {len(sampled_indices)}."
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
        print(warning)

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
