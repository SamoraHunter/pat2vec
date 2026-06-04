import pandas as pd
import os
import pickle
import logging
from datetime import datetime
from typing import List
from tqdm import tqdm

logger = logging.getLogger(__name__)


def extract_datetime_to_column(df: pd.DataFrame, drop: bool = True) -> pd.DataFrame:
    """Extracts datetime information from binary columns and creates a new column."""
    date_cols = [col for col in df.columns if "_date_time_stamp" in col]
    if date_cols:
        col_names = df[date_cols].idxmax(axis=1)
        any_one = df[date_cols].max(axis=1) == 1
        df.loc[any_one, "extracted_datetime_stamp"] = col_names[any_one].str.replace(
            "_date_time_stamp", ""
        )
        df["extracted_datetime_stamp"] = pd.to_datetime(
            df["extracted_datetime_stamp"], format="(%Y, %m, %d)", errors="coerce"
        )

    if drop:
        columns_to_drop = [col for col in df.columns if "date_time_stamp" in col]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
    return df


def extract_datetime_from_binary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Extracts datetime values from binary columns representing dates."""
    date_columns = [
        col.strip("()").split(")")[0] for col in df.columns if "_date_time_stamp" in col
    ]
    date_columns_raw = [col for col in df.columns if "_date_time_stamp" in col]
    date_time_column_values = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        found = False
        for i in range(len(date_columns)):
            if row[date_columns_raw[i]] == 1:
                date_parts = [int(part) for part in date_columns[i].split(", ")]
                date_time_column_values.append(datetime(*date_parts))
                found = True
                break
        if not found:
            date_time_column_values.append(pd.NaT)
    df["datetime"] = date_time_column_values
    return df


def extract_datetime_from_binary_columns_chunk_reader(
    filepath: str, chunk_size: int = 1000
) -> pd.DataFrame:
    """Reads a CSV in chunks and extracts datetime from binary columns."""
    chunks = []
    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        chunks.append(extract_datetime_from_binary_columns(chunk))
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def drop_columns_with_all_nan(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Index]:
    """Drops columns where all values are NaN."""
    nan_columns = df.columns[df.isna().all()]
    df.drop(columns=nan_columns, inplace=True)
    return df, nan_columns


def save_missing_values_pickle(
    df: pd.DataFrame, out_file_path: str, overwrite: bool = False
) -> None:
    """Calculates missing percentage and saves as a pickle."""
    missing_dict = (df.isnull().sum() / len(df) * 100).to_dict()
    pickle_path = os.path.splitext(out_file_path)[0] + "_missing_dict.pickle"
    if not os.path.exists(pickle_path) or overwrite:
        with open(pickle_path, "wb") as f:
            pickle.dump(missing_dict, f)


def convert_true_to_float(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """Converts 'True' strings to 1.0 and ensures columns are float."""
    if columns is None:
        columns = [
            "census_black_african_caribbean_or_black_british",
            "census_mixed_or_multiple_ethnic_groups",
            "census_white",
            "census_asian_or_asian_british",
            "census_other_ethnic_group",
        ]
    df[columns] = df[columns].replace({"True": 1.0, "False": 0.0}).astype(float)
    return df


def impute_datetime(
    df: pd.DataFrame,
    datetime_column: str = "datetime",
    patient_column: str = "client_idcode",
    forward: bool = True,
    backward: bool = True,
    mean_impute: bool = True,
    verbose: bool = False,
) -> pd.DataFrame:
    """Imputes missing datetime values based on temporal order."""
    df[datetime_column] = pd.to_datetime(df[datetime_column])
    df = df.sort_values(by=[patient_column, datetime_column])
    cols_to_fill = df.columns.difference([patient_column])
    if forward:
        df[cols_to_fill] = df.groupby(patient_column)[cols_to_fill].ffill()
    if backward:
        df[cols_to_fill] = df.groupby(patient_column)[cols_to_fill].bfill()
    if mean_impute:
        df[datetime_column] = df[datetime_column].fillna(df[datetime_column].mean())
    return df


def impute_dataframe(
    df: pd.DataFrame,
    verbose: bool = True,
    datetime_column: str = "datetime",
    patient_column: str = "client_idcode",
    forward: bool = True,
    backward: bool = True,
    mean_impute: bool = True,
) -> pd.DataFrame:
    """Imputes missing numeric values in a DataFrame based on patient ID and temporal order."""
    numeric_columns = df.select_dtypes(include="number").columns.tolist()
    df = df.sort_values(by=[patient_column, datetime_column])
    for col in tqdm(numeric_columns, disable=not verbose):
        if forward:
            df[col] = df.groupby(patient_column)[col].ffill()
        if backward:
            df[col] = df.groupby(patient_column)[col].bfill()
        if mean_impute:
            df[col] = df[col].fillna(df[col].mean())
    return df


def missing_percentage_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Calculates missing percentage per column."""
    missing_percentage = dataframe.isnull().mean() * 100
    return pd.DataFrame(
        {
            "Column": missing_percentage.index,
            "MissingPercentage": missing_percentage.values,
        }
    )


def aggregate_dataframe_mean(
    df: pd.DataFrame, group_by_column: str = "client_idcode"
) -> pd.DataFrame:
    """Aggregates a DataFrame by taking the mean of numeric columns."""
    numeric_cols = df.select_dtypes(include="number").columns
    non_numeric_cols = df.select_dtypes(exclude="number").columns.difference(
        [group_by_column]
    )
    agg_dict = {col: "mean" for col in numeric_cols}
    agg_dict.update({col: "first" for col in non_numeric_cols})
    return df.groupby(group_by_column).agg(agg_dict).reset_index()


def collapse_df_to_mean(
    df: pd.DataFrame,
    output_filename: str = "output.csv",
    client_idcode_string: str = "client_idcode",
) -> None:
    """Collapses a DataFrame to means and saves/merges with an output file."""
    aggregated_df = aggregate_dataframe_mean(df, group_by_column=client_idcode_string)
    if os.path.exists(output_filename):
        output_df = pd.read_csv(output_filename)
        aggregated_df = pd.concat([output_df, aggregated_df]).drop_duplicates(
            subset=[client_idcode_string], keep="last"
        )
    aggregated_df.to_csv(output_filename, index=False)
