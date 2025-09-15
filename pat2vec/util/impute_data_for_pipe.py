from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import random
import pickle
from typing import Dict, List, Union


def mean_impute_dataframe(
    data: pd.DataFrame,
    y_vars: Union[str, List[str]],
    test_size: float = 0.25,
    val_size: float = 0.25,
    random_state: int = 1,
    seed: int = 1234,
) -> pd.DataFrame:
    """Splits data, imputes missing numeric values with the mean, and recombines.

    This function performs a standard machine learning preprocessing step. It first
    splits the input DataFrame into training, validation, and test sets. It then
    calculates the mean for each numeric feature based on the training set and
    uses this mean to impute missing values in all three sets. Finally, it
    recombines the imputed sets into a single DataFrame.

    Args:
        data: The input DataFrame containing features and target variables.
        y_vars: The name of the target variable column(s).
        test_size: The proportion of the dataset to allocate to the test split.
        val_size: The proportion of the training dataset to allocate to the
            validation split.
        random_state: Seed for the train-test split for reproducibility.
        seed: Seed for Python's `random` module.

    Returns:
        The full DataFrame with missing numeric values imputed.
    """
    random.seed(seed)

    # Drop columns that are completely empty (no values at all)
    data = data.dropna(axis=1, how='all')
    print(f"After dropping completely empty columns, data shape: {data.shape}")

    # Ensure y_vars is a list
    y_vars = [y_vars] if isinstance(y_vars, str) else y_vars

    # Separate features and target variable
    X = data.drop(columns=y_vars, axis=1)
    y = data[y_vars]

    # Split into train, test, and validation sets
    X_train_orig, X_test_orig, y_train_orig, y_test_orig = train_test_split(X, y, test_size=test_size, random_state=random_state)
    X_train, X_val, y_train, y_val = train_test_split(X_train_orig, y_train_orig, test_size=val_size, random_state=random_state)

    # Print shapes after split
    print(f"Train shape: {X_train.shape}, Validation shape: {X_val.shape}, Test shape: {X_test_orig.shape}")

    # Identify numeric and non-numeric columns
    numeric_cols = X.select_dtypes(include=[np.number]).columns
    non_numeric_cols = X.select_dtypes(exclude=[np.number]).columns
    print(f"Numeric columns: {len(numeric_cols)}, Non-numeric columns: {len(non_numeric_cols)}")

    # Initialize imputed DataFrames
    X_train_imputed = X_train.copy()
    X_val_imputed = X_val.copy()
    X_test_imputed = X_test_orig.copy()

    # Impute missing values with the mean for numeric columns
    if len(numeric_cols) > 0:
        imputer = SimpleImputer(strategy='mean')

        # Process each numeric column separately
        for col in numeric_cols:
            # Check if column is completely empty
            if X_train[col].isnull().all():
                X_train_imputed[col] = 0
                X_val_imputed[col] = 0
                X_test_imputed[col] = 0
            else:
                # Reshape for single column imputation
                col_train = X_train[col].values.reshape(-1, 1)
                col_val = X_val[col].values.reshape(-1, 1)
                col_test = X_test_orig[col].values.reshape(-1, 1)

                # Fit and transform
                imputer.fit(col_train)
                X_train_imputed[col] = imputer.transform(col_train).ravel()
                X_val_imputed[col] = imputer.transform(col_val).ravel()
                X_test_imputed[col] = imputer.transform(col_test).ravel()

    # Combine all splits back together
    X_train_val = pd.concat([X_train_imputed, X_val_imputed])
    X_final = pd.concat([X_train_val, X_test_imputed])

    # Combine all target variables while maintaining order
    y_train_val = pd.concat([y_train, y_val])
    y_final = pd.concat([y_train_val, y_test_orig])

    # Ensure indices match
    X_final = X_final.reset_index(drop=True)
    y_final = y_final.reset_index(drop=True)

    # Combine features and target
    final_data = pd.concat([X_final, y_final], axis=1)

    # Verification step: Check for NaN values post-imputation
    if final_data.isnull().sum().any():
        print("Warning: There are still NaN values after imputation!")
        # Optionally, print which columns still have NaNs
        print(final_data.isnull().sum()[final_data.isnull().sum() > 0])
    else:
        print("No NaN values found after imputation.")

    print(f"Final data shape: {final_data.shape}")


    return final_data

#df_merged = mean_impute_dataframe(df_merged, y_vars=outcome_columns, test_size=0.25, val_size=0.25, random_state=1, seed=1)


def save_missing_percentage(
    df: pd.DataFrame, output_file: str = "percent_missing.pkl"
) -> Dict[str, float]:
    """Calculates and saves the percentage of missing values for each column.

    Args:
        df: The input DataFrame to analyze.
        output_file: The path to save the resulting dictionary as a pickle file.

    Returns:
        A dictionary where keys are column names and values are the
        percentage of missing values.
    """
    percent_missing = df.isnull().mean() * 100
    percent_missing = percent_missing.to_dict()

    with open(output_file, 'wb') as file:
        pickle.dump(percent_missing, file)

    print("Warning: ensure you rename the pickle file: training_data_filename + _percent_missing.pkl")

    return percent_missing


# Assuming df_merged is defined
#result = save_missing_percentage(df_merged)
#print(result)
