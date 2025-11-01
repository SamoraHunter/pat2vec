from datetime import date, datetime
import pandas as pd


def appendAge(dataFrame: pd.DataFrame) -> pd.DataFrame:
    """Calculates current age and appends it as a new 'age' column.

    Args:
        dataFrame: A DataFrame containing a 'client_dob' column with
            date of birth strings.

    Returns:
        The DataFrame with an added 'age' column.
    """

    def age(born):
        born = born.split(".")[0]
        born = datetime.strptime(born, "%Y-%m-%dT%H:%M:%S").date()
        today = date.today()
        return (
            today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        )

    dataFrame["age"] = dataFrame["client_dob"].apply(age)

    return dataFrame


def appendAgeAtRecord(dataFrame: pd.DataFrame) -> pd.DataFrame:
    """Calculates age at the time of record and appends it as 'ageAtRecord'.

    Args:
        dataFrame: A DataFrame with 'client_dob' and 'updatetime' columns.

    Returns:
        The DataFrame with an added 'ageAtRecord' column.
    """

    def ageAtRecord(row):
        born = datetime.strptime(
            row["client_dob"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
        ).date()
        updateTime = datetime.strptime(
            row["updatetime"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
        ).date()

        today = updateTime
        return (
            today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        )

    dataFrame["ageAtRecord"] = dataFrame.apply(ageAtRecord, axis=1)
    return dataFrame


def append_age_at_record_series(series: pd.Series) -> pd.Series:
    """Calculates age at record time for a single row (passed as a Series).

    Args:
        series: A pandas Series representing a single row, containing
            'client_dob' and 'updatetime'.

    Returns:
        The input Series with an added 'age' value.
    """

    def age_at_record(row):
        try:
            born = datetime.strptime(
                row["client_dob"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
            ).date()
        except Exception as e:
            born = datetime.strptime(
                row["client_dob"].iloc[0].split(".")[0], "%Y-%m-%dT%H:%M:%S"
            ).date()

        try:
            updateTime = row["updatetime"].date()
        except:
            updateTime = row["updatetime"].iloc[0].date()

        today = updateTime
        return (
            today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        )

    series["age"] = age_at_record(series)

    return series


def df_column_uniquify(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures all column names in a DataFrame are unique.

    If duplicate column names are found, they are made unique by appending a
    suffix (e.g., 'col_1', 'col_2').

    Args:
        df: The DataFrame to process.

    Returns:
        The DataFrame with unique column names.
    """
    df_columns = df.columns
    new_columns = []
    for item in df_columns:
        counter = 0
        newitem = item
        while newitem in new_columns:
            counter += 1
            newitem = "{}_{}".format(item, counter)
        new_columns.append(newitem)
    df.columns = new_columns
    return df
