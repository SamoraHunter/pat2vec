from datetime import date, datetime


def appendAge(dataFrame):
    """Creates a current age column and adds to dataframe supplied"""

    def age(born):
        born = born.split(".")[0]
        born = datetime.strptime(born, "%Y-%m-%dT%H:%M:%S").date()
        today = date.today()
        return (
            today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        )

    dataFrame["age"] = dataFrame["client_dob"].apply(age)

    return dataFrame


def appendAgeAtRecord(dataFrame):
    """Creates an age at update time column 'ageAtRecord' and computes using clients date of birth and update time"""

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


def append_age_at_record_series(series):
    def age_at_record(row):
        try:
            born = datetime.strptime(
                row["client_dob"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
            ).date()
        except Exception as e:
            #         print(e)
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


def df_column_uniquify(df):
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
