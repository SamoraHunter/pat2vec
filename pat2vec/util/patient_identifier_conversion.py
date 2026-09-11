import re

import pandas as pd


def _get_column_case_insensitive(df: pd.DataFrame, column_name: str) -> pd.Series:
    """Get a DataFrame column in a case-insensitive manner.

    Args:
    ----
        df: The DataFrame to access.
        column_name: The column name (case-insensitive).

    Returns:
    -------
        The Series corresponding to the matching column.

    """
    if column_name in df.columns:
        return df[column_name]

    lower_columns = {col.lower(): col for col in df.columns}
    if column_name.lower() in lower_columns:
        return df[lower_columns[column_name.lower()]]

    msg = f"Column '{column_name}' not found (case-insensitive check also failed)"
    raise KeyError(
        msg,
    )


def extract_hospital_numbers(hospital_number_str: str) -> list[str]:
    """Extract hospital numbers from a comma-separated string.

    Args:
    ----
        hospital_number_str: A string containing one or more hospital numbers,
            separated by commas.

    Returns:
    -------
        A list of cleaned hospital number strings, or an empty list if the
        input is None, NaN, or empty.

    """
    if (
        pd.isna(hospital_number_str)
        or not hospital_number_str
        or hospital_number_str == ""
    ):
        return []

    parts = str(hospital_number_str).split(",")
    return [part.strip() for part in parts if part.strip()]


def extract_nhs_number(nhs_number_str: str) -> str | None:
    """Extract NHS number from a formatted string.

    Args:
    ----
        nhs_number_str: A string potentially containing an NHS number in the
            format "NHS XXX XXX XXXX" where X is a digit.

    Returns:
    -------
        The 10-digit NHS number as a string without spaces, or None if no valid
        NHS number is found.

    """
    if pd.isna(nhs_number_str) or not nhs_number_str or nhs_number_str == "":
        return None

    pattern = r"NHS\s*(\d{3}\s*\d{3}\s*\d{4})"
    match = re.search(pattern, str(nhs_number_str))

    if match:
        return re.sub(r"\s+", "", match.group(1))

    return None


def extract_mrn(mrn_str: str) -> str | None:
    """Extract MRN (Medical Record Number) from a formatted string.

    Args:
    ----
        mrn_str: A string potentially containing an MRN in the format "MRN XXX"
            or "MRN:XXX" where X is an alphanumeric character.

    Returns:
    -------
        The MRN string without the prefix, or None if no valid MRN is found.

    """
    if pd.isna(mrn_str) or not mrn_str or mrn_str == "":
        return None

    pattern = r"MRN\s*[:\s]*([A-Za-z0-9]+)"
    match = re.search(pattern, str(mrn_str))

    if match:
        return match.group(1)

    return None


def convert_hospital_number_to_durable_key(
    hospital_numbers: list[str],
    pat2vec_obj,
) -> tuple[str | None, list[str]]:
    """Convert a list of hospital numbers to a single durable key.

    Args:
    ----
        hospital_numbers: A list of hospital number strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (durable_key, missing_hospital_numbers). durable_key is the
        first durable key found for the given hospital numbers, or None if no
        match is found. missing_hospital_numbers is a list of input hospital
        numbers that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_HospitalNumber", "patient_DurableKey"],
        term_name="patient_HospitalNumber",
        entered_list=hospital_numbers,
        search_string="*",
    )

    if df.empty:
        return None, hospital_numbers.copy()

    found_hn = df["patient_HospitalNumber"].dropna().unique()
    missing_hn = [hn for hn in hospital_numbers if hn not in found_hn]

    if missing_hn:
        pass
    else:
        pass

    durable_keys = df["patient_DurableKey"].dropna().unique()

    if len(durable_keys) >= 1:
        return durable_keys[0], missing_hn

    return None, missing_hn


def convert_hospital_numbers_to_durable_keys(
    hospital_numbers: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of hospital numbers to their corresponding durable keys.

    Args:
    ----
        hospital_numbers: A list of hospital number strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (durable_keys, missing_hospital_numbers). durable_keys is a
        list of unique durable keys corresponding to the given hospital numbers,
        preserving order of first occurrence. missing_hospital_numbers is a list
        of input hospital numbers that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_HospitalNumber", "patient_DurableKey"],
        term_name="patient_HospitalNumber",
        entered_list=hospital_numbers,
        search_string="*",
    )

    if df.empty:
        return [], hospital_numbers.copy()

    found_hn = df["patient_HospitalNumber"].dropna().unique()
    missing_hn = [hn for hn in hospital_numbers if hn not in found_hn]

    if missing_hn:
        pass
    else:
        pass

    durable_keys = df["patient_DurableKey"].dropna().unique().tolist()
    return list(dict.fromkeys(durable_keys)), missing_hn


def convert_durable_key_to_hospital_numbers(
    durable_key: str,
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a durable key to its corresponding hospital numbers.

    Args:
    ----
        durable_key: A durable key string.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (hospital_numbers, missing_durable_keys). hospital_numbers is a
        list of unique hospital number strings corresponding to the given durable
        key, preserving order of first occurrence. missing_durable_keys is a list
        of input durable keys that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_HospitalNumber", "patient_DurableKey"],
        term_name="patient_DurableKey",
        entered_list=[durable_key],
        search_string="*",
    )

    if df.empty:
        return [], [durable_key]

    hospital_numbers = df["patient_HospitalNumber"].dropna().tolist()

    result = []
    for hn in hospital_numbers:
        if isinstance(hn, str):
            result.append(hn)
        elif isinstance(hn, list):
            result.extend([str(x) for x in hn])

    unique_result = list(dict.fromkeys(result))

    return unique_result, []


def convert_nhs_number_to_durable_key(
    nhs_numbers: list[str],
    pat2vec_obj,
) -> tuple[str | None, list[str]]:
    """Convert a list of NHS numbers to a single durable key.

    Args:
    ----
        nhs_numbers: A list of NHS number strings (10 digits without spaces).
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (durable_key, missing_nhs_numbers). durable_key is the first
        durable key found for the given NHS numbers, or None if no match is
        found. missing_nhs_numbers is a list of input NHS numbers that were not
        found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_NhsNumber", "patient_DurableKey"],
        term_name="patient_NhsNumber",
        entered_list=nhs_numbers,
        search_string="*",
    )

    if df.empty:
        return None, nhs_numbers.copy()

    found_nhs = df["patient_NhsNumber"].dropna().unique()
    missing_nhs = [nhs for nhs in nhs_numbers if nhs not in found_nhs]

    if missing_nhs:
        pass
    else:
        pass

    durable_keys = df["patient_DurableKey"].dropna().unique()

    if len(durable_keys) >= 1:
        return durable_keys[0], missing_nhs

    return None, missing_nhs


def convert_durable_key_to_nhs_numbers(
    durable_key: str,
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a durable key to its corresponding NHS numbers.

    Args:
    ----
        durable_key: A durable key string.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (nhs_numbers, missing_durable_keys). nhs_numbers is a list of
        unique NHS number strings corresponding to the given durable key,
        preserving order of first occurrence. missing_durable_keys is a list of
        input durable keys that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_NhsNumber", "patient_DurableKey"],
        term_name="patient_DurableKey",
        entered_list=[durable_key],
        search_string="*",
    )

    if df.empty:
        return [], [durable_key]

    nhs_numbers = df["patient_NhsNumber"].dropna().tolist()

    result = []
    for nhs in nhs_numbers:
        extracted = str(nhs)
        if extracted and extracted != "None":
            result.append(extracted)

    unique_result = list(dict.fromkeys(result))

    return unique_result, []


def convert_mrn_to_durable_key(
    mrns: list[str],
    pat2vec_obj,
) -> tuple[str | None, list[str]]:
    """Convert a list of MRNs to a single durable key.

    Args:
    ----
        mrns: A list of MRN strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (durable_key, missing_mrns). durable_key is the first durable
        key found for the given MRNs, or None if no match is found.
        missing_mrns is a list of input MRNs that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_MRN", "patient_DurableKey"],
        term_name="patient_MRN",
        entered_list=mrns,
        search_string="*",
    )

    if df.empty:
        return None, mrns.copy()

    found_mrn = _get_column_case_insensitive(df, "patient_MRN").dropna().unique()
    missing_mrn = [mrn for mrn in mrns if mrn not in found_mrn]

    if missing_mrn:
        pass
    else:
        pass

    durable_keys = df["patient_DurableKey"].dropna().unique()

    if len(durable_keys) >= 1:
        return durable_keys[0], missing_mrn

    return None, missing_mrn


def convert_durable_key_to_mrn(
    durable_key: str,
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a durable key to its corresponding MRNs.

    Args:
    ----
        durable_key: A durable key string.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (mrns, missing_durable_keys). mrns is a list of unique MRN
        strings corresponding to the given durable key, preserving order of first
        occurrence. missing_durable_keys is a list of input durable keys that were
        not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_MRN", "patient_DurableKey"],
        term_name="patient_DurableKey",
        entered_list=[durable_key],
        search_string="*",
    )

    if df.empty:
        return [], [durable_key]

    mrns = _get_column_case_insensitive(df, "patient_MRN").dropna().tolist()

    result = []
    for mrn in mrns:
        extracted = str(mrn)
        if extracted and extracted != "None":
            result.append(extracted)

    unique_result = list(dict.fromkeys(result))

    return unique_result, []


def convert_durable_keys_to_hospital_numbers(
    durable_keys: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of durable keys to their corresponding hospital numbers.

    Args:
    ----
        durable_keys: A list of durable key strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (hospital_numbers, missing_durable_keys). hospital_numbers is a
        list of unique hospital number strings corresponding to the given durable
        keys, preserving order of first occurrence. missing_durable_keys is a
        list of input durable keys that were not found.

    """
    all_hospital_numbers = []
    missing_dk = []
    for dk in durable_keys:
        hns, missing = convert_durable_key_to_hospital_numbers(dk, pat2vec_obj)
        all_hospital_numbers.extend(hns)
        if missing:
            missing_dk.extend(missing)
    return list(dict.fromkeys(all_hospital_numbers)), missing_dk


def convert_nhs_numbers_to_durable_keys(
    nhs_numbers: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of NHS numbers to their corresponding durable keys.

    Args:
    ----
        nhs_numbers: A list of NHS number strings (10 digits without spaces).
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (durable_keys, missing_nhs_numbers). durable_keys is a list of
        unique durable keys corresponding to the given NHS numbers, preserving
        order of first occurrence. missing_nhs_numbers is a list of input NHS
        numbers that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_NhsNumber", "patient_DurableKey"],
        term_name="patient_NhsNumber",
        entered_list=nhs_numbers,
        search_string="*",
    )

    if df.empty:
        return [], nhs_numbers.copy()

    found_nhs = df["patient_NhsNumber"].dropna().unique()
    missing_nhs = [nhs for nhs in nhs_numbers if nhs not in found_nhs]

    if missing_nhs:
        pass
    else:
        pass

    durable_keys = df["patient_DurableKey"].dropna().unique().tolist()
    return list(dict.fromkeys(durable_keys)), missing_nhs


def convert_durable_keys_to_nhs_numbers(
    durable_keys: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of durable keys to their corresponding NHS numbers.

    Args:
    ----
        durable_keys: A list of durable key strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (nhs_numbers, missing_durable_keys). nhs_numbers is a list of
        unique NHS number strings corresponding to the given durable keys,
        preserving order of first occurrence. missing_durable_keys is a list of
        input durable keys that were not found.

    """
    all_nhs_numbers = []
    missing_dk = []
    for dk in durable_keys:
        nhs, missing = convert_durable_key_to_nhs_numbers(dk, pat2vec_obj)
        all_nhs_numbers.extend(nhs)
        if missing:
            missing_dk.extend(missing)
    return list(dict.fromkeys(all_nhs_numbers)), missing_dk


def convert_mrns_to_durable_keys(
    mrns: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of MRNs to their corresponding durable keys.

    Args:
    ----
        mrns: A list of MRN strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (durable_keys, missing_mrns). durable_keys is a list of unique
        durable keys corresponding to the given MRNs, preserving order of first
        occurrence. missing_mrns is a list of input MRNs that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_MRN", "patient_DurableKey"],
        term_name="patient_MRN",
        entered_list=mrns,
        search_string="*",
    )

    if df.empty:
        return [], mrns.copy()

    found_mrn = _get_column_case_insensitive(df, "patient_MRN").dropna().unique()
    missing_mrn = [mrn for mrn in mrns if mrn not in found_mrn]

    if missing_mrn:
        pass
    else:
        pass

    durable_keys = df["patient_DurableKey"].dropna().unique().tolist()
    return list(dict.fromkeys(durable_keys)), missing_mrn


def convert_durable_keys_to_mrns(
    durable_keys: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of durable keys to their corresponding MRNs.

    Args:
    ----
        durable_keys: A list of durable key strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (mrns, missing_durable_keys). mrns is a list of unique MRN
        strings corresponding to the given durable keys, preserving order of first
        occurrence. missing_durable_keys is a list of input durable keys that were
        not found.

    """
    all_mrns = []
    missing_dk = []
    for dk in durable_keys:
        mrn_list, missing = convert_durable_key_to_mrn(dk, pat2vec_obj)
        all_mrns.extend(mrn_list)
        if missing:
            missing_dk.extend(missing)
    return list(dict.fromkeys(all_mrns)), missing_dk


def extract_source_id(source_id_str: str) -> str | None:
    """Extract Source ID from a formatted string.

    Args:
    ----
        source_id_str: A string potentially containing a Source ID.

    Returns:
    -------
        The Source ID string, or None if no valid Source ID is found.

    """
    if pd.isna(source_id_str) or not source_id_str or source_id_str == "":
        return None

    pattern = r"SourceID\s*[:\s]*([A-Za-z0-9]+)"
    match = re.search(pattern, str(source_id_str))

    if match:
        return match.group(1)

    return None


def convert_source_id_to_durable_key(
    source_ids: list[str],
    pat2vec_obj,
) -> tuple[str | None, list[str]]:
    """Convert a list of Source IDs to a single durable key.

    Args:
    ----
        source_ids: A list of Source ID strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (durable_key, missing_source_ids). durable_key is the first
        durable key found for the given Source IDs, or None if no match is found.
        missing_source_ids is a list of input Source IDs that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_SourceId", "patient_DurableKey"],
        term_name="patient_SourceId",
        entered_list=source_ids,
        search_string="*",
    )

    if df.empty:
        return None, source_ids.copy()

    found_sid = _get_column_case_insensitive(df, "patient_SourceId").dropna().unique()
    missing_sid = [sid for sid in source_ids if sid not in found_sid]

    if missing_sid:
        pass

    durable_keys = df["patient_DurableKey"].dropna().unique()

    if len(durable_keys) >= 1:
        return durable_keys[0], missing_sid

    return None, missing_sid


def convert_durable_key_to_source_id(
    durable_key: str,
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a durable key to its corresponding Source IDs.

    Args:
    ----
        durable_key: A durable key string.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (source_ids, missing_durable_keys). source_ids is a list of
        unique Source ID strings corresponding to the given durable key,
        preserving order of first occurrence. missing_durable_keys is a list of
        input durable keys that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_SourceId", "patient_DurableKey"],
        term_name="patient_DurableKey",
        entered_list=[durable_key],
        search_string="*",
    )

    if df.empty:
        return [], [durable_key]

    source_ids = _get_column_case_insensitive(df, "patient_SourceId").dropna().tolist()

    result = []
    for sid in source_ids:
        extracted = str(sid)
        if extracted and extracted != "None":
            result.append(extracted)

    unique_found = list(dict.fromkeys(result))

    return unique_found, []


def convert_source_ids_to_durable_keys(
    source_ids: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of Source IDs to their corresponding durable keys.

    Args:
    ----
        source_ids: A list of Source ID strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (durable_keys, missing_source_ids). durable_keys is a list of
        unique durable keys corresponding to the given Source IDs, preserving
        order of first occurrence. missing_source_ids is a list of input Source
        IDs that were not found.

    """
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epic_patients",
        fields_list=["patient_SourceId", "patient_DurableKey"],
        term_name="patient_SourceId",
        entered_list=source_ids,
        search_string="*",
    )

    if df.empty:
        return [], source_ids.copy()

    found_sid = _get_column_case_insensitive(df, "patient_SourceId").dropna().unique()
    missing_sid = [sid for sid in source_ids if sid not in found_sid]

    if missing_sid:
        pass

    durable_keys = df["patient_DurableKey"].dropna().unique().tolist()
    return list(dict.fromkeys(durable_keys)), missing_sid


def convert_durable_keys_to_source_ids(
    durable_keys: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of durable keys to their corresponding Source IDs.

    Args:
    ----
        durable_keys: A list of durable key strings.
        pat2vec_obj: An initialized pat2vec object with the
            cohort_searcher_with_terms_and_search method.

    Returns:
    -------
        A tuple of (source_ids, missing_durable_keys). source_ids is a list of
        unique Source ID strings corresponding to the given durable keys,
        preserving order of first occurrence. missing_durable_keys is a list of
        input durable keys that were not found.

    """
    all_source_ids = []
    missing_dk = []
    for dk in durable_keys:
        sid_list, missing = convert_durable_key_to_source_id(dk, pat2vec_obj)
        all_source_ids.extend(sid_list)
        if missing:
            missing_dk.extend(missing)
    return list(dict.fromkeys(all_source_ids)), missing_dk


def convert_client_idcode_to_nhs_number(
    client_idcodes: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of client_idcodes (hospital numbers) to NHS numbers using epr_documents.

    For legacy data in the epr_documents index:
    - client_idcode = Hospital number
    - client_universalnumber = NHS number

    Args:
        client_idcodes: A list of hospital number strings (client_idcode values)
        pat2vec_obj: An initialized pat2vec object with cohort_searcher_with_terms_and_search method.

    Returns:
        A tuple of (nhs_numbers, missing_client_idcodes). nhs_numbers is a list of unique
        NHS number strings. missing_client_idcodes are those not found or without NHS numbers.

    """
    from pat2vec.util.elasticsearch_index_config import EPR_DOCS_FIELDS

    start_year = str(pat2vec_obj.config_obj.global_start_year).zfill(4)
    start_month = str(pat2vec_obj.config_obj.global_start_month).zfill(2)
    start_day = str(pat2vec_obj.config_obj.global_start_day).zfill(2)
    end_year = str(pat2vec_obj.config_obj.global_end_year).zfill(4)
    end_month = str(pat2vec_obj.config_obj.global_end_month).zfill(2)
    end_day = str(pat2vec_obj.config_obj.global_end_day).zfill(2)

    search_string = (
        f"updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-"
        f"{end_month}-{end_day}]"
    )

    fields_to_use = list(EPR_DOCS_FIELDS)
    if "client_universalnumber" not in fields_to_use:
        fields_to_use.append("client_universalnumber")
    if "client_idcode" not in fields_to_use:
        fields_to_use.insert(0, "client_idcode")

    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epr_documents",
        fields_list=fields_to_use,
        term_name=pat2vec_obj.config_obj.client_idcode_term_name,
        entered_list=client_idcodes,
        search_string=search_string,
    )

    if df.empty:
        return [], client_idcodes.copy()

    if "updatetime" in df.columns and not df.empty:
        df = df.copy()
        df["updatetime"] = pd.to_datetime(df["updatetime"], utc=True)
        df = df.sort_values("updatetime", ascending=False)
        df = df.drop_duplicates(subset=["client_idcode"], keep="first")

    nhs_numbers = []
    for nhs in df["client_universalnumber"].dropna():
        if isinstance(nhs, (list, tuple)):
            nhs_numbers.extend(
                [str(x).strip() for x in nhs if pd.notna(x) and str(x).strip()],
            )
        elif pd.notna(nhs) and str(nhs).strip():
            nhs_numbers.append(str(nhs).strip())

    unique_nhs = list(dict.fromkeys(nhs_numbers))

    found_ids_with_nhs = df[df["client_universalnumber"].notna()][
        "client_idcode"
    ].unique()
    missing_ids = [cid for cid in client_idcodes if cid not in found_ids_with_nhs]

    return unique_nhs, missing_ids


def convert_nhs_number_to_client_idcode(
    nhs_numbers: list[str],
    pat2vec_obj,
) -> tuple[list[str], list[str]]:
    """Convert a list of NHS numbers to client_idcodes (hospital numbers) using epr_documents.

    For legacy data in the epr_documents index:
    - client_idcode = Hospital number
    - client_universalnumber = NHS number

    Args:
        nhs_numbers: A list of NHS number strings
        pat2vec_obj: An initialized pat2vec object with cohort_searcher_with_terms_and_search method.

    Returns:
        A tuple of (client_idcodes, missing_nhs_numbers). client_idcodes is a list of unique
        hospital numbers. missing_nhs_numbers are those not found in epr_documents.

    """
    from pat2vec.util.elasticsearch_index_config import EPR_DOCS_FIELDS

    start_year = str(pat2vec_obj.config_obj.global_start_year).zfill(4)
    start_month = str(pat2vec_obj.config_obj.global_start_month).zfill(2)
    start_day = str(pat2vec_obj.config_obj.global_start_day).zfill(2)
    end_year = str(pat2vec_obj.config_obj.global_end_year).zfill(4)
    end_month = str(pat2vec_obj.config_obj.global_end_month).zfill(2)
    end_day = str(pat2vec_obj.config_obj.global_end_day).zfill(2)

    search_string = (
        f"updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-"
        f"{end_month}-{end_day}]"
    )

    fields_to_use = list(EPR_DOCS_FIELDS)
    if "client_universalnumber" not in fields_to_use:
        fields_to_use.append("client_universalnumber")
    if "client_idcode" not in fields_to_use:
        fields_to_use.insert(0, "client_idcode")

    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="epr_documents",
        fields_list=fields_to_use,
        term_name="client_universalnumber",
        entered_list=nhs_numbers,
        search_string=search_string,
    )

    if df.empty:
        return [], nhs_numbers.copy()

    if "updatetime" in df.columns and not df.empty:
        df = df.copy()
        df["updatetime"] = pd.to_datetime(df["updatetime"], utc=True)
        df = df.sort_values("updatetime", ascending=False)
        df = df.drop_duplicates(subset=["client_universalnumber"], keep="first")

    client_idcodes = []
    for cid in df["client_idcode"].dropna():
        if isinstance(cid, (list, tuple)):
            client_idcodes.extend(
                [str(x).strip() for x in cid if pd.notna(x) and str(x).strip()],
            )
        elif pd.notna(cid) and str(cid).strip():
            client_idcodes.append(str(cid).strip())

    unique_cid = list(dict.fromkeys(client_idcodes))

    found_nhs = df["client_universalnumber"].dropna().unique()
    found_nhs_str = [str(n).strip() for n in found_nhs if pd.notna(n)]
    missing_nhs = [nhs for nhs in nhs_numbers if str(nhs).strip() not in found_nhs_str]

    return unique_cid, missing_nhs


__all__ = [
    "convert_client_idcode_to_nhs_number",
    "convert_durable_key_to_hospital_numbers",
    "convert_durable_key_to_mrn",
    "convert_durable_key_to_nhs_numbers",
    "convert_durable_key_to_source_id",
    "convert_durable_keys_to_hospital_numbers",
    "convert_durable_keys_to_mrns",
    "convert_durable_keys_to_nhs_numbers",
    "convert_durable_keys_to_source_ids",
    "convert_hospital_number_to_durable_key",
    "convert_hospital_numbers_to_durable_keys",
    "convert_mrn_to_durable_key",
    "convert_mrns_to_durable_keys",
    "convert_nhs_number_to_client_idcode",
    "convert_nhs_number_to_durable_key",
    "convert_nhs_numbers_to_durable_keys",
    "convert_source_id_to_durable_key",
    "convert_source_ids_to_durable_keys",
    "extract_hospital_numbers",
    "extract_mrn",
    "extract_nhs_number",
    "extract_source_id",
]
