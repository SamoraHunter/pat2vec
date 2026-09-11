import pandas as pd

from pat2vec.util.patient_identifier_conversion import (
    convert_client_idcode_to_nhs_number,
)

df = pd.read_csv("test_files/treatment_docs.csv")
client_idcodes = df["client_idcode"].dropna().astype(str).unique().tolist()

from pat2vec.main_pat2vec import main
from pat2vec.util.config_pat2vec import config_class

config_obj = config_class(testing=False)
pat2vec_obj = main(config_obj=config_obj, cogstack=True)

nhs_numbers, missing = convert_client_idcode_to_nhs_number(
    client_idcodes=client_idcodes,
    pat2vec_obj=pat2vec_obj,
)

if nhs_numbers:
    for i, nhs in enumerate(nhs_numbers[:10], 1):
        print(f"  {i}. {nhs}")
