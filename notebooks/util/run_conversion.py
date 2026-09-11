import pandas as pd

from pat2vec.util.patient_identifier_conversion import (
    convert_client_idcode_to_nhs_number,
)

df = pd.read_csv("test_files/treatment_docs.csv")
client_idcodes = df["client_idcode"].dropna().astype(str).unique().tolist()
print(f"Found {len(client_idcodes)} unique client_idcodes")

from pat2vec.main_pat2vec import main
from pat2vec.util.config_pat2vec import config_class

config_obj = config_class(testing=False)
pat2vec_obj = main(config_obj=config_obj, cogstack=True)

# Debug: Print config and authentication info
print("\n=== Conversion Configuration ===")
print(f"testing flag: {config_obj.testing}")
print(f"Project name: {getattr(config_obj, 'proj_name', 'N/A')}")
print(f"Credential path: {getattr(config_obj, 'credentials_path', 'N/A')}")
print(f"Elasticsearch hosts (from config): {getattr(pat2vec_obj, '_hosts', 'N/A')}")

nhs_numbers, missing = convert_client_idcode_to_nhs_number(
    client_idcodes=client_idcodes,
    pat2vec_obj=pat2vec_obj,
)

print("\nResults:")
print(f"Hospital numbers processed: {len(client_idcodes)}")
print(f"NHS numbers found: {len(nhs_numbers)}")

if nhs_numbers:
    print("\nNHS Numbers:")
    for i, nhs in enumerate(nhs_numbers[:10], 1):
        print(f"  {i}. {nhs}")
