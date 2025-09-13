import getpass
from typing import List

print("imported dummy credential file: this should be placed in gloabl_files")
# CogStack login details
# Any questions on what these details are please contact your local CogStack administrator.

hosts: List[str] = [  # Dummy Elasticsearch URL
    "https://localhost:9200"
]  # This is a list of your CogStack ElasticSearch instances.

# These are your login details (either via http_auth or API) Should be in str format
username = (
    "dummy_user"  # Warning, copy this file to gloabl_files before inputting credentials
)
# getpass.getpass(prompt='Enter your password for username:{}'.format(username))
password = "dummy_password"

host_name = "localhost"

port = "9200"

scheme = "https"

# NLM authentication
# The UMLS REST API requires a UMLS account for the authentication described below.
# If you do not have a UMLS account, you may apply for a license on the UMLS Terminology Services (UTS) website.
# https://documentation.uts.nlm.nih.gov/rest/authentication.html

# TODO: add option for UMLS api key auth


# SNOMED authentication from international and TRUD
# TODO add arg for api key auth
