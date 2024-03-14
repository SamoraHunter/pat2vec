from typing import List

# CogStack login details
# Any questions on what these details are please contact your local CogStack administrator.

hosts: List[str] = [  # Dummy Elasticsearch URL
    "https://localhost:9200"
]  # This is a list of your CogStack ElasticSearch instances.

# These are your login details (either via http_auth or API) Should be in str format
username = 'dummy_user'
password = 'dummy_password'

host_name = 'localhost'

port = '9200'

scheme = 'https'

# NLM authentication
# The UMLS REST API requires a UMLS account for the authentication described below.
# If you do not have a UMLS account, you may apply for a license on the UMLS Terminology Services (UTS) website.
# https://documentation.uts.nlm.nih.gov/rest/authentication.html

# TODO: add option for UMLS api key auth


# SNOMED authentication from international and TRUD
# TODO add arg for api key auth
