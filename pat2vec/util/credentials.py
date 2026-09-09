"""CogStack credential management and validation.

This module provides credential configuration and validation for Elasticsearch
connections. Support for environment variable overrides is available:

- ELASTIC_USERNAME: Override default username
- ELASTIC_PASSWORD: Override default password
- ELASTIC_HOST_NAME: Override host name
- ELASTIC_PORT: Override port (default: 19200 for test instances)
- ELASTIC_SCHEME: Override scheme (http/https, default: https)

Any questions on what these details are please contact your local CogStack administrator.
"""

import os

hosts: list[str] = [  # Dummy Elasticsearch URL (test instance port)
    "http://localhost:19200",
]  # This is a list of your CogStack ElasticSearch instances.

# These are your login details (either via http_auth or API) Should be in str format
username: str = (
    "dummy_user"  # Warning, copy this file to gloabal_files before inputting credentials
)
# getpass.getpass(prompt='Enter your password for username:{}'.format(username))
password: str = "dummy_password"

host_name: str = "localhost"

port: str = "19200"

scheme: str = "http"


def validate_credentials(
    username_arg: str | None = None,
    password_arg: str | None = None,
    hosts_arg: list | None = None,
) -> tuple[bool, list[str]]:
    """Validate credential configuration before attempting Elasticsearch connection.

    Performs early validation of credentials to provide clear error messages
    before trying to connect to Elasticsearch.

    Args:
    ----
        username_arg: Username for authentication. If None, reads from os.environ
            or uses environment variable ELASTIC_USERNAME.
        password_arg: Password for authentication. If None, reads from os.environ
            or uses environment variable ELASTIC_PASSWORD.
        hosts_arg: List of Elasticsearch host URLs. If None, defaults to localhost.

    Returns:
    -------
        tuple[bool, list[str]]: (is_valid, error_messages)
            - is_valid: True if all validations pass, False otherwise
            - error_messages: List of error message strings (empty if valid)

    Example:
    -------
        >>> is_valid, errors = validate_credentials()
        >>> if not is_valid:
        ...     for err in errors:
        ...         print(f"Error: {err}")

    Raises:
    ------
        ValueError: If credentials are invalid or missing.

    """
    errors: list[str] = []

    resolved_username = username_arg
    resolved_password = password_arg
    resolved_hosts = hosts_arg

    if resolved_username is None:
        resolved_username = os.environ.get("ELASTIC_USERNAME")
    if resolved_password is None:
        resolved_password = os.environ.get("ELASTIC_PASSWORD")
    if resolved_hosts is None:
        resolved_hosts = hosts

    if not resolved_username or not isinstance(resolved_username, str):
        errors.append(
            "Username is missing or invalid. Set ELASTIC_USERNAME environment variable "
            "or configure username in credentials.py",
        )
    elif not resolved_username.strip():
        errors.append(
            "Username cannot be empty or whitespace-only. "
            "Set ELASTIC_USERNAME environment variable or configure username in credentials.py",
        )

    if not resolved_password or not isinstance(resolved_password, str):
        errors.append(
            "Password is missing or invalid. Set ELASTIC_PASSWORD environment variable "
            "or configure password in credentials.py",
        )
    elif not resolved_password.strip():
        errors.append(
            "Password cannot be empty or whitespace-only. "
            "Set ELASTIC_PASSWORD environment variable or configure password in credentials.py",
        )

    if (
        not resolved_hosts
        or not isinstance(resolved_hosts, list)
        or len(resolved_hosts) == 0
    ):
        errors.append(
            "At least one Elasticsearch host must be configured. "
            "Set ELASTIC_HOSTS environment variable (comma-separated) or configure hosts in credentials.py",
        )
    elif any(not isinstance(h, str) or not h.strip() for h in resolved_hosts):
        invalid_hosts = [
            h for h in resolved_hosts if not isinstance(h, str) or not h.strip()
        ]
        errors.append(
            f"Invalid host(s) configured: {invalid_hosts}. "
            "All hosts must be non-empty strings with valid URLs.",
        )

    is_valid = len(errors) == 0

    return is_valid, errors


# NLM authentication
# The UMLS REST API requires a UMLS account for the authentication described below.
# If you do not have a UMLS account, you may apply for a license on the UMLS Terminology Services (UTS) website.
# https://documentation.uts.nlm.nih.gov/rest/authentication.html

# TODO: add option for UMLS api key auth


# SNOMED authentication from international and TRUD
# TODO add arg for api key auth
