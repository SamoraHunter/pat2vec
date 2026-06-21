"""Tests for pat2vec/util/credentials.py - security and configuration validation."""

from pat2vec.util.credentials import (
    hosts,
    username,
    password,
    host_name,
    port,
    scheme,
)


def test_credentials_module_importable():
    """Verify credentials module can be imported successfully."""
    from pat2vec.util import credentials

    assert credentials is not None


def test_hosts_is_list():
    """Verify hosts is defined as a list."""
    assert isinstance(hosts, list)
    assert len(hosts) > 0


def test_username_defined():
    """Verify username credential is defined (should be dummy for development)."""
    assert username is not None
    assert isinstance(username, str)
    # Should contain 'dummy' indicating this is a placeholder
    assert "dummy" in username.lower()


def test_password_defined():
    """Verify password credential is defined (should be dummy for development)."""
    assert password is not None
    assert isinstance(password, str)


def test_host_name_defined():
    """Verify host_name is defined."""
    assert host_name is not None
    assert isinstance(host_name, str)


def test_port_defined():
    """Verify port is defined."""
    assert port is not None
    assert isinstance(port, str)


def test_scheme_defined():
    """Verify scheme is defined."""
    assert scheme is not None
    assert isinstance(scheme, str)
    # Should be https for secure connections
    assert scheme in ["http", "https"]


def test_credentials_structure_valid():
    """Verify all required credential fields exist and have correct types."""
    from pat2vec.util import credentials

    assert hasattr(credentials, "hosts")
    assert hasattr(credentials, "username")
    assert hasattr(credentials, "password")
    assert hasattr(credentials, "host_name")
    assert hasattr(credentials, "port")
    assert hasattr(credentials, "scheme")


def test_credentials_for_development_only():
    """Verify credentials are set to dummy values (security check)."""
    # In production, these should be changed via environment variables or config
    assert (
        "dummy" in username.lower() or "change_me" in password.lower()
    ), "Production deployment must override default credentials"
