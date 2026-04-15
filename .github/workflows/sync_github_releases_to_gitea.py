"""
Recommended .env file structure:

# .env
# This file stores environment variables for local development.
# It should NOT be committed to version control.

GITHUB_TOKEN=""
GITEA_TOKEN=""
GITHUB_REPO_OWNER="" # e.g., 'SamoraHunter'
GITHUB_REPO_NAME="pat2vec" # e.g., 'pat2vec'
GITEA_URL="" # Your Gitea instance URL, e.g., 'http://localhost:3000'
GITEA_REPO_OWNER="" # e.g., 'SamoraHunter'
GITEA_REPO_NAME="pat2vec" # e.g., 'pat2vec'
REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"
"""

import subprocess
import sys
import os
import requests
import json
import tempfile
import shutil
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# Global SSL verification setting.
# In corporate environments with SSL-intercepting proxies, SSL verification
# can fail if the corporate CA bundle is not correctly configured.
# Set VERIFY_SSL=false in your .env file to skip verification (not recommended).
VERIFY_SSL = os.getenv("VERIFY_SSL", "true").lower() == "true"


def _get_gitea_api_base(gitea_url):
    """Extracts the base API URL from a Gitea URL, stripping any repo paths."""
    parsed = urlparse(gitea_url)
    # This ensures we get scheme://host:port/api/v1 even if the user provided a repo URL
    return f"{parsed.scheme}://{parsed.netloc}/api/v1"


def run_command(command, check_output=False, suppress_output=False):
    """Helper function to run shell commands."""
    try:
        if suppress_output:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            if result.stderr:
                print(
                    f"Warning (suppressed cmd output): {result.stderr.strip()}",
                    file=sys.stderr,
                )
            return result.stdout.strip()
        else:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            print(result.stdout.strip())
            if result.stderr:
                print(f"Warning: {result.stderr.strip()}", file=sys.stderr)
            return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(command)}", file=sys.stderr)
        print(f"Stderr: {e.stderr.strip()}", file=sys.stderr)
        print(f"Stdout: {e.stdout.strip()}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(
            f"Error: Command '{command[0]}' not found. Please ensure it's installed and in your PATH.",
            file=sys.stderr,
        )
        sys.exit(1)


def _api_request(
    url, method, token, headers=None, data=None, json_data=None, files=None
):
    """Helper for making authenticated API requests."""
    _headers = {"Authorization": f"token {token}"}
    if headers:
        _headers.update(headers)

    try:
        if method == "GET":
            response = requests.get(
                url, headers=_headers, timeout=30, verify=VERIFY_SSL
            )
        elif method == "POST":
            response = requests.post(
                url,
                headers=_headers,
                data=data,
                json=json_data,
                files=files,
                timeout=60,
                verify=VERIFY_SSL,
            )
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()  # Raise an exception for HTTP errors
        return response
    except requests.exceptions.RequestException as e:
        print(f"API request failed ({method} {url}): {e}", file=sys.stderr)
        if hasattr(e, "response") and e.response is not None:
            print(f"Response status: {e.response.status_code}", file=sys.stderr)
            try:
                print(f"Response body: {e.response.json()}", file=sys.stderr)
            except json.JSONDecodeError:
                print(f"Response body: {e.response.text}", file=sys.stderr)
        sys.exit(1)


def get_github_releases_data(owner, repo, token):
    """Fetches all releases from GitHub API."""
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    print(f"Fetching GitHub releases from {url}...")
    response = _api_request(url, "GET", token)
    return response.json()


def get_gitea_releases_data(gitea_url, owner, repo, token):
    """Fetches all releases from Gitea API."""
    api_base = _get_gitea_api_base(gitea_url)
    url = f"{api_base}/repos/{owner}/{repo}/releases"
    print(f"Fetching Gitea releases from {url}...")
    response = _api_request(url, "GET", token)
    return response.json()


def create_gitea_release(
    gitea_url, owner, repo, token, tag_name, name, body, draft, prerelease
):
    """Creates a new release on Gitea."""
    api_base = _get_gitea_api_base(gitea_url)
    url = f"{api_base}/repos/{owner}/{repo}/releases"
    payload = {
        "tag_name": tag_name,
        "name": name,
        "body": body,
        "draft": draft,
        "prerelease": prerelease,
    }
    print(f"Creating Gitea release for tag '{tag_name}'...")
    response = _api_request(url, "POST", token, json_data=payload)

    data = response.json()
    # If the API returns a list (some proxies/versions wrap responses), return the first element
    if isinstance(data, list):
        if len(data) > 0:
            return data[0]
        print(
            "Error: Gitea API returned an empty list for release creation.",
            file=sys.stderr,
        )
        sys.exit(1)
    return data


def download_github_asset(asset_url, gh_token, output_path):
    """Downloads a release asset from GitHub."""
    headers = {
        "Authorization": f"token {gh_token}",
        "Accept": "application/octet-stream",
    }
    print(f"  - Downloading asset from GitHub: {asset_url}...")
    try:
        response = requests.get(
            asset_url, headers=headers, stream=True, timeout=120, verify=VERIFY_SSL
        )
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  - Downloaded to {output_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading asset {asset_url}: {e}", file=sys.stderr)
        return False


def upload_gitea_release_asset(
    gitea_url, owner, repo, release_id, token, asset_name, file_path, content_type
):
    """Uploads an asset to a Gitea release."""
    api_base = _get_gitea_api_base(gitea_url)

    url = f"{api_base}/repos/{owner}/{repo}/releases/{release_id}/assets?name={asset_name}"
    headers = {"Content-Type": content_type}
    print(f"  - Uploading asset '{asset_name}' to Gitea release {release_id}...")
    try:
        with open(file_path, "rb") as f:
            response = _api_request(url, "POST", token, headers=headers, data=f.read())
        print(f"  - Asset '{asset_name}' uploaded successfully.")
        return response.json()
    except Exception as e:
        print(f"Error uploading asset '{asset_name}': {e}", file=sys.stderr)
        return None


def sync_releases_to_gitea(
    github_repo_owner,
    github_repo_name,
    github_token,
    gitea_url,
    gitea_repo_owner,
    gitea_repo_name,
    gitea_token,
    github_remote_name="origin",
    gitea_remote_name="gitea",
):
    """
    Synchronizes Git tags and GitHub releases (including assets) to Gitea.
    """
    print("--- Starting Git Tag Synchronization ---")
    run_command(["git", "fetch", github_remote_name, "--tags"])
    print(f"Tags from {github_remote_name} fetched successfully.")
    # Use --force to ensure the Gitea mirror tags are perfectly in sync with the source
    run_command(["git", "push", gitea_remote_name, "--tags", "--force"])
    print(f"All tags pushed to {gitea_remote_name}.")
    print("--- Git Tag Synchronization Complete ---")

    print("\n--- Starting Release Synchronization ---")

    # 1. Get GitHub releases
    github_releases = get_github_releases_data(
        github_repo_owner, github_repo_name, github_token
    )
    github_releases_by_tag = {r["tag_name"]: r for r in github_releases}
    print(f"Found {len(github_releases)} releases on GitHub.")

    # 2. Get Gitea releases
    gitea_releases = get_gitea_releases_data(
        gitea_url, gitea_repo_owner, gitea_repo_name, gitea_token
    )
    gitea_releases_by_tag = {r["tag_name"]: r for r in gitea_releases}
    print(f"Found {len(gitea_releases)} releases on Gitea.")

    # Create a temporary directory for assets
    temp_dir = tempfile.mkdtemp()
    print(f"Using temporary directory for assets: {temp_dir}")

    try:
        # 3. Iterate and sync
        for tag_name, gh_release in github_releases_by_tag.items():
            print(f"\nProcessing release for tag: {tag_name}")
            if tag_name not in gitea_releases_by_tag:
                print(f"  Release '{tag_name}' not found on Gitea. Creating it...")
                # Create release on Gitea
                gitea_created_release = create_gitea_release(
                    gitea_url,
                    gitea_repo_owner,
                    gitea_repo_name,
                    gitea_token,
                    tag_name=gh_release["tag_name"],
                    name=gh_release["name"] or gh_release["tag_name"],
                    body=gh_release["body"] or "",
                    draft=gh_release["draft"],
                    prerelease=gh_release["prerelease"],
                )

                # Safeguard: ensure we have a valid ID before proceeding
                release_id = gitea_created_release.get("id")
                if release_id is None:
                    raise KeyError(
                        f"Gitea release created but 'id' missing from response: {gitea_created_release}"
                    )

                print(f"  Release '{tag_name}' created on Gitea (ID: {release_id}).")

                # Upload assets if any
                if gh_release["assets"]:
                    print(
                        f"  Found {len(gh_release['assets'])} assets on GitHub for '{tag_name}'."
                    )
                    for gh_asset in gh_release["assets"]:
                        asset_name = gh_asset["name"]
                        asset_url = gh_asset["url"]
                        content_type = gh_asset["content_type"]
                        download_path = os.path.join(temp_dir, asset_name)

                        if download_github_asset(
                            asset_url, github_token, download_path
                        ):
                            upload_gitea_release_asset(
                                gitea_url,
                                gitea_repo_owner,
                                gitea_repo_name,
                                release_id,
                                gitea_token,
                                asset_name,
                                download_path,
                                content_type,
                            )
                            os.remove(download_path)  # Clean up downloaded asset
                else:
                    print(f"  No assets found for GitHub release '{tag_name}'.")
            else:
                print(f"  Release '{tag_name}' already exists on Gitea. Skipping.")
                # TODO: Implement update logic if necessary (e.g., update description, add missing assets)

    finally:
        # Clean up the temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"Cleaned up temporary directory: {temp_dir}")

    print("\n--- Release Synchronization Complete ---")


if __name__ == "__main__":
    # Load environment variables
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    GITEA_TOKEN = os.getenv("GITEA_TOKEN")
    GITHUB_REPO_OWNER = os.getenv("GITHUB_REPO_OWNER")
    GITHUB_REPO_NAME = os.getenv("GITHUB_REPO_NAME")
    GITEA_URL = os.getenv("GITEA_URL")
    GITEA_REPO_OWNER = os.getenv("GITEA_REPO_OWNER")
    GITEA_REPO_NAME = os.getenv("GITEA_REPO_NAME")
    GITHUB_REMOTE_NAME = os.getenv(
        "GITHUB_REMOTE_NAME", "origin"
    )  # Default to 'origin'
    GITEA_REMOTE_NAME = os.getenv("GITEA_REMOTE_NAME", "gitea")  # Default to 'gitea'

    required_vars = {
        "GITHUB_TOKEN": GITHUB_TOKEN,
        "GITEA_TOKEN": GITEA_TOKEN,
        "GITHUB_REPO_OWNER": GITHUB_REPO_OWNER,
        "GITHUB_REPO_NAME": GITHUB_REPO_NAME,
        "GITEA_URL": GITEA_URL,
        "GITEA_REPO_OWNER": GITEA_REPO_OWNER,
        "GITEA_REPO_NAME": GITEA_REPO_NAME,
    }

    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        print(
            f"Error: Missing required environment variables: {', '.join(missing_vars)}",
            file=sys.stderr,
        )
        sys.exit(1)

    sync_releases_to_gitea(
        github_repo_owner=GITHUB_REPO_OWNER,
        github_repo_name=GITHUB_REPO_NAME,
        github_token=GITHUB_TOKEN,
        gitea_url=GITEA_URL,
        gitea_repo_owner=GITEA_REPO_OWNER,
        gitea_repo_name=GITEA_REPO_NAME,
        gitea_token=GITEA_TOKEN,
        github_remote_name=GITHUB_REMOTE_NAME,
        gitea_remote_name=GITEA_REMOTE_NAME,
    )
