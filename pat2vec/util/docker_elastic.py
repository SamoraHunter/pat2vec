import subprocess
import socket
import os
import time
import logging
import uuid
import requests
import warnings
import atexit
from typing import Optional, Tuple

# Suppress insecure request warnings for local test instance
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter("ignore", InsecureRequestWarning)

logger = logging.getLogger(__name__)


class ElasticContainer:
    """Manages a transient Elasticsearch Docker container for testing."""

    def __init__(self, image: Optional[str] = None, port: int = 19200):
        # Use env var if set, otherwise argument, otherwise default
        # Note: We default to Docker Hub (elasticsearch:8.17.0) instead of docker.elastic.co
        # because corporate proxies often block the redirects to Cloudflare R2 used by
        # Elastic's own registry, whereas Docker Hub's infrastructure is usually permitted.
        self.image = os.environ.get(
            "PAT2VEC_ELASTIC_IMAGE", image or "elasticsearch:8.17.0"
        )
        self.port = port
        self.container_name = f"pat2vec-test-elastic-{uuid.uuid4().hex[:8]}"
        self.password = "test_password_123"
        self.container_id: Optional[str] = None
        self.host = "localhost"

    def __enter__(self):
        if not self.start():
            raise RuntimeError("Failed to start Elasticsearch container")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @staticmethod
    def cleanup_orphans():
        """Finds and removes any orphaned pat2vec test containers."""
        logger.info("Cleaning up any orphaned test containers...")
        try:
            # Find containers with the specific name pattern
            result = subprocess.run(
                [
                    "docker",
                    "ps",
                    "-a",
                    "--filter",
                    "name=pat2vec-test-elastic-",
                    "--format",
                    "{{.ID}}",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning(f"Could not list orphaned containers: {result.stderr}")
                return

            container_ids = [cid for cid in result.stdout.strip().split("\n") if cid]
            if not container_ids:
                logger.info("No orphaned containers found.")
                return

            logger.info(
                f"Found {len(container_ids)} orphaned container(s): {container_ids}. Removing..."
            )
            # Remove them
            subprocess.run(
                ["docker", "rm", "-f"] + container_ids,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError:
            logger.warning(
                "Docker command not found, cannot clean up orphaned containers."
            )
        except Exception as e:
            logger.error(f"An error occurred during orphan cleanup: {e}")

    def _ensure_image(self) -> bool:
        """Ensures the Docker image exists locally, pulling with retries if needed."""
        # Check if image exists
        check = subprocess.run(
            ["docker", "image", "inspect", self.image],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if check.returncode == 0:
            return True

        # Pull with retries
        retries = 3
        for i in range(retries):
            logger.info(f"Pulling image {self.image} (attempt {i+1}/{retries})...")
            result = subprocess.run(
                ["docker", "pull", self.image], capture_output=True, text=True
            )
            if result.returncode == 0:
                return True
            logger.warning(f"Pull failed: {result.stderr.strip()}")
            time.sleep(10)

        return False

    def _is_port_free(self, port: int) -> bool:
        """Checks if a local port is free."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # connect_ex returns 0 if connection succeeds (port is busy)
            return s.connect_ex(("localhost", port)) != 0

    def _get_mapped_port(self) -> int:
        """Retrieves the host port mapped to container port 9200."""
        try:
            result = subprocess.run(
                ["docker", "port", self.container_name, "9200"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and result.stdout:
                # Output format example: 0.0.0.0:32768
                return int(result.stdout.strip().split(":")[-1])
        except Exception as e:
            logger.warning(f"Could not retrieve mapped port: {e}")
        return self.port

    def start(self, timeout: int = 120) -> bool:
        """Starts an Elasticsearch container. Returns True if successful."""
        # First, clean up any containers from previous failed runs
        self.cleanup_orphans()

        # Check if docker is available
        try:
            subprocess.run(
                ["docker", "--version"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("Docker not found. Skipping Elasticsearch container start.")
            return False

        if not self._ensure_image():
            logger.error(f"Failed to pull Docker image: {self.image}")
            return False

        # Determine port mapping. Use random port if configured port is busy.
        port_mapping = f"{self.port}:9200"
        if not self._is_port_free(self.port):
            logger.warning(
                f"Port {self.port} is in use. Letting Docker assign a random port."
            )
            port_mapping = "9200"

        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            self.container_name,
            "-p",
            port_mapping,
            "-e",
            "discovery.type=single-node",
            "-e",
            "xpack.security.enabled=true",
            "-e",
            "xpack.security.http.ssl.enabled=false",
            "-e",
            f"ELASTIC_PASSWORD={self.password}",
            "-e",
            f"ELASTICSEARCH_PASSWORD={self.password}",  # Compatibility with Bitnami images
            "-e",
            "ES_JAVA_OPTS=-Xms512m -Xmx512m",
            self.image,
        ]

        logger.info(f"Starting Elasticsearch container: {self.container_name}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Failed to start container: {result.stderr}")
                return False

            self.container_id = result.stdout.strip()
            self.port = self._get_mapped_port()
            logger.info(f"Container started on port {self.port}")
            atexit.register(self.stop)
            self._wait_for_ready(timeout)
            return True
        except Exception as e:
            logger.error(f"Error starting container: {e}")
            try:
                logs = subprocess.run(
                    ["docker", "logs", "--tail", "20", self.container_name],
                    capture_output=True,
                    text=True,
                )
                logger.error(f"Container logs:\n{logs.stdout}\n{logs.stderr}")
            except Exception:
                pass
            self.stop()
            return False

    def stop(self) -> None:
        """Stops and removes the container."""
        try:
            # Safely unregister the exit handler
            atexit.unregister(self.stop)
        except Exception:
            pass
        if self.container_id or self.container_name:
            # Try stopping by name if ID is missing (cleanup)
            target = self.container_id or self.container_name
            logger.info(f"Stopping container: {target}")
            subprocess.run(
                ["docker", "rm", "-f", target],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.container_id = None

    def _wait_for_ready(self, timeout: int) -> None:
        """Waits for Elasticsearch to be responsive."""
        start_time = time.time()
        url = f"http://{self.host}:{self.port}/_cluster/health"
        auth = ("elastic", self.password)

        logger.info(f"Waiting for Elasticsearch to become ready at {url}...")
        while time.time() - start_time < timeout:
            try:
                # Check health
                response = requests.get(url, auth=auth, verify=False, timeout=2)
                if response.status_code == 200:
                    status = response.json().get("status")
                    # Yellow is fine for single node
                    if status in ["green", "yellow"]:
                        logger.info(f"Elasticsearch is ready (status: {status}).")
                        return
            except (requests.exceptions.RequestException, ConnectionError) as e:
                logger.debug(f"Connection attempt failed: {e}")
            except Exception as e:
                logger.warning(f"Health check error: {e}")
            time.sleep(2)

        raise TimeoutError("Elasticsearch container failed to start within timeout.")

    def get_credentials(self) -> Tuple[str, str, str]:
        """Returns (host_url, username, password)."""
        return f"http://{self.host}:{self.port}", "elastic", self.password
