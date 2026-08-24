import atexit
import logging
import os
import socket
import struct
import subprocess
import time
import uuid

import requests

logger = logging.getLogger(__name__)


class ElasticContainer:
    """Manages a transient Elasticsearch Docker container for testing."""

    def __init__(
        self,
        image: str | None = None,
        port: int = 19200,
        *,
        port_offset: int = 0,
    ):
        # Use env var if set, otherwise argument, otherwise default
        # Note: We default to Docker Hub (elasticsearch:8.17.0) instead of docker.elastic.co
        # because corporate proxies often block the redirects to Cloudflare R2 used by
        # Elastic's own registry, whereas Docker Hub's infrastructure is usually permitted.
        self.image = os.environ.get(
            "PAT2VEC_ELASTIC_IMAGE",
            image or "elasticsearch:8.17.0",
        )
        # Add port_offset and wrap to safe range 19200-19210
        TEST_PORT_START = 19200
        TEST_PORT_END = 19210
        PORT_RANGE_SIZE = TEST_PORT_END - TEST_PORT_START + 1  # 11 ports
        self.port = TEST_PORT_START + (
            (port + port_offset - TEST_PORT_START) % PORT_RANGE_SIZE
        )
        self.container_name = f"pat2vec-test-elastic-{uuid.uuid4().hex[:8]}"
        self.password = "test_password_123"
        self.container_id: str | None = None
        self.host = "127.0.0.1"

    def __enter__(self):
        if not self.start():
            msg = "Failed to start Elasticsearch container"
            raise RuntimeError(msg)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @staticmethod
    @staticmethod
    def cleanup_orphans() -> None:
        """Finds and removes orphaned pat2vec test containers that are no longer in use.

        This function handles:
        1. Exited/created containers (normal cleanup)
        2. Running containers that have been up for too long (>30 minutes, likely crashed)
        3. Enforces max container limit - stops oldest if more than MAX_CONTAINERS

        MAX_CONTAINERS is set to 4 as tests rarely need more parallel workers.
        """
        logger.info("Cleaning up any orphaned test containers...")
        MAX_CONTAINERS = 4
        CRASHED_TIMEOUT_MINUTES = 30

        try:
            # First, collect ALL matching containers with their details
            result = subprocess.run(
                [
                    "docker",
                    "ps",
                    "-a",
                    "--filter",
                    "name=pat2vec-test-elastic-",
                    "--format",
                    "{{.ID}}\t{{.Status}}\t{{.RunningFor}}",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                logger.warning(f"Could not list containers: {result.stderr}")
                return

            lines = [line for line in result.stdout.strip().split("\n") if line]

            if not lines:
                logger.info("No matching pat2vec-test-elastic containers found.")
                return

            container_ids_to_remove = []

            for line in lines:
                parts = line.split("\t")
                if len(parts) < 3:
                    continue
                cid, status, runtime = parts[0], parts[1], parts[2]

                # Classify container state
                status_lower = status.lower()
                if "exited" in status_lower or "created" in status_lower:
                    container_ids_to_remove.append(cid)
                    logger.info(f"Removing stopped container {cid[:12]} ({status})")
                elif "Up" in status:
                    try:
                        runtime_minutes = _parse_runtime_minutes(runtime)
                        if runtime_minutes > CRASHED_TIMEOUT_MINUTES:
                            container_ids_to_remove.append(cid)
                            logger.info(
                                f"Removing crashed container {cid[:12]} "
                                f"(running for {runtime_minutes} min)",
                            )
                    except (ValueError, TypeError):
                        pass

            # Check if too many containers - remove oldest running ones
            if len(lines) > MAX_CONTAINERS:
                logger.warning(
                    f"Too many test containers ({len(lines)}), removing oldest to maintain max {MAX_CONTAINERS}",
                )
                sorted_containers = sorted(
                    lines,
                    key=lambda x: _parse_runtime_for_sort(
                        x.split("\t")[2] if len(x.split("\t")) > 2 else "",
                    ),
                )
                num_to_remove = len(sorted_containers) - MAX_CONTAINERS
                removed_count = 0
                for line in sorted_containers:
                    if removed_count >= num_to_remove:
                        break
                    parts = line.split("\t")
                    cid = parts[0]
                    if "Up" in parts[1] and cid not in container_ids_to_remove:
                        container_ids_to_remove.append(cid)
                        removed_count += 1
                        logger.info(
                            f"Removing oldest container {cid[:12]} for max limit",
                        )

            if not container_ids_to_remove:
                logger.info("No containers to remove.")
                return

            logger.info(
                f"Removing {len(container_ids_to_remove)} container(s): {container_ids_to_remove}",
            )
            subprocess.run(
                ["docker", "rm", "-f", *container_ids_to_remove],
                capture_output=True,
                check=False,
            )

        except FileNotFoundError:
            logger.warning(
                "Docker command not found, cannot clean up orphaned containers.",
            )
        except Exception as e:
            logger.error(f"An error occurred during orphan cleanup: {e}")

    def _ensure_image(self) -> bool:
        """Ensures the Docker image exists locally, pulling with retries if needed."""
        # Check if image exists
        check = subprocess.run(
            ["docker", "image", "inspect", self.image],
            capture_output=True,
            check=False,
        )
        if check.returncode == 0:
            return True

        # Pull with retries
        retries = 3
        for i in range(retries):
            logger.info(f"Pulling image {self.image} (attempt {i + 1}/{retries})...")
            result = subprocess.run(
                ["docker", "pull", self.image],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return True
            logger.warning(f"Pull failed: {result.stderr.strip()}")
            time.sleep(10)

        return False

    def _is_port_free(self, port: int, timeout: float = 1.0) -> bool:
        """Checks if a local port is free with retry logic.

        Args:
        ----
            port: The port number to check.
            timeout: Maximum time in seconds to wait for the port to become free.
                Defaults to 1.0 second.

        Returns:
        -------
            True if the port is free, False if it's in use or timeout occurs.

        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # connect_ex returns 0 if connection succeeds (port is busy)
                result = s.connect_ex(("localhost", port))
                if result != 0:
                    return True

            # Port is busy, wait a bit before retrying
            time.sleep(0.2)

        return False

    def _find_free_port(
        self,
        start_port: int,
        max_attempts: int = 10,
        timeout: float = 1.0,
    ) -> int:
        """Finds a free port starting from start_port.

        Args:
        ----
            start_port: The port number to start checking from.
            max_attempts: Maximum number of ports to try. Defaults to 10.
            timeout: Time to wait for each port check in seconds. Defaults to 1.0.

        Returns:
        -------
            A free port number within the safe test range (19200-19210).

        Raises:
        ------
            RuntimeError: If no free port is found within the allowed range.

        """
        # Safe test port range - avoids collision with real clusters which typically
        # use standard ports like 9200, 9300, etc.
        TEST_PORT_START = 19200
        TEST_PORT_END = 19210

        for attempt in range(max_attempts):
            port = start_port + attempt
            if port > TEST_PORT_END:
                port = TEST_PORT_START + (port % (TEST_PORT_END - TEST_PORT_START + 1))

            if self._is_port_free(port, timeout=timeout):
                return port

        msg = (
            f"Could not find a free port within the test range "
            f"({TEST_PORT_START}-{TEST_PORT_END}) after {max_attempts} attempts. "
            f"Another process may be holding these ports."
        )
        raise RuntimeError(msg)

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

    def _get_container_ip(self) -> str | None:
        """Retrieves the internal IP address of the container."""
        if not self.container_id:
            return None
        try:
            result = subprocess.run(
                [
                    "docker",
                    "inspect",
                    self.container_id,
                    "-f",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                ],
                capture_output=True,
                text=True,
            )
            return result.stdout.strip()
        except Exception:
            return None

    def _get_host_gateway_ip(self) -> str:
        """Attempts to find the default gateway IP (usually the host)."""
        try:
            # Method 1: Check /proc/net/route (standard Linux way, works even if 'ip' command is missing)
            if os.path.exists("/proc/net/route"):
                with open("/proc/net/route") as f:
                    for line in f:
                        fields = line.strip().split()
                        if len(fields) > 2 and fields[1] == "00000000":  # Default route
                            return socket.inet_ntoa(
                                struct.pack("<L", int(fields[2], 16)),
                            )
        except Exception:
            pass

        try:
            result = subprocess.run(
                ["ip", "route", "show", "default"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and "via" in result.stdout:
                return result.stdout.split("via")[1].split()[0]
        except Exception:
            pass
        return "127.0.0.1"

    def start(self, timeout: int = 180) -> bool:
        """Starts an Elasticsearch container. Returns True if successful."""
        # First, clean up any containers from previous failed runs
        self.cleanup_orphans()

        # Check if docker is available
        try:
            subprocess.run(
                ["docker", "--version"],
                capture_output=True,
                check=False,
            )
        except FileNotFoundError:
            logger.warning("Docker not found. Skipping Elasticsearch container start.")
            return False

        if not self._ensure_image():
            logger.error(f"Failed to pull Docker image: {self.image}")
            return False

        # Determine port mapping - try to find a free port within the safe test range
        # If no free port found in the range, let Docker assign one automatically

        # First, clean up any containers that may be holding ports
        self.cleanup_orphans()

        # Try to find a free port with retries - check multiple times for robustness
        try:
            self.port = self._find_free_port(self.port, max_attempts=10, timeout=3.0)
            logger.info(f"Found available test port: {self.port}")
        except RuntimeError as e:
            logger.warning(
                f"Fallback: Docker auto-assigning port (no free ports in range "
                f"{19200}-{19210}). Original error: {e}",
            )
            # Let Docker assign a random free port
            port_mapping = "9200"
        else:
            # Use specific port mapping since we now have a confirmed free port
            port_mapping = f"{self.port}:9200"

        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            self.container_name,
            "-p",
            port_mapping,
            "--add-host=host.docker.internal:host-gateway",
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
                capture_output=True,
                check=False,
            )
            self.container_id = None

    def _wait_for_ready(self, timeout: int) -> None:
        """Waits for Elasticsearch to be responsive."""
        start_time = time.time()
        auth = ("elastic", self.password)

        logger.info("Waiting for Elasticsearch to become ready...")

        # Use a Session with trust_env=False to completely bypass environment proxy settings.
        # This ensures the health check hits the local container loopback even if
        # http_proxy is set, which is common in self-hosted CI runners.
        session = requests.Session()
        session.trust_env = False

        # Determine connection targets: Localhost, Gateway, and internal Container IP
        # Determine connection targets
        container_ip = self._get_container_ip()
        gateway_ip = self._get_host_gateway_ip()

        # Target list: (host, port)
        targets = [
            ("127.0.0.1", self.port),
            (gateway_ip, self.port),
            ("host.docker.internal", self.port),
        ]
        if container_ip:
            targets.append((container_ip, 9200))

        logger.info(f"Waiting for Elasticsearch health on targets: {targets}")

        while time.time() - start_time < timeout:
            for test_host, test_port in targets:
                test_url = f"http://{test_host}:{test_port}/_cluster/health"
                try:
                    response = session.get(test_url, auth=auth, timeout=2)
                    if response.status_code == 200:
                        status = response.json().get("status")
                        if status in ["green", "yellow"]:
                            logger.info(
                                f"✅ Elasticsearch is ready at {test_url} (status: {status}).",
                            )
                            # Update instance state to the successful connection info
                            self.host = test_host
                            self.port = test_port
                            return
                except (requests.exceptions.RequestException, ConnectionError):
                    continue
                except Exception as e:
                    logger.warning(
                        f"Unexpected health check error for {test_host}: {e}",
                    )

            time.sleep(5)

        msg = "Elasticsearch container failed to start within timeout."
        raise TimeoutError(msg)

    def get_credentials(self) -> tuple[str, str, str]:
        """Returns (host_url, username, password)."""
        return f"http://{self.host}:{self.port}", "elastic", self.password


def _parse_runtime_minutes(runtime_str: str) -> float | None:
    """Parse Docker runtime string to minutes.

    Args:
    ----
        runtime_str: Runtime string from docker (e.g., "2 hours", "10 minutes", "5 seconds")

    Returns:
    -------
        Runtime in minutes, or None if parsing fails.

    """
    import re

    runtime_str = runtime_str.lower().strip()

    if not runtime_str or "about" in runtime_str or "ago" in runtime_str:
        return 0.0

    total_minutes = 0.0
    hours_match = re.search(r"(\d+)\s*(hour|hr|h)", runtime_str)
    minutes_match = re.search(r"(\d+)\s*(minute|min|m)", runtime_str)
    seconds_match = re.search(r"(\d+)\s*(second|sec|s)", runtime_str)

    if hours_match:
        total_minutes += int(hours_match.group(1)) * 60
    if minutes_match:
        total_minutes += int(minutes_match.group(1))
    if seconds_match:
        total_minutes += int(seconds_match.group(1)) / 60

    return total_minutes


def _parse_runtime_for_sort(runtime_str: str) -> float:
    """Parse runtime string for sorting (newer containers first)."""
    minutes = _parse_runtime_minutes(runtime_str)
    return -minutes if minutes is not None else 0.0
